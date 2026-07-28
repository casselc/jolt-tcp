(ns teensyp.windows-runtime-test
  "Native Windows socket-runtime gate for jolt-tcp.

  Replaces the former `teensyp.windows-portable-test`, whose central assertion
  was that `jolt.net/open-poller` fails closed on Windows. The pinned jolt-net
  revision ships reviewed Windows readiness backends for x86-64 and aarch64,
  so that assertion is obsolete: this gate instead requires real loopback
  sockets to work on the exact architecture named by JOLT_EXPECTED_ARCH.

  Deliberately dependency-free. It requires only the production namespaces and
  `clojure.test`, never jolt-hegel and never an installed native artifact, so
  Windows socket coverage cannot silently disappear because an optional test
  dependency failed to resolve or install. The Hegel-required suite is a
  separate lane.

  Every wait here is bounded. Sleeps are used only to let a peer make progress
  before an assertion that does not depend on that progress having happened;
  the correctness oracle is always a bounded `deref` on a promise or future,
  and a watchdog turns a wedged operation into a failure rather than a hang."
  (:require [clojure.test :as t :refer [deftest is testing]]
            [jolt.net :as net]
            [teensyp.buffer :as buf]
            [teensyp.client :as client]
            [teensyp.client-test]
            [teensyp.concurrent]
            [teensyp.ffi-net :as compat]
            [teensyp.server :as tcp]
            [teensyp.stream]))

;; --- bounded synchronization helpers ---------------------------------------

(def ^:private watchdog-ms 5000)

(defn- settled
  "Run `f` on another thread and return {:value v} or {:error e}, or
  ::watchdog if it did not finish within `ms`. An operation that wedges
  therefore fails an assertion instead of hanging the gate."
  ([f] (settled watchdog-ms f))
  ([ms f]
   (deref (future
            (try {:value (f)}
                 (catch :default e {:error e})))
          ms ::watchdog)))

(defn- err-of [outcome]
  (:err (ex-data (:error outcome))))

(defn- kind-of [outcome]
  (:jolt.net/kind (ex-data (:error outcome))))

(defn- utf8 [^String s] (.getBytes s "UTF-8"))
(defn- ->str [^bytes b] (when b (String. b "UTF-8")))

(defn- echo-handler
  "Echo every readable chunk, and close once the peer has half-closed."
  ([_socket] nil)
  ([state socket buffer]
   (let [n (buf/remaining buffer)]
     (when (pos? n)
       (tcp/write socket (buf/wrap (buf/get-bytes! buffer n))))
     (when (tcp/peer-eof-notified? socket)
       (tcp/close socket))
     state))
  ([_state _exception] nil))

(defn- with-echo-server
  "Start a port-zero echo server, run `f` with the handle, and always stop it."
  [f]
  (let [server (tcp/run-server :port 0
                               :handler echo-handler
                               :reuse-address? true)]
    (try (f server)
         (finally (tcp/stop-server server)))))

(defn- receive-exactly
  "Read until `n` bytes have arrived or the deadline lapses. Returns the bytes
  read as a String. Never assumes one write arrives as one read."
  [connection n timeout-ms]
  (loop [acc ""]
    (if (>= (alength (utf8 acc)) (long n))
      acc
      (if-let [chunk (client/receive-at-most! connection 4096
                                              {:timeout-ms timeout-ms})]
        (recur (str acc (->str chunk)))
        acc))))

;; --- real loopback contracts ------------------------------------------------

(deftest port-zero-listen-connect-and-request-response
  (with-echo-server
    (fn [server]
      (testing "a port-zero listen reports the real bound port"
        (is (pos? (:port server)))
        (is (true? @(:running? server))))
      (let [connection (client/connect "127.0.0.1" (:port server)
                                       {:connect-timeout-ms 2000
                                        :no-delay? true})]
        (try
          (testing "accept, write, and read complete over real loopback"
            (is (client/connection? connection))
            (is (= :open (:state (client/connection-info connection))))
            (is (= (:port server)
                   (get-in (client/connection-info connection)
                           [:remote-address :port])))
            (client/send-all! connection (utf8 "windows-runtime")
                              {:timeout-ms 2000})
            (is (= "windows-runtime"
                   (receive-exactly connection 15 2000))))

          (testing "a second request/response round trip reuses the connection"
            (client/send-all! connection (utf8 "second") {:timeout-ms 2000})
            (is (= "second" (receive-exactly connection 6 2000))))

          (testing "no socket, poller, or native descriptor leaks into the API"
            (let [info (client/connection-info connection)]
              (is (not (contains? info :fd)))
              (is (not (contains? info :jolt.net/raw)))))
          (finally (client/close! connection)))))))

(deftest peer-half-close-is-answered-and-then-reaches-eof
  (with-echo-server
    (fn [server]
      (let [connection (client/connect "127.0.0.1" (:port server)
                                       {:connect-timeout-ms 2000
                                        :no-delay? true})]
        (try
          (client/send-all! connection (utf8 "half") {:timeout-ms 2000})
          (is (= "half" (receive-exactly connection 4 2000)))

          (testing "half-closing the write side is idempotent and observable"
            (is (true? (client/shutdown-write! connection)))
            (is (false? (client/shutdown-write! connection)))
            (is (true? (:write-shutdown?
                        (client/connection-info connection)))))

          (testing "writing after half-close is refused structurally"
            (is (= :teensyp.client/write-shutdown
                   (err-of (settled #(client/send-all! connection
                                                       (utf8 "nope")))))))

          (testing "the peer answers the half-close and then delivers EOF"
            (let [outcome (settled #(client/receive-at-most!
                                     connection 128 {:timeout-ms 2000}))]
              (is (not= ::watchdog outcome))
              (is (nil? (:value outcome))))
            (is (true? (:read-eof? (client/connection-info connection)))))
          (finally (client/close! connection)))))))

;; Windows does not surface a refused TCP connect the way POSIX does. A POSIX
;; loopback connect to a closed port fails on the RST within a millisecond;
;; Windows retransmits the SYN first and only then reports WSAECONNREFUSED, so
;; readiness measured ~2.03-2.05s here across repeated native runs. jolt.net
;; itself is correct -- it reports #{:hangup :write :error} and classifies
;; code 10061 as :connection-refused -- but any connect deadline shorter than
;; that latency legitimately expires as a timeout first. The generous deadline
;; below is therefore a platform fact, not a tolerance for flakiness: the
;; assertions still require an exact classification.
(def ^:private refusal-latency-headroom-ms 15000)

(deftest refused-outbound-connection-is-classified
  ;; Bind a port, then release it, so the port is one nothing is listening on
  ;; rather than an arbitrary guess.
  (let [dead-port (with-echo-server (fn [server] (:port server)))
        outcome (settled (+ refusal-latency-headroom-ms 5000)
                         #(client/connect
                           "127.0.0.1" dead-port
                           {:connect-timeout-ms refusal-latency-headroom-ms}))]
    (is (not= ::watchdog outcome))
    (is (some? (:error outcome)))
    (testing "a refused connect is a classified connect failure, not a timeout"
      (is (not= :teensyp.client/connect-timeout (err-of outcome)))
      (is (= :connection-refused (kind-of outcome))))))

(deftest outbound-connect-is-deadline-aware
  (with-echo-server
    (fn [server]
      (testing "an already-expired connect deadline never opens a socket"
        (let [outcome (settled #(client/connect "127.0.0.1" (:port server)
                                                {:connect-timeout-ms 0}))]
          (is (not= ::watchdog outcome))
          (is (= :teensyp.client/connect-timeout (err-of outcome)))))

      (testing "the same endpoint connects when the deadline allows it"
        (let [outcome (settled #(client/connect "127.0.0.1" (:port server)
                                                {:connect-timeout-ms 2000}))]
          (is (not= ::watchdog outcome))
          (is (client/connection? (:value outcome)))
          (client/close! (:value outcome)))))))

(deftest connection-and-server-close-are-idempotent
  (let [server (tcp/run-server :port 0 :handler echo-handler
                               :reuse-address? true)
        connection (client/connect "127.0.0.1" (:port server)
                                   {:connect-timeout-ms 2000})]
    (testing "only the call that begins close returns true"
      (is (true? (client/close! connection)))
      (is (false? (client/close! connection)))
      (is (true? (client/closed? connection)))
      (is (= :closed (:state (client/connection-info connection)))))

    (testing "operations after close fail closed rather than blocking"
      (let [outcome (settled #(client/receive-at-most! connection 1
                                                       {:deadline-nanos 0}))]
        (is (not= ::watchdog outcome))
        (is (= :teensyp.client/closed (err-of outcome)))))

    (testing "repeated stop-server observes the same completion"
      (is (not= ::watchdog (settled #(tcp/stop-server server))))
      (is (not= ::watchdog (settled #(tcp/stop-server server))))
      (is (false? @(:running? server))))))

(deftest repeated-server-start-stop-keeps-serving
  (doseq [round (range 3)]
    (with-echo-server
      (fn [server]
        (let [connection (client/connect "127.0.0.1" (:port server)
                                         {:connect-timeout-ms 2000
                                          :no-delay? true})
              payload (str "round-" round)]
          (try
            (client/send-all! connection (utf8 payload) {:timeout-ms 2000})
            (is (= payload
                   (receive-exactly connection
                                    (alength (utf8 payload)) 2000)))
            (finally (client/close! connection))))))))

(deftest server-stop-wakes-a-blocked-readiness-wait
  (let [server (tcp/run-server :port 0 :handler echo-handler
                               :reuse-address? true)
        connection (client/connect "127.0.0.1" (:port server)
                                   {:connect-timeout-ms 2000})
        reader-entered (promise)
        reading (future
                  (deliver reader-entered true)
                  (try {:value (client/receive-at-most! connection 128)}
                       (catch :default e {:error e})))]
    (try
      (is (true? (deref reader-entered 2000 false)))
      ;; Not an oracle: this only gives the reader time to park in the poller,
      ;; so the wake below is exercised against a genuinely blocked waiter.
      (Thread/sleep 100)
      (tcp/stop-server server)
      (testing "stopping the server releases the blocked reader promptly"
        (let [outcome (deref reading watchdog-ms ::watchdog)]
          (is (not= ::watchdog outcome))
          ;; The server closes the connection, so the blocked read observes
          ;; EOF or a classified transport error -- never a wedge.
          (is (or (nil? (:value outcome)) (some? (:error outcome))))))
      (finally
        (client/close! connection)
        (tcp/stop-server server)))))

(deftest client-close-wakes-a-blocked-readiness-wait
  (with-echo-server
    (fn [server]
      (let [connection (client/connect "127.0.0.1" (:port server)
                                       {:connect-timeout-ms 2000})
            reader-entered (promise)
            reading (future
                      (deliver reader-entered true)
                      (try {:value (client/receive-at-most! connection 128)}
                           (catch :default e {:error e})))]
        (is (true? (deref reader-entered 2000 false)))
        (Thread/sleep 100)
        (is (true? (client/close! connection)))
        (testing "close from another thread releases the blocked reader"
          (let [outcome (deref reading watchdog-ms ::watchdog)]
            (is (not= ::watchdog outcome))
            (is (or (nil? (:value outcome)) (some? (:error outcome))))))))))

(deftest compatibility-wrappers-drive-the-same-public-connection
  ;; teensyp.ffi-net is a thin compatibility layer over teensyp.client. This
  ;; keeps that claim honest on Windows rather than assuming it from source.
  (with-echo-server
    (fn [server]
      (let [socket (compat/connect-loopback (:port server))]
        (try
          (is (client/connection? socket))
          (compat/client-send-all socket (utf8 "compat"))
          (is (= "compat" (->str (compat/client-recv socket 4096))))
          (is (true? (compat/shutdown-write! socket)))
          (let [outcome (settled #(compat/client-recv socket 4096))]
            (is (not= ::watchdog outcome))
            (is (nil? (:value outcome))))
          (finally (compat/close! socket)))))))

;; --- gate driver ------------------------------------------------------------

(defn- test-vars-in [namespace]
  (->> (ns-interns namespace)
       vals
       (filter #(contains? (meta %) :test))
       (sort-by #(str (:name (meta %))))))

(defn -main [& _]
  (let [expected-name (System/getenv "JOLT_EXPECTED_ARCH")
        expected-arch (case expected-name
                        "x86-64" :x86-64
                        "aarch64" :aarch64
                        (throw
                          (ex-info
                            "JOLT_EXPECTED_ARCH must be x86-64 or aarch64"
                            {:value expected-name})))
        observed (jolt.host/target)]
    (when-not (= [:windows expected-arch 64]
                 [(:os observed) (:arch observed) (:pointer-bits observed)])
      (throw
        (ex-info "Windows runtime gate ran on the wrong native target"
                 {:expected [:windows expected-arch 64]
                  :target observed})))

    ;; A readiness poller must now really open. The predecessor gate asserted
    ;; the opposite; keeping an explicit positive check makes the W5 promotion
    ;; the thing this gate fails on if it ever regresses.
    (let [poller (net/open-poller)]
      (net/close! poller))

    (let [client-vars (vec (test-vars-in 'teensyp.client-test))
          runtime-vars (vec (test-vars-in 'teensyp.windows-runtime-test))
          loopback-vars (filterv #(= 'public-client-loopback-contract
                                     (:name (meta %)))
                                 client-vars)
          all-vars (into client-vars runtime-vars)]
      ;; The real public client loopback contract is no longer excluded.
      (when-not (= 1 (count loopback-vars))
        (throw
          (ex-info "the public client loopback contract is missing"
                   {:client-tests (mapv #(-> % meta :name) client-vars)})))
      (when-not (and (= 15 (count client-vars))
                     (= 9 (count runtime-vars)))
        (throw
          (ex-info "Windows runtime gate inventory changed"
                   {:client-count (count client-vars)
                    :runtime-count (count runtime-vars)
                    :client-tests (mapv #(-> % meta :name) client-vars)
                    :runtime-tests (mapv #(-> % meta :name) runtime-vars)})))
      (when-not (pos? (count all-vars))
        (throw (ex-info "Windows runtime gate selected no tests" {})))

      (t/test-vars all-vars)

      (let [failed (+ (t/n-fail) (t/n-error))]
        (when-not (pos? (t/n-pass))
          (throw (ex-info "Windows runtime gate was vacuous" {})))
        (println "Windows runtime gate:"
                 (count all-vars) "tests ("
                 (count client-vars) "client +"
                 (count runtime-vars) "loopback),"
                 (t/n-pass) "assertions passed,"
                 (t/n-fail) "failures," (t/n-error) "errors")
        (flush)
        (System/exit (if (zero? failed) 0 1))))))
