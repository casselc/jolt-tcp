(ns teensyp.client-test
  "Deterministic connector semantics plus one real loopback contract."
  (:require [clojure.test :refer [deftest is testing]]
            [jolt.net :as net]
            [teensyp.buffer :as buf]
            [teensyp.client :as client]
            [teensyp.server :as tcp]))

(defn- pop-result! [results]
  (let [result (first @results)]
    (swap! results #(vec (next %)))
    result))

(deftest partial-and-would-block-loops
  (testing "send-all preserves the unsent slice across would-block and partial writes"
    (let [results (atom [net/would-block 2 1])
          calls (atom [])
          waits (atom 0)
          bytes (.getBytes "abc" "UTF-8")]
      (@#'client/send-all-loop!
        (fn [_ off len]
          (swap! calls conj [off len])
          (pop-result! results))
        #(swap! waits inc)
        bytes 0 3)
      (is (= [[0 3] [0 3] [2 1]] @calls))
      (is (= 1 @waits))
      (is (empty? @results))))

  (testing "receive retries would-block, returns a partial count, and preserves EOF"
    (let [results (atom [net/would-block 2])
          waits (atom 0)
          dest (byte-array 4)]
      (is (= 2
             (@#'client/receive-loop!
               (fn [_ _ _] (pop-result! results))
               #(swap! waits inc)
               dest 0 4)))
      (is (= 1 @waits))
      (is (nil?
            (@#'client/receive-loop!
              (fn [_ _ _] net/eof)
              (fn [] (throw (ex-info "must not wait at EOF" {})))
              dest 0 4))))))

(defn- thrown-by [f]
  (try
    (f)
    nil
    (catch :default e e)))

(defn- operation-timeout! [op deadline]
  (throw (@#'client/operation-timeout-ex op deadline)))

(defn- timeout-data [exception]
  (select-keys (ex-data exception)
               [:err
                :teensyp.client/op
                :teensyp.client/kind
                :teensyp.client/deadline-nanos]))

(deftest operation-deadline-options-and-expiry
  (is (= 5000007
         (@#'client/operation-deadline :send {:timeout-ms 5} 7)))
  (is (= 42
         (@#'client/operation-deadline :receive
           {:deadline-nanos 42} 7)))
  (is (nil? (@#'client/operation-deadline :send {} 7)))
  (let [waits (atom [])]
    (is (true?
          (@#'client/await-operation!
            1000001
            (constantly 1000000)
            (fn [timeout-ms]
              (swap! waits conj timeout-ms)
              [:ready]))))
    (is (= [1] @waits)))
  (let [invalid
        (thrown-by
          #(@#'client/operation-deadline
             :send {:timeout-ms 1 :deadline-nanos 2} 0))]
    (is (= ::client/invalid (:err (ex-data invalid)))))

  (testing "an already-expired deadline performs no I/O or readiness wait"
    (let [writes (atom 0)
          waits (atom 0)
          deadline 10
          exception
          (thrown-by
            #(@#'client/send-all-loop!
               (fn [_ _ _] (swap! writes inc) 1)
               (fn [] (swap! waits inc) true)
               (constantly true)
               #(operation-timeout! :send deadline)
               (byte-array 1) 0 1))]
      (is (= {:err ::client/timed-out
              :teensyp.client/op :send
              :teensyp.client/kind :timed-out
              :teensyp.client/deadline-nanos deadline}
             (timeout-data exception)))
      (is (zero? @writes))
      (is (zero? @waits)))))

(deftest operation-deadlines-bound-wait-and-retry
  (testing "send would-block expires through the readiness wait"
    (let [clock (atom 0)
          waits (atom [])
          deadline 10000000
          exception
          (thrown-by
            #(@#'client/send-all-loop!
               (fn [_ _ _] net/would-block)
               #(@#'client/await-operation!
                  deadline
                  #(long @clock)
                  (fn [timeout-ms]
                    (swap! waits conj timeout-ms)
                    (reset! clock deadline)
                    []))
               #(>= (long @clock) deadline)
               #(operation-timeout! :send deadline)
               (byte-array 1) 0 1))]
      (is (= [10] @waits))
      (is (= :send (:teensyp.client/op (ex-data exception))))
      (is (= ::client/timed-out (:err (ex-data exception))))))

  (testing "receive would-block has a distinct receive timeout"
    (let [clock (atom 0)
          deadline 10000000
          exception
          (thrown-by
            #(@#'client/receive-loop!
               (fn [_ _ _] net/would-block)
               #(@#'client/await-operation!
                  deadline
                  #(long @clock)
                  (fn [_]
                    (reset! clock deadline)
                    []))
               #(>= (long @clock) deadline)
               #(operation-timeout! :receive deadline)
               (byte-array 1) 0 1))]
      (is (= :receive (:teensyp.client/op (ex-data exception))))
      (is (= ::client/timed-out (:err (ex-data exception))))))

  (testing "partial progress cannot restart or overrun the deadline"
    (let [clock (atom 0)
          calls (atom [])
          deadline 10000000
          exception
          (thrown-by
            #(@#'client/send-all-loop!
               (fn [_ off len]
                 (swap! calls conj [off len])
                 (reset! clock deadline)
                 2)
               (fn [] (throw (ex-info "must not wait" {})))
               #(>= (long @clock) deadline)
               #(operation-timeout! :send deadline)
               (byte-array 3) 0 3))]
      (is (= [[0 3]] @calls))
      (is (= :send (:teensyp.client/op (ex-data exception))))
      (is (= ::client/timed-out (:err (ex-data exception))))))

  (testing "readiness and complete write before the deadline succeed"
    (let [clock (atom 0)
          results (atom [net/would-block 3])
          waits (atom [])
          deadline 10000000]
      (is (nil?
            (@#'client/send-all-loop!
              (fn [_ _ _] (pop-result! results))
              #(@#'client/await-operation!
                 deadline
                 #(long @clock)
                 (fn [timeout-ms]
                   (swap! waits conj timeout-ms)
                   (reset! clock 4000000)
                   [:ready]))
              #(>= (long @clock) deadline)
              #(operation-timeout! :send deadline)
              (byte-array 3) 0 3)))
      (is (= [10] @waits))
      (is (empty? @results))))

  (testing "EOF reported with readiness wins a simultaneous deadline"
    (let [clock (atom 0)
          results (atom [net/would-block net/eof])
          deadline 10000000]
      (is (nil?
            (@#'client/receive-loop!
              (fn [_ _ _] (pop-result! results))
              #(@#'client/await-operation!
                 deadline
                 #(long @clock)
                 (fn [_]
                   (reset! clock deadline)
                   [:ready]))
              #(>= (long @clock) deadline)
              #(operation-timeout! :receive deadline)
              (byte-array 1) 0 1)))
      (is (empty? @results)))))

  (testing "a native reset reported with readiness is never rewritten"
    (let [clock (atom 0)
          first? (atom true)
          deadline 10000000
          reset-error (ex-info "connection reset"
                               {:jolt.net/op :read
                                :jolt.net/kind :connection-reset})
          exception
          (thrown-by
            #(@#'client/receive-loop!
               (fn [_ _ _]
                 (if @first?
                   (do (reset! first? false) net/would-block)
                   (throw reset-error)))
               #(@#'client/await-operation!
                  deadline
                  #(long @clock)
                  (fn [_]
                    (reset! clock deadline)
                    [:ready]))
               #(>= (long @clock) deadline)
               #(operation-timeout! :receive deadline)
               (byte-array 1) 0 1))]
      (is (identical? reset-error exception))))

(deftest timed-gate-cancellation-cannot-steal-or-wedge-ownership
  (let [gate (@#'client/operation-gate)
        times (atom [0 10])]
    (is (true? (@#'client/acquire-gate! gate)))
    (is (false?
          (@#'client/acquire-gate!
            gate 10
            (fn []
              (let [now (first @times)]
                (swap! times #(vec (next %)))
                now)))))
    ;; The cancelled ticket remains inert until release observes and skips it.
    (is (= 1 (count (:waiters @gate))))
    (@#'client/release-gate! gate)
    (is (false? (:held? @gate)))
    (is (empty? (:waiters @gate)))
    (is (true? (@#'client/acquire-gate! gate)))
    (@#'client/release-gate! gate)))

(deftest same-direction-operations-are-fifo-serialized
  (let [gate (@#'client/operation-gate)
        first-entered (promise)
        release-first (promise)
        order (atom [])
        first-task
        (future
          (@#'client/with-gate
            gate
            (fn []
              (swap! order conj :first-enter)
              (deliver first-entered true)
              @release-first
              (swap! order conj :first-exit))))
        _ (deref first-entered 1000 :timeout)
        second-task
        (future
          (@#'client/with-gate gate
            #(swap! order conj :second-enter)))]
    (Thread/sleep 25)
    (is (= [:first-enter] @order))
    (deliver release-first true)
    (is (not= :timeout (deref first-task 1000 :timeout)))
    (is (not= :timeout (deref second-task 1000 :timeout)))
    (is (= [:first-enter :first-exit :second-enter] @order))))

(defn- fake-ops
  [{:keys [clock resolve-result try-connect-fn finish-fn await-fn events]}]
  (let [events (or events (atom []))]
    {:now #(long @clock)
     :resolve (fn [_ _] resolve-result)
     :open-poller (fn [] (swap! events conj [:open :poller]) :poller)
     :try-connect try-connect-fn
     :register (fn [poller socket interests]
                 (let [token [:token socket]]
                   (swap! events conj [:register poller socket interests token])
                   token))
     :remove (fn [poller token]
               (swap! events conj [:remove poller token])
               true)
     :await (fn [poller timeout-ms]
              (swap! events conj [:await poller timeout-ms])
              (await-fn timeout-ms))
     :finish finish-fn
     :close (fn [owned]
              (swap! events conj [:close owned])
              true)}))

(deftest explicit-unbounded-connect-does-not-inherit-the-default-deadline
  (is (= 30000000007
         (@#'client/deadline-for {} 7)))
  (is (= 7
         (@#'client/deadline-for {:connect-timeout-ms 0} 7)))
  (is (nil?
        (@#'client/deadline-for {:connect-timeout-ms nil} 7)))
  (let [clock (atom 0)
        waits (atom [])
        ops (fake-ops
              {:clock clock
               :resolve-result [:address]
               :try-connect-fn
               (fn [_ _]
                 {:jolt.net/socket :socket
                  :jolt.net/status net/in-progress})
               :await-fn
               (fn [timeout-ms]
                 (swap! waits conj timeout-ms)
                 ;; Advance well beyond the ordinary 30-second default. The
                 ;; second turn reports readiness and must still complete.
                 (swap! clock + 31000000000)
                 (when (= 2 (count @waits))
                   [{:events #{:write}}]))
               :finish-fn (fn [_] net/connected)})
        established (@#'client/establish! :endpoint {} nil ops)]
    (is (= [1000 1000] @waits))
    (is (= :socket (:socket established)))
    (is (= :poller (:write-poller established)))))

(deftest one-absolute-deadline-spans-all-candidates
  (let [clock (atom 0)
        events (atom [])
        first-error (ex-info "candidate a failed"
                             {:jolt.net/op :connect :jolt.net/code 101})
        ops (fake-ops
              {:clock clock
               :events events
               :resolve-result [:a :b]
               :try-connect-fn
               (fn [address _]
                 {:jolt.net/socket (if (= address :a) :socket-a :socket-b)
                  :jolt.net/status net/in-progress})
               :await-fn
               (fn [_]
                 (swap! clock + 4000000)
                 [{:events #{:write}}])
               :finish-fn
               (fn [socket]
                 (if (= socket :socket-a)
                   (throw first-error)
                   net/connected))})
        established (@#'client/establish! :endpoint {} 10000000 ops)
        waits (mapv #(nth % 2)
                    (filter #(= :await (first %)) @events))]
    ;; Candidate b receives only the time left by candidate a. A relative
    ;; per-candidate timeout would have produced [10 10].
    (is (= [10 6] waits))
    (is (= :socket-b (:socket established)))
    (is (= :poller (:write-poller established)))
    (is (some #{[:remove :poller [:token :socket-a]]} @events))
    (is (some #{[:close :socket-a]} @events))
    (is (not (some #{[:close :poller]} @events)))
    (is (not (some #{[:close :socket-b]} @events)))))

(deftest synchronous-attempt-failure-still-advances-resolver-order
  ;; socket(2), option, and nonblocking setup can fail for one address family.
  ;; jolt.net has already rolled that attempt back, so the connector must still
  ;; try a later usable family rather than requiring :jolt.net/op :connect.
  (let [clock (atom 0)
        events (atom [])
        family-error (ex-info "address family unsupported"
                              {:jolt.net/op :socket :jolt.net/code 97})
        ops (fake-ops
              {:clock clock
               :events events
               :resolve-result [:ipv6 :ipv4]
               :try-connect-fn
               (fn [address _]
                 (if (= address :ipv6)
                   (throw family-error)
                   {:jolt.net/socket :socket-v4
                    :jolt.net/status net/connected}))
               :await-fn (fn [_] [])
               :finish-fn (fn [_] net/connected)})
        established (@#'client/establish! :endpoint {} 10000000 ops)]
    (is (= :socket-v4 (:socket established)))
    (is (some #(and (= :register (first %))
                    (= :socket-v4 (nth % 2)))
              @events))
    (is (not (some #{[:close :poller]} @events)))))

(deftest poller-failure-does-not-get-misclassified-as-a-candidate-failure
  (let [clock (atom 0)
        events (atom [])
        attempts (atom [])
        poll-error (ex-info "poll failed"
                            {:jolt.net/op :poll :jolt.net/code 9})
        ops (fake-ops
              {:clock clock
               :events events
               :resolve-result [:a :b]
               :try-connect-fn
               (fn [address _]
                 (swap! attempts conj address)
                 {:jolt.net/socket (if (= address :a) :socket-a :socket-b)
                  :jolt.net/status net/in-progress})
               :await-fn (fn [_] (throw poll-error))
               :finish-fn (fn [_] net/connected)})
        thrown
        (try
          (@#'client/establish! :endpoint {} 10000000 ops)
          nil
          (catch :default e e))]
    (is (identical? poll-error thrown))
    (is (= [:a] @attempts))
    (is (some #{[:close :socket-a]} @events))
    (is (some #{[:close :poller]} @events))))

(deftest timeout-closes-current-attempt-and-retains-last-real-cause
  (let [clock (atom 0)
        events (atom [])
        first-error (ex-info "candidate a failed"
                             {:jolt.net/op :connect :jolt.net/code 111})
        ops (fake-ops
              {:clock clock
               :events events
               :resolve-result [:a :b]
               :try-connect-fn
               (fn [address _]
                 (if (= address :a)
                   (throw first-error)
                   {:jolt.net/socket :socket-b
                    :jolt.net/status net/in-progress}))
               :await-fn
               (fn [_]
                 (reset! clock 10000000)
                 [])
               :finish-fn (fn [_] net/in-progress)})
        thrown
        (try
          (@#'client/establish! :endpoint {} 10000000 ops)
          nil
          (catch :default e e))]
    (is (= ::client/connect-timeout (:err (ex-data thrown))))
    (is (identical? first-error (ex-cause thrown)))
    (is (some #{[:remove :poller [:token :socket-b]]} @events))
    (is (some #{[:close :socket-b]} @events))
    (is (some #{[:close :poller]} @events))))

(deftest deadline-expiry-during-resolution-or-completion-is-still-timeout
  (testing "an already-expired deadline does not start synchronous resolution"
    (let [clock (atom 10000000)
          events (atom [])
          resolver-called? (atom false)
          ops (assoc
                (fake-ops
                  {:clock clock
                   :events events
                   :resolve-result []
                   :try-connect-fn (fn [_ _] nil)
                   :await-fn (fn [_] [])
                   :finish-fn (fn [_] net/in-progress)})
                :resolve
                (fn [_ _]
                  (reset! resolver-called? true)
                  []))
          thrown
          (try
            (@#'client/establish! :endpoint {} 10000000 ops)
            nil
            (catch :default e e))]
      (is (= ::client/connect-timeout (:err (ex-data thrown))))
      (is (false? @resolver-called?))
      (is (not (some #{[:open :poller]} @events)))))

  (testing "a resolver failure after the absolute deadline is retained as cause"
    (let [clock (atom 0)
          events (atom [])
          resolver-error (ex-info "resolver failed"
                                  {:jolt.net/op :resolve
                                   :jolt.net/code -2})
          ops (assoc
                (fake-ops
                  {:clock clock
                   :events events
                   :resolve-result []
                   :try-connect-fn (fn [_ _] nil)
                   :await-fn (fn [_] [])
                   :finish-fn (fn [_] net/in-progress)})
                :resolve
                (fn [_ _]
                  (reset! clock 10000000)
                  (throw resolver-error)))
          thrown
          (try
            (@#'client/establish! :endpoint {} 10000000 ops)
            nil
            (catch :default e e))]
      (is (= ::client/connect-timeout (:err (ex-data thrown))))
      (is (identical? resolver-error (ex-cause thrown)))
      (is (not (some #{[:open :poller]} @events)))))

  (testing "a completion error observed at the deadline is the timeout cause"
    (let [clock (atom 0)
          events (atom [])
          completion-error (ex-info "connection refused"
                                    {:jolt.net/op :connect
                                     :jolt.net/code 111})
          ops (fake-ops
                {:clock clock
                 :events events
                 :resolve-result [:a]
                 :try-connect-fn
                 (fn [_ _]
                   {:jolt.net/socket :socket-a
                    :jolt.net/status net/in-progress})
                 :await-fn
                 (fn [_]
                   (reset! clock 10000000)
                   [{:events #{:error}}])
                 :finish-fn (fn [_] (throw completion-error))})
          thrown
          (try
            (@#'client/establish! :endpoint {} 10000000 ops)
            nil
            (catch :default e e))]
      (is (= ::client/connect-timeout (:err (ex-data thrown))))
      (is (identical? completion-error (ex-cause thrown)))
      (is (some #{[:close :socket-a]} @events))
      (is (some #{[:close :poller]} @events)))))

(deftest exhaustion-throws-the-last-real-candidate-error
  (let [clock (atom 0)
        events (atom [])
        first-error (ex-info "candidate a failed"
                             {:jolt.net/op :connect :jolt.net/code 101})
        last-error (ex-info "candidate b failed"
                            {:jolt.net/op :connect :jolt.net/code 111})
        ops (fake-ops
              {:clock clock
               :events events
               :resolve-result [:a :b]
               :try-connect-fn
               (fn [address _]
                 (if (= address :a)
                   (throw first-error)
                   {:jolt.net/socket :socket-b
                    :jolt.net/status net/in-progress}))
               :await-fn (fn [_] [{:events #{:error}}])
               :finish-fn (fn [_] (throw last-error))})
        thrown
        (try
          (@#'client/establish! :endpoint {} 10000000 ops)
          nil
          (catch :default e e))]
    (is (identical? last-error thrown))
    (is (some #{[:close :socket-b]} @events))
    (is (some #{[:close :poller]} @events))))

(deftest connection-construction-rolls-back-transferred-ownership
  (testing "failure to create the read poller closes transferred ownership"
    (let [closed (atom [])]
      (with-redefs [net/open-poller (fn [] (throw (ex-info "no poller" {})))
                    net/close! (fn [owned] (swap! closed conj owned) true)]
        (is (some?
              (try
                (@#'client/build-connection!
                  {:socket :socket :write-poller :write-poller})
                nil
                (catch :default e e))))
        (is (= [:write-poller :socket] @closed)))))

  (testing "failure after read registration closes all three owned resources"
    (let [closed (atom [])
          endpoint-error (ex-info "endpoint inspection failed" {})]
      (with-redefs [net/open-poller (fn [] :read-poller)
                    net/register! (fn [_ _ _] :read-token)
                    net/local-endpoint (fn [_] (throw endpoint-error))
                    net/close! (fn [owned] (swap! closed conj owned) true)]
        (let [thrown
              (try
                (@#'client/build-connection!
                  {:socket :socket :write-poller :write-poller})
                nil
                (catch :default e e))]
          (is (identical? endpoint-error thrown))
          (is (= [:read-poller :write-poller :socket] @closed)))))))

(deftest translate-closed-native-helper
  (let [poller-closed-ex
        (ex-info "jolt.net await-ready: poller is closed"
                 {:jolt.net/op :await-ready
                  :jolt.net/kind :invalid
                  :jolt.net/platform :posix
                  :jolt.net/message "poller is closed"})]
    (testing "a native error during receive after close begins is canonicalized"
      (let [lifecycle (atom {:phase :closing})
            thrown
            (thrown-by
              #(@#'client/translate-closed-native!
                 lifecycle :receive
                 (fn [] (throw poller-closed-ex))))]
        (is (= ::client/closed (:err (ex-data thrown))))
        (is (= :receive (:teensyp.client/op (ex-data thrown))))
        (is (= :closed (:teensyp.client/kind (ex-data thrown))))
        (is (= "teensyp.client receive: connection is closed"
               (ex-message thrown)))
        ;; No jolt.net data leaks through the public client boundary.
        (is (not (contains? (ex-data thrown) :jolt.net/op)))
        (is (= :closing (:phase @lifecycle)))))
    (testing "a native error during send after close completes is canonicalized"
      (let [lifecycle (atom {:phase :closed})
            thrown
            (thrown-by
              #(@#'client/translate-closed-native!
                 lifecycle :send
                 (fn [] (throw poller-closed-ex))))]
        (is (= ::client/closed (:err (ex-data thrown))))
        (is (= :send (:teensyp.client/op (ex-data thrown))))
        (is (= :closed (:teensyp.client/kind (ex-data thrown))))
        (is (= "teensyp.client send: connection is closed"
               (ex-message thrown)))))
    (testing "a native reset observed while lifecycle is still open is preserved"
      ;; The contract: only the terminal-close race is translated. A real native
      ;; error on a live connection must surface identically, with its cause and
      ;; jolt.net data intact, so callers can distinguish reset/EOF/etc.
      (let [lifecycle (atom {:phase :open})
            reset-ex
            (ex-info "connection reset"
                     {:jolt.net/op :read
                      :jolt.net/kind :connection-reset
                      :jolt.net/code 104})
            thrown
            (thrown-by
              #(@#'client/translate-closed-native!
                 lifecycle :receive
                 (fn [] (throw reset-ex))))]
        (is (identical? reset-ex thrown))
        (is (= :connection-reset
               (:jolt.net/kind (ex-data thrown))))))))

(defn- loopback-handler
  ([_socket] nil)
  ([state socket buffer]
   (let [n (buf/remaining buffer)]
     (when (pos? n)
       (tcp/write socket (buf/wrap (buf/get-bytes! buffer n))))
     (when (tcp/peer-eof-notified? socket)
       (tcp/close socket))
     state))
  ([_state _exception] nil))

(deftest public-client-loopback-contract
  (let [server (tcp/run-server :port 0
                               :handler loopback-handler
                               :reuse-address? true)
        connection (client/connect "127.0.0.1" (:port server)
                                   {:connect-timeout-ms 2000
                                    :no-delay? true})]
    (try
      (let [payload (.getBytes "client-loopback" "UTF-8")]
        (is (client/connection? connection))
        (is (= :open (:state (client/connection-info connection))))
        (is (= (:port server)
               (get-in (client/connection-info connection)
                       [:remote-address :port])))
        (is (not (contains? (client/connection-info connection) :fd)))
        (is (not (contains? (client/connection-info connection)
                            :jolt.net/raw)))
        (let [send-timeout
              (thrown-by
                #(client/send-all! connection payload
                                   {:deadline-nanos 0}))
              receive-timeout
              (thrown-by
                #(client/receive-at-most! connection 1
                                          {:deadline-nanos 0}))]
          (is (= [:send ::client/timed-out]
                 [(:teensyp.client/op (ex-data send-timeout))
                  (:err (ex-data send-timeout))]))
          (is (= [:receive ::client/timed-out]
                 [(:teensyp.client/op (ex-data receive-timeout))
                  (:err (ex-data receive-timeout))])))
        ;; Start the read first: a persistent read poller must not serialize or
        ;; deadlock the independent write direction.
        (let [reader-started (promise)
              read-buffer (byte-array 128)
              reading (future
                        (deliver reader-started true)
                        (let [n (client/receive-into!
                                  connection read-buffer 0 128
                                  {:timeout-ms 2000})]
                          (when n
                            (java.util.Arrays/copyOfRange
                              read-buffer 0 (int n)))))]
          (deref reader-started 1000 :timeout)
          (client/send-all! connection payload 0 (alength payload)
                            {:timeout-ms 2000})
          (let [received (deref reading 2000 :timeout)]
            (is (bytes? received))
            (is (= "client-loopback"
                   (when (bytes? received)
                     (String. received "UTF-8"))))))
        (is (true? (client/shutdown-write! connection)))
        (is (false? (client/shutdown-write! connection)))
        (is (true? (:write-shutdown? (client/connection-info connection))))
        (is (= ::client/write-shutdown
               (:err
                 (ex-data
                   (try
                     (client/send-all! connection payload)
                     nil
                     (catch :default e e))))))
        (is (nil? (client/receive-at-most! connection 128
                                           {:timeout-ms 2000})))
        (is (true? (:read-eof? (client/connection-info connection))))
        (is (= 0 (alength (client/receive-at-most! connection 0)))))
      (finally
        (is (true? (client/close! connection)))
        (is (= ::client/closed
               (:err
                 (ex-data
                   (thrown-by
                     #(client/receive-at-most! connection 1
                                               {:deadline-nanos 0}))))))
        (is (false? (client/close! connection)))
        (is (client/closed? connection))
        (is (= :closed (:state (client/connection-info connection))))
        (tcp/stop-server server)))))

(deftest blocked-native-waits-translate-concurrent-close
  ;; Causally enter each client-owned await boundary before closing. This
  ;; proves the send and receive wiring, not only the helper in isolation.
  (doseq [[op expected-poller]
          [[:receive :read-poller]
           [:send :write-poller]]]
    (testing (str (name op) " translates a close that wakes await-ready")
      (let [await-entered (promise)
            poller-closed {:read-poller (promise)
                           :write-poller (promise)}
            closed (atom [])
            poller-error
            (ex-info "jolt.net await-ready: poller is closed"
                     {:jolt.net/op :await-ready
                      :jolt.net/kind :invalid})]
        (with-redefs
          [net/open-poller (fn [] :read-poller)
           net/register! (fn [_poller _socket _events] :read-token)
           net/local-endpoint
           (fn [_]
             {:jolt.net/host "127.0.0.1"
              :jolt.net/port 1000
              :jolt.net/family :ipv4})
           net/peer-endpoint
           (fn [_]
             {:jolt.net/host "127.0.0.1"
              :jolt.net/port 2000
              :jolt.net/family :ipv4})
           net/try-read-bytes!
           (fn [_socket _dest _off _len] net/would-block)
           net/try-write-bytes!
           (fn [_socket _src _off _len] net/would-block)
           net/await-ready
           (fn [poller _timeout-ms]
             (deliver await-entered poller)
             (when (= ::close-timeout
                      (deref (get poller-closed poller)
                             2000 ::close-timeout))
               (throw (ex-info "test close did not reach poller"
                               {:poller poller})))
             (throw poller-error))
           net/close!
           (fn [owned]
             (swap! closed conj owned)
             (when-let [closed-signal (get poller-closed owned)]
               (deliver closed-signal true))
             true)]
          (let [connection
                (@#'client/build-connection!
                  {:socket :socket :write-poller :write-poller})]
            (try
              (let [operation
                    (future
                      (try
                        (case op
                          :receive
                          (client/receive-at-most! connection 1)

                          :send
                          (client/send-all! connection (byte-array 1)))
                        (catch :default e e)))
                    entered (deref await-entered 2000 ::await-timeout)]
                (is (= expected-poller entered))
                (is (true? (client/close! connection)))
                (let [outcome (deref operation 2000 ::operation-timeout)]
                  (is (not= ::operation-timeout outcome))
                  (is (= ::client/closed (:err (ex-data outcome))))
                  (is (= op (:teensyp.client/op (ex-data outcome))))
                  (is (= :closed (:teensyp.client/kind (ex-data outcome))))
                  (is (not (contains? (ex-data outcome) :jolt.net/op)))
                  (is (= [:read-poller :write-poller :socket] @closed))))
              (finally
                ;; Idempotent cleanup remains inside the redefined native seam.
                (client/close! connection)))))))))
