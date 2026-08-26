(ns teensyp.server-test
  "Framework-less acceptance tests driven over real loopback TCP. Each scenario
  starts a server, drives it with the blocking client in teensyp.ffi-net, and
  asserts the bytes on the wire. `-main` exits non-zero if anything fails, so
  `jolt -M:test` gates the released-runtime migration."
  (:require [clojure.string :as str]
            [clojure.test]
            [teensyp.server :as tcp]
            [teensyp.buffer :as buf]
            [teensyp.stream :as stream]
            [teensyp.client :as client]
            [teensyp.ffi-net :as net]
            [jolt.net :as jnet]
            ;; loaded for their side effects on the run: the deftests below are
            ;; discovered by clojure.test/run-tests, the properties by run-properties!
            [teensyp.client-test]
            [teensyp.buffer-property-test]
            [teensyp.server-property-test]))

(def ^:private failures (atom 0))

(declare no-op-handler)

(defn- check [label expected actual]
  (if (= expected actual)
    (println "ok  " label)
    (do (swap! failures inc)
        (println "FAIL" label "\n   expected:" (pr-str expected) "\n   actual:  " (pr-str actual)))))

(defn- utf8 [s] (.getBytes ^String s "UTF-8"))
(defn- ->str [^bytes b] (when b (String. b "UTF-8")))

(defn- recv-until-eof
  "Read from a test-client socket until the peer closes; return the accumulated
  String."
  [socket]
  (loop [acc ""]
    (if-let [b (net/client-recv socket 4096)]
      (recur (str acc (->str b)))
      acc)))

(defn- recv-until-size
  "Read until at least `n` UTF-8 bytes arrive, bounded by `timeout-ms`.

  Unlike one client-recv after a sleep, this does not assume that two queued
  writes are coalesced into one native read."
  [fd n timeout-ms]
  (deref
   (future
     (loop [acc ""]
       (if (>= (alength (utf8 acc)) (long n))
         acc
         (if-let [b (net/client-recv fd 4096)]
           (recur (str acc (->str b)))
           acc))))
   timeout-ms :TIMEOUT))

(defn- with-server [handler f]
  ;; Keep the original scenarios on randomized explicit ports; newer lifecycle
  ;; tests use port 0 and assert the jolt.net-reported bound endpoint directly.
  (let [port (+ 18700 (rand-int 500))
        srv  (tcp/run-server :port port :handler handler :reuse-address? true)]
    (Thread/sleep 200)
    (try (f port) (finally (tcp/stop-server srv) (Thread/sleep 100)))))

;; --- handlers --------------------------------------------------------------
(defn- echo-handler
  ([_sock] {})
  ([state sock b]
   (let [n (buf/remaining b)]
     (when (pos? n) (tcp/write sock (buf/wrap (buf/get-bytes! b n))))
     state))
  ([_state _ex] nil))

(defn- line-reverse-handler
  ([_sock] nil)
  ([_ sock b]
   (loop []
     (when-let [line (buf/read-line b "UTF-8")]
       (tcp/write sock (buf/str->buffer (str (str/join (reverse line)) "\n") "UTF-8"))
       (recur)))
   nil)
  ([_ _] nil))

(defn- doubler-handler
  ([_sock] nil)
  ([_ sock b]
   (loop []
     (when-let [line (buf/read-line b "UTF-8")]
       (let [t (str/trim line)]
         (when (seq t) (tcp/write sock (buf/str->buffer (str (* 2 (parse-long t)) "\n") "UTF-8"))))
       (recur)))
   nil)
  ([_ _] nil))

(defn- greet-close-handler
  ([sock] (tcp/write sock (buf/str->buffer "bye\n" "UTF-8")) (tcp/close sock) nil)
  ([s _ _] s)
  ([_ _] nil))

(defn- callback-chain-handler
  ;; write one\n, then two\n only after one\n fully flushed, then close.
  ([sock] (tcp/write sock (buf/str->buffer "one\n" "UTF-8")
                     (fn [] (tcp/write sock (buf/str->buffer "two\n" "UTF-8")
                                       (fn [] (tcp/close sock)))))
          nil)
  ([s _ _] s)
  ([_ _] nil))

(defn- pause-resume-echo-handler
  ;; pause reads on accept, resume after a delay: data sent during the pause
  ;; must still be echoed once reads resume.
  ([sock] (tcp/pause-reads sock)
          (future (Thread/sleep 150) (tcp/resume-reads sock))
          {})
  ([state sock b]
   (let [n (buf/remaining b)]
     (when (pos? n) (tcp/write sock (buf/wrap (buf/get-bytes! b n))))
     state))
  ([_ _] nil))

;; --- scenarios -------------------------------------------------------------
(defn- test-echo []
  (with-server echo-handler
    (fn [port]
      (let [fd (net/connect-loopback port)]
        (net/client-send-all fd (utf8 "hello"))
        (Thread/sleep 100)
        (check "echo hello" "hello" (->str (net/client-recv fd 4096)))
        (net/client-send-all fd (utf8 "again!"))
        (Thread/sleep 100)
        (check "echo again" "again!" (->str (net/client-recv fd 4096)))
        (net/close! fd)))))

(defn- test-line-reverse-partial []
  (with-server line-reverse-handler
    (fn [port]
      (let [fd (net/connect-loopback port)]
        ;; two complete lines at once
        (net/client-send-all fd (utf8 "abc\ndef\n"))
        (Thread/sleep 100)
        (check "reverse two lines" "cba\nfed\n" (->str (net/client-recv fd 4096)))
        ;; a line split across two sends (partial-frame handling)
        (net/client-send-all fd (utf8 "par"))
        (Thread/sleep 80)
        (net/client-send-all fd (utf8 "tial\n"))
        (Thread/sleep 100)
        (check "reverse split line" "laitrap\n" (->str (net/client-recv fd 4096)))
        (net/close! fd)))))

(defn- test-doubler []
  (with-server doubler-handler
    (fn [port]
      (let [fd (net/connect-loopback port)]
        (net/client-send-all fd (utf8 "21\n4\n"))
        (Thread/sleep 100)
        (check "doubler" "42\n8\n" (->str (net/client-recv fd 4096)))
        (net/close! fd)))))

(defn- test-write-then-close []
  (with-server greet-close-handler
    (fn [port]
      (let [fd (net/connect-loopback port)]
        (check "greet then EOF" "bye\n" (recv-until-eof fd))
        (net/close! fd)))))

(defn- test-callback-chain []
  (with-server callback-chain-handler
    (fn [port]
      (let [fd (net/connect-loopback port)]
        (check "callback ordering" "one\ntwo\n" (recv-until-eof fd))
        (net/close! fd)))))

(defn- test-pause-resume []
  (with-server pause-resume-echo-handler
    (fn [port]
      (let [fd (net/connect-loopback port)]
        (net/client-send-all fd (utf8 "buffered"))   ;; sent while reads are paused
        (Thread/sleep 400)                            ;; > resume delay
        (check "echo after resume" "buffered" (->str (net/client-recv fd 4096)))
        (net/close! fd)))))

;; teensyp.stream: a blocking line-echo consumer running on its own thread.
(defn- test-stream-lines []
  (with-server (stream/stream-handler
                (fn [conn]
                  (loop []
                    (when-let [line (stream/conn-read-line conn)]
                      (stream/conn-send conn (str "<" line ">\n"))
                      (recur)))))
    (fn [port]
      (let [fd (net/connect-loopback port)]
        (net/client-send-all fd (utf8 "hi\nyo\n"))
        (let [expected "<hi>\n<yo>\n"]
          (check "stream line-echo" expected
                 (recv-until-size fd (alength (utf8 expected)) 4000)))
        (net/close! fd)))))

;; Many simultaneous connections echoing distinct payloads through one server.
(defn- test-concurrent []
  (with-server echo-handler
    (fn [port]
      (let [n 12
            oks (mapv (fn [i]
                        (future
                          (let [fd  (net/connect-loopback port)
                                msg (str "client-" i "-payload")]
                            (net/client-send-all fd (utf8 msg))
                            (Thread/sleep 200)
                            (let [r (->str (net/client-recv fd 4096))]
                              (net/close! fd)
                              (= msg r)))))
                      (range n))]
        (check "12 concurrent echoes" true (every? true? (mapv deref oks)))))))

;; A reactor-side exception must cost only the connection that caused it.
;; Regression test: the reactor loop used to have no catch, so a throw from
;; handle-read/handle-write escaped into the `future` running the reactor and
;; was swallowed — the server silently stopped serving every connection, with
;; `running?` still true and nothing logged.
(defn- test-reactor-survives-connection-error []
  (let [errs  (atom 0)
        conns (atom 0)
        port  (+ 18700 (rand-int 500))
        ;; The first connection queues a write and then corrupts the queued
        ;; buffer, so the reactor throws while trying to send it.
        handler (fn
                  ([sock]
                   (if (= 1 (swap! conns inc))
                     (let [b (buf/wrap (utf8 "hello"))]
                       (tcp/write sock b)
                       (buf/set-position! b -1))
                     (do (tcp/write sock (buf/str->buffer "OK\n" "UTF-8"))
                         (tcp/close sock)))
                   nil)
                  ([s _ _] s)
                  ([_ _] nil))
        srv (tcp/run-server :port port :reuse-address? true
                            :error-logger (fn [_] (swap! errs inc))
                            :handler handler)
        probe (fn []
                (let [fd (net/connect-loopback port)]
                  (try (let [r (deref (future (net/client-recv fd 128)) 2500 :TIMEOUT)]
                         (if (= r :TIMEOUT) "TIMEOUT" (->str r)))
                       (finally (net/close! fd)))))]
    (Thread/sleep 250)
    (try
      (probe)                                    ; poison the reactor
      (check "reactor error was reported" true (pos? @errs))
      (check "server still serves after reactor error" "OK\n" (probe))
      (check "and keeps serving" "OK\n" (probe))
      (finally (tcp/stop-server srv) (Thread/sleep 150)))))

;; A peer that half-closes its write side is still waiting for a response.
;; Regression test: EOF used to close the connection outright, discarding
;; whatever the handler was about to write — so a client that sent a request and
;; then shut down its write side (an ordinary pattern) got nothing back.
(defn- test-half-close-still-answered []
  (let [port (+ 18700 (rand-int 500))
        ;; Replies only after EOF, from a worker thread, which is the case the
        ;; reactor cannot detect on its own: nothing is queued and no handler
        ;; arity is running, yet a response is still owed.
        handler (fn
                  ([sock]
                   (future (Thread/sleep 120)
                           (tcp/write sock (buf/str->buffer "late-reply\n" "UTF-8"))
                           (tcp/close sock))
                   nil)
                  ([s _ _] s)
                  ([_ _] nil))
        srv (tcp/run-server :port port :reuse-address? true :handler handler
                            :error-logger (fn [_]))]
    (Thread/sleep 250)
    (try
      (let [fd (net/connect-loopback port)]
        (net/client-send-all fd (utf8 "request"))
        (net/shutdown-write! fd)                 ; half-close, then wait
        (check "half-closed peer still gets its response"
               "late-reply\n" (->str (net/client-recv fd 4096)))
        (net/close! fd))
      (check "peer-closed? is observable" true
             (boolean (some? (resolve 'teensyp.server/peer-closed?))))
      (finally (tcp/stop-server srv) (Thread/sleep 150)))))

;; Found by the generative echo property (teensyp.server-property-test): a client
;; that half-closes while the handler is mid-call used to get back MORE bytes than
;; it sent, and sometimes nothing at all.
;;
;; Both are the same race. EOF was detected while WORKING was set, so
;; (a) handle-read had not compacted the consumed prefix and handle-eof! rebuilt
;;     the read-view over bytes the handler had already consumed and echoed, and
;; (b) if WORKING was still set inside handle-eof!, the EOF notification was
;;     dropped entirely — nothing else delivers it, so a handler waiting for
;;     peer-closed? before closing hung the connection until the client gave up.
;;
;; The sleep makes the race deterministic: EOF always lands while the handler is
;; working. Without the fix this both duplicates bytes and hangs.
(defn- slow-echo-close-on-eof-handler
  ([_sock] nil)
  ([state sock b]
   (let [n (buf/remaining b)]
     (when (pos? n)
       (Thread/sleep 150)                    ; EOF arrives during this window
       (tcp/write sock (buf/wrap (buf/get-bytes! b n))))
     (when (tcp/peer-closed? sock) (tcp/close sock))
     state))
  ([_state _ex] nil))

(defn- test-half-close-during-handler []
  (with-server slow-echo-close-on-eof-handler
    (fn [port]
      (let [fd (net/connect-loopback port)]
        (net/client-send-all fd (utf8 "payload-1234567890"))
        (net/shutdown-write! fd)             ; half-close while the handler sleeps
        (let [r (deref (future (recv-until-eof fd)) 4000 :TIMEOUT)]
          (check "half-close during handler echoes exactly once, then closes"
                 "payload-1234567890" r))
        (net/close! fd)))))

;; Regression: a client that half-closes used to hang forever against a stream
;; handler. teensyp.stream's read arity ignored peer-closed?, so the core.async
;; channel was never closed; conn-read-line blocked on a channel nothing would
;; ever feed again, `f` never returned, and its (finally (tcp/close sock)) never
;; ran. The connection was closed for us on EOF, so nothing else broke the cycle.
(defn- test-stream-half-close []
  (with-server (stream/stream-handler
                (fn [conn]
                  (loop []
                    (when-let [line (stream/conn-read-line conn)]
                      (stream/conn-send conn (str "<" line ">\n"))
                      (recur)))))
    (fn [port]
      (let [fd (net/connect-loopback port)]
        (net/client-send-all fd (utf8 "hi\nyo\n"))
        (net/shutdown-write! fd)             ; half-close, then wait for EOF
        (check "stream handler ends the stream on half-close"
               "<hi>\n<yo>\n" (deref (future (recv-until-eof fd)) 4000 :TIMEOUT))
        (net/close! fd)))))

;; --- EOF notification contract --------------------------------------------
;; peer-closed? and peer-eof-notified? answer different questions, and a
;; protocol that releases connections needs the second one. This pins the
;; boundary between them:
;;
;;   1. while an older handler invocation is still running, EOF may already be
;;      observed — peer-closed? true, peer-eof-notified? false;
;;   2. in the terminal invocation that follows, both are true and every byte
;;      received before EOF is visible in the view that invocation was handed.
;;
;; Getting (1) wrong is how a server closes a connection whose buffered requests
;; it has not parsed yet; jolt-http had exactly that bug.
(defn- test-eof-notification-contract []
  (let [observations (atom [])
        release      (promise)
        handler
        (fn
          ([_sock] {:seen 0})
          ([state sock b]
           (let [n (buf/remaining b)
                 s (when (pos? n) (String. (buf/get-bytes! b n) "UTF-8"))]
             (when (zero? (long (:seen state)))
               ;; Hold this invocation open past the peer's half-close, so the
               ;; reactor observes EOF while an older view is still in hand.
               (deref release 3000 :timeout))
             ;; Flags are read at the *end* of the invocation, which is the
             ;; interesting moment: by now the peer has gone, but this
             ;; invocation began before it did.
             (swap! observations conj {:bytes s
                                       :closed? (tcp/peer-closed? sock)
                                       :notified? (tcp/peer-eof-notified? sock)})
             (when (tcp/peer-eof-notified? sock) (tcp/close sock))
             (update state :seen inc)))
          ([_state _ex] nil))]
    (with-server handler
      (fn [port]
        (let [fd (net/connect-loopback port)]
          (net/client-send-all fd (utf8 "first"))
          (Thread/sleep 150)                    ; let the first invocation start
          (net/client-send-all fd (utf8 "second"))
          (net/shutdown-write! fd)              ; EOF while the handler is busy
          (Thread/sleep 150)
          (deliver release :go)
          (deref (future (recv-until-eof fd)) 4000 :TIMEOUT)
          (net/close! fd))))
    (let [obs @observations
          busy (first obs)
          term (last obs)]
      (check "EOF contract: an invocation ran before the terminal one"
             true (>= (count obs) 2))
      (check "EOF contract: during the older invocation, EOF is observed but not notified"
             [true false] [(:closed? busy) (:notified? busy)])
      (check "EOF contract: the terminal invocation sees both"
             [true true] [(:closed? term) (:notified? term)])
      ;; Nothing received before EOF may be hidden behind the notification.
      (check "EOF contract: all pre-EOF bytes are visible by the terminal invocation"
             "firstsecond" (apply str (keep :bytes obs))))))

;; --- jolt.net migration invariants -----------------------------------------

(defn- test-socket-info-and-generation []
  (let [accepted (promise)
        closed   (promise)
        handler  (fn
                   ([sock] (deliver accepted sock) nil)
                   ([state _sock _buffer] state)
                   ([_state _ex] (deliver closed true)))
        srv      (tcp/run-server :port 0 :handler handler :reuse-address? true)
        client   (net/connect-loopback (:port srv))]
    (try
      (let [sock (deref accepted 2000 :timeout)
            info (tcp/socket-info sock)
            token @(:token sock)
            generation (:jolt.net/generation token)
            client-local (:local-address (client/connection-info client))
            client-peer (:remote-address (client/connection-info client))]
        (check "accepted Context owns its jolt.net socket"
               true (true? (:jolt.net/handle (:socket sock))))
        (check "connection registry is keyed by stable handle generation"
               true (identical? sock (get @(:conns (:srv srv)) generation)))
        (check "Context generation agrees with its current token"
               generation (:generation sock))
        (check "socket-info reports the real local endpoint"
               client-peer (:local-address info))
        (check "socket-info reports the real peer endpoint"
               client-local (:remote-address info))
        (check "socket-info retains the raw descriptor as diagnostics"
               (jnet/native-handle (:socket sock)) (:fd info))
        (tcp/close sock)
        (deref closed 2000 :timeout))
      (finally
        (net/close! client)
        (tcp/stop-server srv)))))

(defn- test-write-callback-is-buffer-release-fence []
  (let [written  (atom nil)
        callback (promise)
        handler
        (fn
          ([sock]
           (let [buffer (buf/str->buffer "owned-until-callback" "UTF-8")]
             (reset! written buffer)
             (tcp/write
              sock buffer
              (fn []
                (deliver callback
                         {:same? (identical? buffer @written)
                          :position (buf/position buffer)
                          :limit (buf/limit buffer)})
                (tcp/close sock))))
           nil)
          ([state _sock _buffer] state)
          ([_state _ex] nil))
        srv (tcp/run-server :port 0 :handler handler :reuse-address? true)
        client (net/connect-loopback (:port srv))]
    (try
      (check "queued Buffer bytes arrive before its callback"
             "owned-until-callback" (recv-until-eof client))
      (let [{:keys [same? position limit]}
            (deref callback 2000 {:same? false :position -1 :limit -2})]
        (check "write callback releases the exact queued Buffer" true same?)
        (check "write callback observes position == limit"
               limit position))
      (finally
        (net/close! client)
        (tcp/stop-server srv)))))

(defn- test-callbacks-cannot-deadlock-handler-pool []
  (let [pool-size 3
        callback-results (atom [])
        handler
        (fn
          ([sock]
           (let [done (promise)]
             (tcp/write sock (buf/str->buffer "x" "UTF-8")
                        #(deliver done :written))
             (let [result (deref done 3000 :timeout)]
               (swap! callback-results conj result)
               (tcp/close sock)))
           nil)
          ([state _sock _buffer] state)
          ([_state _ex] nil))
        srv (tcp/run-server :port 0 :handler handler :reuse-address? true
                            :pool-size pool-size)
        clients (mapv (fn [_] (net/connect-loopback (:port srv)))
                      (range pool-size))]
    (try
      (check "callbacks release exactly pool-size blocked handlers"
             (vec (repeat pool-size "x"))
             (mapv recv-until-eof clients))
      (check "no blocked handler timed out waiting for its callback"
             #{:written} (set @callback-results))
      (finally
        (doseq [client clients] (net/close! client))
        (tcp/stop-server srv)))))

(defn- test-active-task-tracking-has-a-stable-empty-barrier []
  (let [handler-ex (java.util.concurrent.Executors/newSingleThreadExecutor)
        callback-ex (java.util.concurrent.Executors/newSingleThreadExecutor)
        callbacks (@#'tcp/task-tracker)
        closes (@#'tcp/task-tracker)
        srv {:executor handler-ex
             :callback-executor callback-ex
             :callback-tasks callbacks
             :opts {:error-logger (fn [_] nil)}}
        callback-count (atom 0)
        close-count (atom 0)
        started (promise)
        release (promise)]
    (try
      (dotimes [_ 100]
        (deref (@#'tcp/submit-callback srv #(swap! callback-count inc))))
      (dotimes [_ 100]
        (deref (@#'tcp/track-task! srv handler-ex closes
                                  #(swap! close-count inc))))
      (check "completed callbacks are removed from task tracking"
             [100 #{}] [@callback-count (:active @callbacks)])
      (check "completed close arities are removed from task tracking"
             [100 #{}] [@close-count (:active @closes)])
      (@#'tcp/submit-callback
       srv
       #(do (deliver started true) (deref release)))
      (deref started 1000 :timeout)
      (let [waiting
            (future
              (@#'tcp/freeze-and-await-tasks! callbacks)
              :empty)]
        (check "task cleanup waits for the active callback"
               :waiting (deref waiting 25 :waiting))
        (deliver release :go)
        (check "task cleanup reaches a stable empty set"
               :empty (deref waiting 1000 :timeout))
        (check "task tracker is frozen and empty at the cleanup boundary"
               {:accepting? false :active #{}}
               @callbacks)
        (let [late-ran (atom false)
              late-done (@#'tcp/submit-callback
                         srv #(reset! late-ran true))]
          (check "a post-freeze callback takes the synchronous fallback"
                 [true :done] [@late-ran (deref late-done 0 :timeout)])
          (check "post-freeze fallback cannot reopen or grow the tracker"
                 {:accepting? false :active #{}}
                 @callbacks)))
      (finally
        (deliver release :go)
        (.shutdown handler-ex)
        (.shutdown callback-ex)
        (.awaitTermination handler-ex 2000)
        (.awaitTermination callback-ex 2000)))))

(defn- test-callback-executor-contract []
  (let [shared (java.util.concurrent.Executors/newSingleThreadExecutor)]
    (try
      (let [err (try
                  (tcp/run-server :port 0 :handler no-op-handler
                                  :executor shared
                                  :callback-executor shared)
                  nil
                  (catch :default e e))
            usable (promise)]
        (check "identical handler/callback executors are rejected"
               ::tcp/shared-handler-callback-executor
               (:err (ex-data err)))
        (.execute shared #(deliver usable :borrowed))
        (check "a rejected borrowed shared executor is not shut down"
               :borrowed (deref usable 1000 :timeout)))
      (finally
        (.shutdown shared)
        (.awaitTermination shared 2000))))

  (let [srv (tcp/run-server :port 0 :handler no-op-handler
                            :callback-pool-size 1)
        first-started (promise)
        release-first (promise)
        second-started (promise)]
    (try
      (@#'tcp/submit-callback
       (:srv srv)
       #(do (deliver first-started true) (deref release-first)))
      (deref first-started 1000 :timeout)
      (@#'tcp/submit-callback (:srv srv)
                              #(deliver second-started true))
      (check "configured one-worker callback pool is fixed and bounded"
             :waiting (deref second-started 25 :waiting))
      (deliver release-first :go)
      (check "the fixed callback worker runs the next task after release"
             true (deref second-started 1000 false))
      (finally
        (deliver release-first :go)
        (tcp/stop-server srv)))))

(defn- test-cas-bounded-queue-admission-and-credit-rollback []
  (let [q (atom clojure.lang.PersistentQueue/EMPTY)
        gate (promise)
        offers (mapv (fn [n]
                       (future
                         (deref gate)
                         (@#'tcp/q-offer! q 1 n)))
                     (range 32))]
    (deliver gate :go)
    (let [accepted (count (filter true? (mapv deref offers)))]
      (check "CAS queue admission accepts at most its configured cap"
             [1 1] [accepted (count @q)])))

  (let [ctx (tcp/map->Context
             {:srv {:pending (atom #{}) :poller nil}
              :fd 7 :generation 11
              :lock (java.util.concurrent.locks.ReentrantLock.)
              :flags (atom 0)
              :write-queue (atom clojure.lang.PersistentQueue/EMPTY)
              :write-limit (atom 8)
              :write-queue-cap 0
              :write-admission (atom {:phase :open :active 0})})
        err (try
              (tcp/queue-write ctx (buf/wrap (byte-array 8)) nil)
              nil
              (catch :default e e))]
    (check "a lost write-slot race reports queue-full"
           ::tcp/write-queue-full (:err (ex-data err)))
    (check "failed slot admission rolls reserved byte credit back"
           [8 0] [@(:write-limit ctx) (count @(:write-queue ctx))])))

(defn- test-close-exception-state-is-atomic []
  (let [first-ex (ex-info "first" {:which :first})
        second-ex (ex-info "second" {:which :second})
        ctx (tcp/map->Context
             {:srv {:pending (atom #{}) :poller nil}
              :fd 9 :generation 12
              :lock (java.util.concurrent.locks.ReentrantLock.)
              :flags (atom 0)
              :write-admission (atom {:phase :open :active 0})
              :close-ex (atom nil)
              :close-callback (atom nil)})]
    (@#'tcp/handle-close ctx first-ex)
    (@#'tcp/handle-close ctx second-ex)
    (check "close preserves the first exception through atomic updates"
           first-ex @(:close-ex ctx))
    (check "close exception state supports an atomic compare-and-set"
           true (compare-and-set! (:close-ex ctx) first-ex first-ex))
    (let [late-write
          (try
            (tcp/write-completion ctx (buf/wrap (byte-array 1)))
            nil
            (catch :default e e))]
      (check "closed write admission remains a synchronous structured error"
             ::tcp/socket-closed (:err (ex-data late-write)))
      (check "closed write admission retains the first connection failure"
             first-ex (ex-cause late-write)))))

(defn- fake-endpoint [port]
  {:jolt.net/host 2130706433
   :jolt.net/port port
   :jolt.net/family :inet})

(defn- fake-accept-server [executor]
  {:listener :listener
   :poller :poller
   :running? (atom true)
   :admission (atom {:phase :open :active 0})
   :accept-gate (atom false)
   :conns (atom {})
   :pending (atom #{})
   :executor executor
   :opts {:accept-batch-size 3
          :handler no-op-handler
          :read-buffer-size 64
          :write-buffer-size 64
          :write-queue-size 4
          :control-queue-size 4
          :error-logger (fn [_] nil)}})

(defn- test-accept-batches-cancellation-and-registration-rollback []
  (let [executor (java.util.concurrent.Executors/newSingleThreadExecutor)
        srv (fake-accept-server executor)
        accepts (atom 0)]
    (try
      (with-redefs
        [jnet/try-accept
         (fn [_ _]
           (let [n (swap! accepts inc)] {:fake-socket n}))
         jnet/register!
         (fn [_ socket _]
           {:jolt.net/generation (:fake-socket socket)})
         jnet/native-handle (fn [socket] (:fake-socket socket))
         jnet/local-endpoint (fn [socket] (fake-endpoint (+ 2000 (:fake-socket socket))))
         jnet/peer-endpoint (fn [socket] (fake-endpoint (+ 3000 (:fake-socket socket))))
         jnet/wake! (fn [_] nil)]
        (@#'tcp/do-accept srv))
      (check "one readiness turn accepts no more than its configured batch"
             [3 3] [@accepts (count @(:conns srv))])
      (finally
        (.shutdown executor)
        (.awaitTermination executor 2000))))

  (let [executor (java.util.concurrent.Executors/newSingleThreadExecutor)
        srv (fake-accept-server executor)
        removed (atom 0)
        closed (atom 0)]
    (try
      (with-redefs
        [jnet/register!
         (fn [_ _ _]
           (reset! (:running? srv) false)
           {:jolt.net/generation 41})
         jnet/native-handle (fn [_] 41)
         jnet/local-endpoint (fn [_] (fake-endpoint 2041))
         jnet/peer-endpoint (fn [_] (fake-endpoint 3041))
         jnet/remove-registration! (fn [_ _] (swap! removed inc))
         jnet/close! (fn [_] (swap! closed inc))]
        (check "stop cancellation rejects a registered-but-unpublished accept"
               false (@#'tcp/admit-accepted! srv :cancelled-socket)))
      (check "cancelled accept rolls registration and socket ownership back"
             [1 1 0] [@removed @closed (count @(:conns srv))])
      (finally
        (.shutdown executor)
        (.awaitTermination executor 2000))))

  (let [entered (promise)
        release (promise)
        events (atom [])
        srv {:running? (atom true)
             :admission (atom {:phase :open :active 0})
             :accept-gate (atom false)
             :poller nil}
        publication
        (future
          (@#'tcp/with-accept-publication
           srv
           #(do
              (deliver entered true)
              (deref release)
              (swap! events conj :published)
              true)))
        _ (deref entered 1000 :timeout)
        stopping
        (future
          (let [result (@#'tcp/request-stop! srv)]
            (swap! events conj :stopped)
            result))]
    (check "stop waits behind an accept publication that linearized first"
           :waiting (deref stopping 25 :waiting))
    (deliver release :go)
    (check "the pre-stop publication completes"
           true (deref publication 1000 :timeout))
    (check "stop then closes publication admission"
           true (deref stopping 1000 :timeout))
    (check "accept publication and stop have one total order"
           [:published :stopped] @events)
    (@#'tcp/with-accept-publication
     srv #(swap! events conj :late-publication))
    (check "publication cannot begin after the stop CAS"
           [:published :stopped] @events))

  (let [srv {:poller :poller
             :running? (atom true)
             :admission (atom {:phase :open :active 0})
             :accept-gate (atom false)}
        closed (atom 0)
        err (with-redefs
              [jnet/register! (fn [_ _ _]
                                (throw (ex-info "register failed"
                                                {:err ::register-failed})))
               jnet/close! (fn [_] (swap! closed inc))]
              (try
                (@#'tcp/admit-accepted! srv :unregistered-socket)
                nil
                (catch :default e e)))]
    (check "registration failure is preserved"
           ::register-failed (:err (ex-data err)))
    (check "registration failure closes the accepted socket"
           1 @closed)))

(defn- test-direct-executor-stop-releases-accept-gate []
  ;; Executor permits a legal direct implementation. Its execute method runs on
  ;; the reactor call stack, so an accept handler that calls public stop must be
  ;; able to acquire accept-gate and reach stop's bounded wait. A watchdog makes
  ;; the historical self-deadlock a bounded test failure by releasing only that
  ;; stuck synthetic gate after the expected timeout has long elapsed.
  (let [server* (atom nil)
        handler-entered (promise)
        stop-outcome (promise)
        watchdog-released-gate? (atom false)
        direct-executor
        (reify java.util.concurrent.Executor
          (execute [_ task] (task)))
        handler
        (fn
          ([_sock]
           (deliver handler-entered true)
           (deliver stop-outcome
                    (try
                      ((:stop @server*))
                      :returned
                      (catch :default e e)))
           :accepted)
          ([state _sock _buffer] state)
          ([_state _ex] nil))
        srv (tcp/run-server :port 0 :handler handler :reuse-address? true
                            :executor direct-executor
                            :stop-timeout-ms 25)
        _ (reset! server* srv)
        watchdog
        (future
          (when (= true (deref handler-entered 1000 false))
            (Thread/sleep 100)
            (when (and (= :pending (deref stop-outcome 0 :pending))
                       @(:accept-gate (:srv srv)))
              (reset! watchdog-released-gate? true)
              (reset! (:accept-gate (:srv srv)) false)))
          :done)
        client (net/connect-loopback (:port srv))]
    (try
      (check "direct executor runs the accept handler"
             true (deref handler-entered 1000 false))
      (let [outcome (deref stop-outcome 2000 :timeout)]
        (check "inline handler stop reaches the documented bounded wait"
               ::tcp/stop-timeout (:err (ex-data outcome)))
        (check "inline handler stop never needs the watchdog to release accept-gate"
               false @watchdog-released-gate?))
      (check "direct-executor watchdog completes"
             :done (deref watchdog 2000 :timeout))
      (check "reactor cleanup completes after the inline handler returns"
             :stopped (deref (:stopped srv) 2000 :timeout))
      (check "a later stop observes the completed direct-executor shutdown"
             nil (tcp/stop-server srv))
      (finally
        (net/close! client)
        (when @(:running? srv)
          (try (tcp/stop-server srv) (catch :default _ nil)))))))

(defn- test-closed-shutdown-context-clears-write-readiness []
  (let [token {:jolt.net/generation 77}
        updates (atom [])
        ctx (tcp/map->Context
             {:generation 77
              :token (atom token)
              :interests (atom #{:write})
              :flags (atom (bit-or tcp/CLOSED tcp/WRITING))})
        srv {:conns (atom {77 ctx}) :poller :poller}]
    (with-redefs
      [jnet/update-registration!
       (fn [_ old-token interests]
         (swap! updates conj interests)
         old-token)]
      (@#'tcp/reconcile-shutdown-registration! srv ctx))
    (check "CLOSED shutdown contexts explicitly clear write readiness"
           [[#{}] #{}] [@updates @(:interests ctx)])))

(defn- test-write-completion-success-and-failure []
  (let [observed (promise)
        handler
        (fn
          ([sock]
           (let [outcome
                 (deref
                  (tcp/write-completion
                   sock (buf/str->buffer "completion-ok" "UTF-8"))
                  2000 {:status :timeout})]
             (deliver observed outcome)
             (tcp/close sock))
           nil)
          ([state _sock _buffer] state)
          ([_state _ex] nil))
        srv (tcp/run-server :port 0 :handler handler)
        client (net/connect-loopback (:port srv))]
    (try
      (check "write-completion success still writes every byte"
             "completion-ok" (recv-until-eof client))
      (check "write-completion settles success explicitly"
             {:status :written} (deref observed 1000 :timeout))
      (finally
        (net/close! client)
        (tcp/stop-server srv))))

  (let [synthetic (ex-info "synthetic native write failure"
                           {:err ::synthetic-write-failure})
        observed (promise)
        legacy-called? (atom false)
        handler
        (fn
          ([sock]
           (let [completion
                 (tcp/write-completion
                  sock (buf/str->buffer "must-fail" "UTF-8"))]
             (tcp/write sock (buf/str->buffer "legacy" "UTF-8")
                        #(reset! legacy-called? true))
             (deliver observed
                      (deref completion 2000 {:status :timeout})))
           nil)
          ([state _sock _buffer] state)
          ([_state _ex] nil))]
    (with-redefs
      [jnet/try-write-bytes!
       (fn [_ _ _ _] (throw synthetic))]
      (let [srv (tcp/run-server :port 0 :handler handler
                                :error-logger (fn [_] nil)
                                :stop-timeout-ms 3000)
            client (net/connect-loopback (:port srv))]
        (try
          (let [outcome (deref observed 2000 {:status :timeout})]
            (check "native write failure settles the outcome completion"
                   :failed (:status outcome))
            (check "outcome completion retains the native exception"
                   ::synthetic-write-failure
                   (:err (ex-data (:exception outcome))))
            (check "legacy zero-arg write callbacks remain success-only"
                   false @legacy-called?)
            (check "write-error handler can quiesce and retire"
                   nil (tcp/stop-server srv)))
          (finally
            (net/close! client)
            (when @(:running? srv) (tcp/stop-server srv))))))))

(defn- exercise-rejected-handler-submission [label executor]
  (let [errors   (atom [])
        closed   (promise)
        handler  (fn
                   ([_sock] :unexpected)
                   ([state _sock _buffer] state)
                   ([_state ex] (deliver closed ex)))
        srv      (tcp/run-server :port 0 :handler handler :reuse-address? true
                                 :executor executor
                                 :error-logger #(swap! errors conj %)
                                 :stop-timeout-ms 2000)
        client   (net/connect-loopback (:port srv))]
    (try
      (let [close-ex (deref closed 2000 :timeout)]
        (check (str label ": rejected handler submission is reported")
               true (pos? (count @errors)))
        (check (str label ": rejection reaches the close arity")
               true (and (not= :timeout close-ex) (some? close-ex)))
        (check (str label ": rejection retires its Context")
               0 (count @(:conns (:srv srv))))
        (check (str label ": rejection cannot wedge stop")
               nil (tcp/stop-server srv))
        (check (str label ": rejected cleanup publishes completion")
               :stopped (deref (:stopped srv) 0 :timeout)))
      (finally
        (net/close! client)
        (when @(:running? srv) (tcp/stop-server srv))))))

(defn- test-rejected-handler-submission-does-not-leak-working []
  ;; The synthetic executor pins jolt-tcp's boundary independently of any host
  ;; ExecutorService implementation.
  (exercise-rejected-handler-submission
   "synthetic executor"
   (reify java.util.concurrent.Executor
     (execute [_ _]
       (throw (ex-info "synthetic handler rejection"
                       {:err ::synthetic-rejection})))))

  ;; This is also an integration witness for the reviewed Jolt core: execute
  ;; after shutdown must reject synchronously. If it silently accepts into the
  ;; dead queue, no task can clear WORKING and this test times out at close/stop.
  (let [executor (java.util.concurrent.Executors/newSingleThreadExecutor)]
    (.shutdown executor)
    (.awaitTermination executor 2000)
    (exercise-rejected-handler-submission "shutdown ExecutorService" executor)))

(defn- test-stop-linearizes-with-reactor-admission []
  ;; Pin the ordering without depending on a scheduler-selected socket race:
  ;; the gate admits one event before stop, the stop CAS closes admission, and
  ;; every later ordinary reactor action is rejected. The earlier action may
  ;; finish after the CAS; public stop waits for reactor cleanup around it.
  (let [entered (promise)
        release (promise)
        calls   (atom 0)
        srv     {:running? (atom true)
                 :admission (atom {:phase :open :active 0})
                 :accept-gate (atom false)
                 ;; request-stop! catches a wake failure; no real poller is
                 ;; needed for this pure admission witness.
                 :poller nil}
        admitted
        (future
          (@#'tcp/with-running-admission
           srv
           (fn []
             (deliver entered true)
             (deref release)
             (swap! calls inc))))
        _ (deref entered 1000 :timeout)
        stopping (future (@#'tcp/request-stop! srv))]
    (check "the first stop request wins"
           true (deref stopping 1000 :timeout))
    (check "stop publishes admission closed"
           [false :stopping 1]
           [@(:running? srv)
            (:phase @(:admission srv))
            (:active @(:admission srv))])
    (@#'tcp/with-running-admission srv #(swap! calls inc))
    (check "ordinary reactor work is not admitted after the stop CAS"
           0 @calls)
    (deliver release :go)
    (check "the event admitted before stop may finish"
           1 (deref admitted 1000 :timeout))
    (check "pre-stop admission releases without reopening the gate"
           [:stopping 0]
           ((juxt :phase :active) @(:admission srv)))
    (check "only the pre-stop action ran"
           1 @calls)))

(defn- test-stop-drains-active-handler-write []
  ;; Keep the client's receive window deliberately small and queue much more
  ;; than it can hold. The write callback therefore cannot fire before the
  ;; client starts draining. Stop is requested first: cleanup must continue
  ;; reactor-owned write service until that callback releases the active handler.
  (let [payload-size (* 1024 1024)
        payload      (byte-array payload-size)
        queued       (promise)
        write-done   (promise)
        order        (atom [])
        handler
        (fn
          ([sock]
           (tcp/write sock (buf/wrap payload)
                      #(deliver write-done :written))
           (deliver queued true)
           (let [result (deref write-done 5000 :timeout)]
             (swap! order conj [:accept-return result])
             result))
          ([state _sock _buffer] state)
          ([state _ex] (swap! order conj [:close state])))
        srv (tcp/run-server :port 0 :handler handler :reuse-address? true
                            :write-buffer-size payload-size
                            :stop-timeout-ms 7000)
        client (client/connect "127.0.0.1" (:port srv)
                               {:no-delay? true :recv-buffer-size 1024})]
    (try
      (check "active handler queued its backpressured write"
             true (deref queued 2000 false))
      (let [stopping
            (future
              (try
                (tcp/stop-server srv)
                :stopped
                (catch :default e e)))]
        ;; request-stop! publishes this before it waits for cleanup.
        (loop [n 0]
          (when (and @(:running? srv) (< n 10000))
            (Thread/yield)
            (recur (inc n))))
        (check "stop is waiting for the active write callback"
               :waiting (deref stopping 25 :waiting))
        (let [received
              (deref
               (future
                 (loop [total 0]
                   (if-let [chunk (net/client-recv client 16384)]
                     (recur (+ total (alength chunk)))
                     total)))
               6000 :timeout)]
          (check "shutdown drain flushes the active handler's complete Buffer"
                 payload-size received)
          (check "active write callback releases its handler during stop"
                 :written (deref write-done 0 :missing))
          (check "stop completes after draining active handler work"
                 :stopped (deref stopping 1000 :timeout))
          (check "active handler still closes last with its final state"
                 [[:accept-return :written] [:close :written]] @order)))
      (finally
        ;; Release a failed regression run instead of leaving its worker parked.
        (deliver write-done :aborted)
        (net/close! client)
        (when @(:running? srv)
          (try (tcp/stop-server srv) (catch :default _ nil)))))))

(defn- test-stop-quiesces-active-handler-before-close []
  (let [started (promise)
        release (promise)
        order   (atom [])
        handler
        (fn
          ([sock]
           (deliver started sock)
           (deref release)
           (swap! order conj :accept-return)
           :accepted)
          ([state _sock _buffer] state)
          ([state _ex] (swap! order conj [:close state])))
        srv (tcp/run-server :port 0 :handler handler :reuse-address? true
                            :stop-timeout-ms 3000)
        client (net/connect-loopback (:port srv))]
    (try
      (deref started 2000 :timeout)
      (let [stopping (future (tcp/stop-server srv))]
        (check "stop waits while an accept handler is WORKING"
               :waiting (deref stopping 50 :waiting))
        (deliver release :go)
        (check "stop completes after the active handler quiesces"
               nil (deref stopping 2000 :timeout))
        (check "handler close arity is last and sees final state"
               [:accept-return [:close :accepted]] @order)
        (check "late worker wake after poller close is harmless"
               nil (@#'tcp/wake-server! (:srv srv))))
      (finally
        (deliver release :go)
        (net/close! client)
        (when @(:running? srv) (tcp/stop-server srv))))))


(defn- no-op-handler
  ([_sock] nil)
  ([state _sock _buffer] state)
  ([_state _ex] nil))

(defn- test-repeated-start-stop-is-complete-and-idempotent []
  ;; Reuse one port immediately, with no post-stop sleep. This only works when
  ;; stop-server waits until the listener and poller have really been closed.
  (let [port (+ 19650 (rand-int 200))
        outcomes (atom [])]
    (dotimes [_ 8]
      (try
        (let [srv (tcp/run-server :port port :handler no-op-handler
                                  :reuse-address? true)
              first-result (tcp/stop-server srv)
              second-result (tcp/stop-server srv)]
          (swap! outcomes conj
                 {:results [first-result second-result]
                  :running? @(:running? srv)
                  :stopped (deref (:stopped srv) 0 :timeout)
                  :future? (some? (:reactor-future srv))}))
        (catch :default e
          (swap! outcomes conj {:error (ex-message e)}))))
    (check "eight immediate start/stop cycles complete without a bind race"
           8 (count (filter #(nil? (:error %)) @outcomes)))
    (check "stop is idempotent and retains completion/future state"
           true
           (every? #(= {:results [nil nil]
                        :running? false
                        :stopped :stopped
                        :future? true}
                       %)
                   @outcomes))))

(defn- test-stop-timeout-is-bounded-and-recoverable []
  (let [port (+ 19850 (rand-int 100))
        close-started (promise)
        handler (fn
                  ([_sock] nil)
                  ([state _sock _buffer] state)
                  ([_state _ex]
                   (deliver close-started true)
                   (Thread/sleep 150)))
        srv (tcp/run-server :port port :handler handler :reuse-address? true
                            :stop-timeout-ms 25)
        fd (net/connect-loopback port)]
    (try
      (Thread/sleep 150) ; ensure accept and its handler arity have completed
      (let [err (try
                  (tcp/stop-server srv)
                  nil
                  (catch :default e e))]
        (check "stop timeout is reported structurally"
               ::tcp/stop-timeout (:err (ex-data err)))
        (check "timeout came from an in-progress close arity"
               true (deref close-started 1000 false))
        (check "cleanup still reaches completion after a timed-out waiter"
               :stopped (deref (:stopped srv) 2000 :timeout))
        (check "a later idempotent stop observes the same completion"
               nil (tcp/stop-server srv)))
      (finally (net/close! fd)))))

(defn -main [& _]
  (println "== teensyp.server acceptance tests (jolt) ==")
  (test-echo)
  (test-line-reverse-partial)
  (test-doubler)
  (test-write-then-close)
  (test-callback-chain)
  (test-pause-resume)
  (test-stream-lines)
  (test-concurrent)
  (test-reactor-survives-connection-error)
  (test-half-close-still-answered)
  (test-half-close-during-handler)
  (test-stream-half-close)
  (test-eof-notification-contract)
  (test-socket-info-and-generation)
  (test-write-callback-is-buffer-release-fence)
  (test-callbacks-cannot-deadlock-handler-pool)
  (test-active-task-tracking-has-a-stable-empty-barrier)
  (test-callback-executor-contract)
  (test-cas-bounded-queue-admission-and-credit-rollback)
  (test-close-exception-state-is-atomic)
  (test-accept-batches-cancellation-and-registration-rollback)
  (test-direct-executor-stop-releases-accept-gate)
  (test-closed-shutdown-context-clears-write-readiness)
  (test-write-completion-success-and-failure)
  (test-rejected-handler-submission-does-not-leak-working)
  (test-stop-linearizes-with-reactor-admission)
  (test-stop-drains-active-handler-write)
  (test-stop-quiesces-active-handler-before-close)
  (test-repeated-start-stop-is-complete-and-idempotent)
  (test-stop-timeout-is-bounded-and-recoverable)

  ;; Generative layers. The pure buffer properties run under clojure.test (via
  ;; hegel.clojure-test/with); the TCP properties use hegel.core/run-test!
  ;; directly and count their own failures. Both fold into the same total so
  ;; `jolt -M:test` stays the single released-runtime migration gate.
  (println "\n-- teensyp.buffer generative properties (jolt-hegel) --")
  (let [s (clojure.test/run-tests 'teensyp.client-test
                                  'teensyp.buffer-property-test)]
    (swap! failures + (+ (:fail s 0) (:error s 0))))
  (swap! failures + (teensyp.server-property-test/run-properties!))

  (println (str "\n" (if (zero? @failures) "ALL PASS" (str @failures " FAILURE(S)"))))
  ;; core.async (used by teensyp.stream) spawns non-daemon threads that would
  ;; keep the process alive, so exit explicitly with a CI-meaningful status.
  (flush)
  (System/exit (if (pos? @failures) 1 0)))
