(ns teensyp.server-test
  "Framework-less acceptance tests driven over real loopback TCP. Each scenario
  starts a server, drives it with the blocking client in teensyp.ffi-net, and
  asserts the bytes on the wire. `-main` exits non-zero if anything fails, so
  `joltc -M:test` gates CI."
  (:require [clojure.string :as str]
            [clojure.test]
            [teensyp.server :as tcp]
            [teensyp.buffer :as buf]
            [teensyp.stream :as stream]
            [teensyp.ffi-net :as net]
            [jolt.ffi :as ffi]
            ;; loaded for their side effects on the run: the deftests below are
            ;; discovered by clojure.test/run-tests, the properties by run-properties!
            [teensyp.buffer-property-test]
            [teensyp.server-property-test]))

(def ^:private failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "ok  " label)
    (do (swap! failures inc)
        (println "FAIL" label "\n   expected:" (pr-str expected) "\n   actual:  " (pr-str actual)))))

(defn- utf8 [s] (.getBytes ^String s "UTF-8"))
(defn- ->str [^bytes b] (when b (String. b "UTF-8")))

(defn- recv-until-eof
  "Read from a blocking client fd until the peer closes; return the accumulated
  String."
  [fd]
  (loop [acc ""]
    (if-let [b (net/client-recv fd 4096)]
      (recur (str acc (->str b)))
      acc)))

(defn- with-server [handler f]
  ;; port 0 lets the OS choose, but our make-sockaddr binds a fixed port; use a
  ;; per-scenario fixed port with SO_REUSEADDR to avoid TIME_WAIT collisions.
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
        (Thread/sleep 150)
        (check "stream line-echo" "<hi>\n<yo>\n" (->str (net/client-recv fd 4096)))
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

;; --- fd ownership + context identity ---------------------------------------
;; POSIX may reuse an fd immediately after close(2). Reactor work therefore
;; cannot identify a connection by fd alone, and no worker may close the raw fd:
;; a stale worker could otherwise close a replacement connection that inherited
;; the same number. These are deterministic unit witnesses for a race that the
;; rapid-connect HTTP properties only catch intermittently.
(defn- immediate-executor []
  (reify java.util.concurrent.Executor
    (execute [_ f] (f))))

(defn- lifecycle-context [srv fd generation]
  {:srv srv
   :fd fd
   :generation generation
   :flags (atom 0)
   :state (volatile! nil)
   :write-queue (atom clojure.lang.PersistentQueue/EMPTY)
   :close-ex (volatile! nil)
   :close-callback (atom nil)})

(defn- test-reactor-owned-close-and-generation []
  (let [closed         (atom [])
        close-arities  (atom [])
        callback-seen  (atom [])
        executor       (immediate-executor)
        srv            {:conns (atom {})
                        :pending (atom #{})
                        :executor executor
                        :callback-executor executor
                        :wake-w -1
                        :wake-gate (atom false)
                        :wake-open? (atom true)
                        :running? (atom true)
                        :opts {:handler (fn [_state ex]
                                          (swap! close-arities conj ex))}}
        ctx            (lifecycle-context srv 41 1)]
    (reset! (:conns srv) {41 ctx})
    (swap! (:write-queue ctx) conj
           [::tcp/close (fn [] (swap! callback-seen conj (vec @closed)))])
    (with-redefs [net/close! (fn [fd] (swap! closed conj fd))
                  net/wake!  (fn [_] nil)]
      ;; Consuming ::close only requests closure. The reactor finalizer owns the
      ;; one actual close(2), and callbacks observe it afterwards.
      (@#'tcp/handle-write srv ctx)
      (check "explicit close is deferred to the reactor finalizer" [] @closed)
      (@#'tcp/handle-pending-close srv ctx)
      (check "reactor closes the fd exactly once" [41] @closed)
      (check "close callback runs after the fd is closed" [[41]] @callback-seen)
      (check "close arity runs exactly once" 1 (count @close-arities))
      (check "closed context is removed" nil (get @(:conns srv) 41))

      ;; Reuse the same raw number for a new generation, then deliver stale close
      ;; work from the old context. Neither the fd nor the replacement registry
      ;; entry may be touched.
      (let [old (lifecycle-context srv 41 1)
            new (lifecycle-context srv 41 2)]
        (reset! closed [])
        (reset! close-arities [])
        (reset! (:pending srv) #{})
        (reset! (:conns srv) {41 new})
        (@#'tcp/handle-close old nil)
        (check "worker close request does not call close(2)" [] @closed)
        (@#'tcp/handle-pending-close srv old)
        (check "stale generation cannot close a reused fd" [] @closed)
        (check "stale generation cannot remove the replacement context"
               true (identical? new (get @(:conns srv) 41)))
        (check "stale generation cannot run the old close arity"
               0 (count @close-arities))))))

;; --- server resource lifecycle ---------------------------------------------

(defn- no-op-handler
  ([_sock] nil)
  ([state _sock _buffer] state)
  ([_state _ex] nil))

(defn- test-full-cleanup-is-exactly-once []
  (let [closed  (atom [])
        freed   (atom [])
        ex      (java.util.concurrent.Executors/newFixedThreadPool 1)
        cb-ex   (java.util.concurrent.Executors/newFixedThreadPool 1)
        stopped (promise)
        srv     {:listen-fd 61 :wake-r 62 :wake-w 63
                 :recv-buf 71 :send-buf 72
                 :conns (atom {})
                 :executor ex :callback-executor cb-ex
                 :owns-executor? true
                 :owns-callback-executor? true
                 :shutdown-executor? true
                 :shutdown-callback-executor? true
                 :cleanup-started? (atom false)
                 :running? (atom true)
                 :wake-gate (atom false)
                 :wake-open? (atom true)
                 :stopped stopped
                 :opts {:handler no-op-handler :error-logger (fn [_])}}]
    (with-redefs [net/close! (fn [fd] (swap! closed conj fd))
                  ffi/free  (fn [ptr] (swap! freed conj ptr))]
      (@#'tcp/cleanup-server! srv)
      (@#'tcp/cleanup-server! srv))
    ;; wake-w is retired under the gate before any other descriptor cleanup.
    (check "full cleanup closes each owned descriptor exactly once"
           [63 61 62] @closed)
    (check "full cleanup frees each native buffer exactly once"
           [71 72] @freed)
    (check "full cleanup shuts down the handler executor"
           true (.isShutdown ex))
    (check "full cleanup shuts down the callback executor"
           true (.isShutdown cb-ex))
    (check "full cleanup publishes completion after release"
           :stopped (deref stopped 0 :timeout))))

(defn- test-wake-close-serialization []
  (let [events           (atom [])
        wake-count       (atom 0)
        worker-admitted  (promise)
        release-worker   (promise)
        terminal-admitted (promise)
        release-terminal (promise)
        wake-closed      (promise)
        ex               (java.util.concurrent.Executors/newFixedThreadPool 1)
        cb-ex            (java.util.concurrent.Executors/newFixedThreadPool 1)
        stopped          (promise)
        srv              {:listen-fd 61 :wake-r 62 :wake-w 63
                          :recv-buf 71 :send-buf 72
                          :conns (atom {})
                          :executor ex :callback-executor cb-ex
                          :owns-executor? true
                          :owns-callback-executor? true
                          :shutdown-executor? true
                          :shutdown-callback-executor? true
                          :cleanup-started? (atom false)
                          :running? (atom true)
                          :wake-gate (atom false)
                          :wake-open? (atom true)
                          :stopped stopped
                          :opts {:handler no-op-handler
                                 :error-logger (fn [_])}}]
    (with-redefs
      [net/wake!
       (fn [_fd]
         (case (swap! wake-count inc)
           1 (do
               ;; The worker has been admitted and owns the gate, but its
               ;; synthetic native write is held until the test releases it.
               (deliver worker-admitted true)
               (deref release-worker 2000 :timeout)
               (swap! events conj :worker-wake)
               nil)
           2 (do
               ;; stop owns the same gate across running? CAS + terminal write.
               (deliver terminal-admitted true)
               (deref release-terminal 2000 :timeout)
               (swap! events conj :terminal-wake)
               nil)
           (do (swap! events conj :unexpected-wake) nil)))
       net/close!
       (fn [fd]
         (when (= fd 63)
           (swap! events conj :wake-close)
           (deliver wake-closed true)))
       ffi/free (fn [_] nil)]
      (let [worker (future (@#'tcp/wake-server! srv))]
        (try
          (check "worker wake is admitted before its native write"
                 true (deref worker-admitted 1000 false))
          (let [stopper (future (@#'tcp/request-stop! srv))]
            ;; stop cannot acquire the ownership gate until this admitted worker
            ;; write completes.
            (deliver release-worker true)
            (check "terminal stop waits behind the admitted worker wake"
                   true (deref terminal-admitted 1000 false))
            (let [cleaner (future (@#'tcp/cleanup-server! srv))]
              (check "cleanup cannot close wake-w during the terminal write"
                     :not-closed (deref wake-closed 50 :not-closed))
              (deliver release-terminal true)
              (deref worker 1000 :timeout)
              (deref stopper 1000 :timeout)
              (deref cleaner 1000 :timeout)
              (check "admitted worker, terminal wake, and close are ordered"
                     [:worker-wake :terminal-wake :wake-close] @events)
              (check "serialized cleanup publishes completion"
                     :stopped (deref stopped 0 :timeout))
              ;; A worker arriving after close obtains the gate, observes both
              ;; closed admission flags, and performs no native write.
              (@#'tcp/wake-server! srv)
              (check "no worker wake occurs after wake-w is closed"
                     2 @wake-count)))
          (finally
            (deliver release-worker true)
            (deliver release-terminal true)))))))

(defn- test-future-construction-failure-preserves-supplied-executors []
  (let [closed (atom [])
        freed  (atom [])
        allocs (atom 90)
        error  (atom nil)
        ex      (java.util.concurrent.Executors/newFixedThreadPool 1)
        cb-ex   (java.util.concurrent.Executors/newFixedThreadPool 1)]
    (try
      (with-redefs
        [net/listen-socket (fn [_port _opts] 81)
         net/make-pipe     (fn [] [82 83])
         net/close!        (fn [fd] (swap! closed conj fd))
         ffi/alloc         (fn [_n] (swap! allocs inc))
         ffi/free          (fn [ptr] (swap! freed conj ptr))
         clojure.core/future-call
         (fn [_]
           (throw (ex-info "synthetic future construction failure"
                           {:stage :future-construction})))]
        (try
          (tcp/run-server :port 19002 :handler no-op-handler
                          :executor ex :callback-executor cb-ex)
          (catch :default e (reset! error e))))
      (check "future construction failure propagates"
             :future-construction (:stage (ex-data @error)))
      (check "future construction failure closes acquired descriptors"
             {81 1, 82 1, 83 1} (frequencies @closed))
      (check "future construction failure frees acquired native buffers"
             {91 1, 92 1} (frequencies @freed))
      (check "failed construction does not shut supplied handler executor"
             false (.isShutdown ex))
      (check "failed construction does not shut supplied callback executor"
             false (.isShutdown cb-ex))
      (finally
        (.shutdown ex)
        (.shutdown cb-ex)))))

(defn- test-future-construction-failure-shuts-adopted-executors []
  (let [ex    (java.util.concurrent.Executors/newFixedThreadPool 1)
        cb-ex (java.util.concurrent.Executors/newFixedThreadPool 1)]
    (try
      (with-redefs
        [net/listen-socket (fn [_port _opts] 81)
         net/make-pipe     (fn [] [82 83])
         net/close!        (fn [_fd] nil)
         ffi/alloc         (fn [_n] 91)
         ffi/free          (fn [_ptr] nil)
         clojure.core/future-call
         (fn [_]
           (throw (ex-info "synthetic future construction failure"
                           {:stage :future-construction})))]
        (try
          (tcp/run-server :port 19003 :handler no-op-handler
                          :executor ex :callback-executor cb-ex
                          :shutdown-executor? true
                          :shutdown-callback-executor? true)
          (catch :default _ nil)))
      (check "failed construction shuts an explicitly adopted handler executor"
             true (.isShutdown ex))
      (check "failed construction shuts an explicitly adopted callback executor"
             true (.isShutdown cb-ex))
      (finally
        (.shutdown ex)
        (.shutdown cb-ex)))))

(defn- test-supplied-executors-are-borrowed-unless-adopted []
  (let [port  (+ 19450 (rand-int 100))
        ex    (java.util.concurrent.Executors/newFixedThreadPool 1)
        cb-ex (java.util.concurrent.Executors/newFixedThreadPool 1)]
    (try
      (let [srv (tcp/run-server :port port :handler no-op-handler
                                :reuse-address? true
                                :executor ex :callback-executor cb-ex)]
        (tcp/stop-server srv)
        (check "successful stop preserves a borrowed handler executor"
               false (.isShutdown ex))
        (check "successful stop preserves a borrowed callback executor"
               false (.isShutdown cb-ex)))
      (finally
        (.shutdown ex)
        (.shutdown cb-ex))))
  (let [port  (+ 19550 (rand-int 100))
        ex    (java.util.concurrent.Executors/newFixedThreadPool 1)
        cb-ex (java.util.concurrent.Executors/newFixedThreadPool 1)]
    (let [srv (tcp/run-server :port port :handler no-op-handler
                              :reuse-address? true
                              :executor ex :callback-executor cb-ex
                              :shutdown-executor? true
                              :shutdown-callback-executor? true)]
      (tcp/stop-server srv)
      (check "successful stop shuts an explicitly adopted handler executor"
             true (.isShutdown ex))
      (check "successful stop shuts an explicitly adopted callback executor"
             true (.isShutdown cb-ex)))))

(defn- test-reactor-start-gate-honors-abort []
  (let [calls (atom [])
        aborted (promise)
        started (promise)]
    (deliver aborted :abort)
    (check "aborted reactor start returns without entering released state"
           nil
           (@#'tcp/run-after-reactor-start!
            aborted
            #(do (swap! calls conj :aborted) :wrong)))
    (check "aborted reactor start never invokes the reactor"
           [] @calls)
    (deliver started :start)
    (check "committed reactor start invokes the reactor exactly once"
           :ran
           (@#'tcp/run-after-reactor-start!
            started
            #(do (swap! calls conj :started) :ran)))
    (check "only the committed start ran"
           [:started] @calls)))

(defn- test-partial-start-cleanup []
  (let [closed (atom [])
        freed  (atom [])
        allocs (atom 0)
        error  (atom nil)]
    (with-redefs
      [net/listen-socket (fn [_port _opts] 81)
       net/make-pipe     (fn [] [82 83])
       net/close!        (fn [fd] (swap! closed conj fd))
       ffi/alloc         (fn [_n]
                           (if (= 1 (swap! allocs inc))
                             91
                             (throw (ex-info "synthetic send-buffer failure"
                                             {:stage :send-buffer}))))
       ffi/free          (fn [ptr] (swap! freed conj ptr))]
      (try
        (tcp/run-server :port 19001 :handler no-op-handler)
        (catch :default e (reset! error e))))
    (check "partial start propagates the acquisition failure"
           :send-buffer (:stage (ex-data @error)))
    (check "partial start closes every acquired descriptor exactly once"
           {81 1, 82 1, 83 1} (frequencies @closed))
    (check "partial start frees every acquired native buffer exactly once"
           {91 1} (frequencies @freed))))

(defn- test-repeated-start-stop-is-complete-and-idempotent []
  ;; Reuse one port immediately, with no post-stop sleep. This only works when
  ;; stop-server waits until the listener and self-pipe have really been closed.
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
  (test-reactor-owned-close-and-generation)
  (test-full-cleanup-is-exactly-once)
  (test-wake-close-serialization)
  (test-future-construction-failure-preserves-supplied-executors)
  (test-future-construction-failure-shuts-adopted-executors)
  (test-supplied-executors-are-borrowed-unless-adopted)
  (test-reactor-start-gate-honors-abort)
  (test-partial-start-cleanup)
  (test-repeated-start-stop-is-complete-and-idempotent)
  (test-stop-timeout-is-bounded-and-recoverable)

  ;; Generative layers. The pure buffer properties run under clojure.test (via
  ;; hegel.clojure-test/with); the TCP properties use hegel.core/run-test!
  ;; directly and count their own failures. Both fold into the same total so
  ;; `joltc -M:test` stays the single gate.
  (println "\n-- teensyp.buffer generative properties (jolt-hegel) --")
  (let [s (clojure.test/run-tests 'teensyp.buffer-property-test)]
    (swap! failures + (+ (:fail s 0) (:error s 0))))
  (swap! failures + (teensyp.server-property-test/run-properties!))

  (println (str "\n" (if (zero? @failures) "ALL PASS" (str @failures " FAILURE(S)"))))
  ;; core.async (used by teensyp.stream) spawns non-daemon threads that would
  ;; keep the process alive, so exit explicitly with a CI-meaningful status.
  (flush)
  (System/exit (if (pos? @failures) 1 0)))
