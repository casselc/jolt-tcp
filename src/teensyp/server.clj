(ns teensyp.server
  "A teensyp-compatible TCP server for jolt, built on a single jolt.net
  readiness reactor plus a worker pool.

  Same contract as JVM teensyp: a handler is a 3-arity fn

      (fn handler
        ([socket] initial-state)           ;; on accept
        ([state socket buffer] new-state)  ;; on read  (buffer is a teensyp.buffer/Buffer)
        ([state exception]))               ;; on close (exception nil if graceful)

  threaded per connection, guaranteed serial per connection (accept first, close
  last, reads sequential), with the write queue drained before the next read.

  Differences from JVM teensyp (documented, contract-preserving):
   - All socket I/O happens on the reactor thread, so `try-write` never writes
     immediately — it always returns false and `write` queues. The completion
     callback and write-queue limits behave the same.
   - `socket-info` returns transport-neutral numeric endpoint maps
     `{:host ... :port ... :family ...}` for :local-address and
     :remote-address, plus the raw :fd as diagnostics only."
  (:refer-clojure :exclude [])
  (:require [jolt.net :as net]
            [teensyp.buffer :as buf]
            [teensyp.concurrent :refer [with-lock]]))

;; --- flags (per-connection, on a volatile) ---------------------------------
(def ^:const WRITING 0x01)
(def ^:const WORKING 0x02)
(def ^:const CLOSED  0x04)
(def ^:const PAUSED  0x08)
(def ^:const FULL    0x10)
;; The peer half-closed its write side: no more data will arrive, but it is
;; still reading, so any queued response must be flushed before closing.
(def ^:const EOF     0x20)
;; The handler has been told about EOF (its read arity ran once with
;; peer-closed? true). Distinct from EOF itself because the notification can be
;; deferred: if the handler is mid-call when EOF arrives, it must be delivered
;; afterwards, exactly once. Deliberately NOT part of READ-MASK.
(def ^:const EOF-SEEN 0x40)
;; EOF joins the mask so the reactor stops asking for POLLIN — a half-closed
;; socket reports readable forever, which would spin the loop.
(def ^:const READ-MASK (bit-or PAUSED CLOSED FULL EOF))

(declare handle-close submit-read-handler report-error
         reconcile-registration! reconcile-shutdown-registration!)

;; --- small helpers ---------------------------------------------------------
(defn- execute!
  "Submit through the portable Executor boundary.

  Jolt v0.7.28's modeled ExecutorService can accept `execute` after shutdown
  without throwing even though no worker can run the task. Detect that terminal
  state before admission so every caller retains its synchronous-rejection
  cleanup contract. Plain Executor implementations still delegate directly."
  [executor f]
  (when (try (true? (.isShutdown executor))
             (catch :default _ false))
    (throw (ex-info "ExecutorService is shut down"
                    {:err ::executor-shutdown})))
  (.execute executor f))

(defn- submit [srv f] (execute! (:executor srv) f))

(defn- task-tracker []
  (atom {:accepting? true :active #{}}))

(defn- register-task!
  "Atomically admit `done` while the tracker is open.

  Cleanup closes admission before waiting. That gives it a stable-empty
  boundary: after the close CAS, the active set can only shrink."
  [tracker done]
  (loop []
    (let [old @tracker]
      (if-not (:accepting? old)
        false
        (if (compare-and-set! tracker old (update old :active conj done))
          true
          (recur))))))

(defn- finish-task! [tracker done]
  (swap! tracker update :active disj done)
  nil)

(defn- submit-callback
  "Run a write/control completion callback.

  Deliberately a *different* executor from the one running handler arities. A
  handler is allowed to block until one of its writes completes (that is how a
  blocking stream/sink over a socket is built), and if the callback that
  releases it were queued to the same bounded pool, enough concurrent blocked
  handlers would consume every thread and none would be left to run the
  callbacks — a hard deadlock at exactly `pool-size` concurrent writers.
  Callbacks are short and non-blocking, so the dedicated pool stays available."
  [srv f]
  (let [done (promise)
        tracker (:callback-tasks srv)
        tracked? (if tracker (register-task! tracker done) false)
        run (fn []
              (try (f)
                   (catch :default e (report-error srv e))
                   (finally
                     (deliver done :done)
                     (when tracked? (finish-task! tracker done)))))]
    (if (and tracker (not tracked?))
      ;; Cleanup has already frozen callback admission. There should be no
      ;; producers after handler quiescence, but an executor rejection or a
      ;; caller-supplied late callback must still observe completion.
      (run)
      (try
        (execute! (:callback-executor srv) run)
        (catch :default e
          (report-error srv e)
          (run))))
    done))

(defn- ex-control-queue-full [ctx]
  (ex-info "Control queue full"
           {:err ::control-queue-full
            :fd (:fd ctx)
            :generation (:generation ctx)
            :queue-count (count @(:control-queue ctx))
            :queue-cap (:control-queue-cap ctx)
            :flags @(:flags ctx)}))
(defn- ex-write-queue-full          [] (ex-info "Write queue full" {:err ::write-queue-full}))
(defn- ex-write-queue-over-capacity [] (ex-info "Write queue over capacity" {:err ::write-queue-over-capacity}))
(defn- ex-socket-closed [ctx]
  (let [close-ex (:close-ex ctx)
        cause (when close-ex @close-ex)]
    (ex-info "Socket is closed"
             {:err ::socket-closed
              :fd (:fd ctx)
              :generation (:generation ctx)}
             cause)))

;; PersistentQueue-in-an-atom. Queue admission and removal are each one CAS, so
;; multiple producers cannot pass a stale count check and collectively exceed
;; the configured cap.
;; The cap check and append below share the same queue snapshot.
(defn- q-offer!
  [qa cap x]
  (loop []
    (let [q @qa]
      (if (>= (count q) (long cap))
        false
        (if (compare-and-set! qa q (conj q x))
          true
          (recur))))))
(defn- q-peek  [qa] (peek @qa))
(defn- q-poll! [qa]
  (loop [] (let [q @qa] (when (seq q) (if (compare-and-set! qa q (pop q)) (peek q) (recur))))))

(defn- drain-pending! [pa]
  (loop [] (let [cur @pa] (if (compare-and-set! pa cur #{}) cur (recur)))))

(defn- pending-key [ctx]
  (:generation ctx))

(defn- mark-pending! [ctx]
  (swap! (:pending (:srv ctx)) conj (pending-key ctx)))

(defn- wake-server!
  "Wake the reactor after publishing worker-owned flags or pending work.

  jolt.net owns wake admission and descriptor retirement. A worker racing final
  poller close may observe a closed-poller exception; the state it published is
  already irrelevant to the stopped server, so that late wake is harmless.

  Do not gate this on running?: after stop revokes ordinary reactor service, the
  reactor still has to drain writes needed by handlers that were already active.
  Their publications must wake that bounded shutdown drain too."
  [srv]
  (try (net/wake! (:poller srv)) (catch :default _ nil)))

(defn- with-cas-gate
  "Run `f` under a short owner-independent gate.

  Jolt futures can share host-thread identity, so a ReentrantLock is not a sound
  ownership boundary here. Gate bodies perform no socket I/O that can block."
  [gate f]
  (loop []
    (when-not (compare-and-set! gate false true)
      (Thread/yield)
      (recur)))
  (try
    (f)
    (finally (reset! gate false))))

(defn- request-stop!
  "Transition to stopped and wake the reactor. Returns true only for the winner.

  The admission phase CAS is the stop linearization boundary. An action that
  acquired admission first may finish, but no action can acquire admission after
  the CAS. Public stop still waits for reactor cleanup, hence for every such
  pre-admitted action."
  [srv]
  (with-cas-gate
    (:accept-gate srv)
    (fn []
      (let [admission (:admission srv)]
        (loop []
          (let [old @admission]
            (when (= :open (:phase old))
              (if (compare-and-set! admission old (assoc old :phase :stopping))
                (do
                  (reset! (:running? srv) false)
                  (try (net/wake! (:poller srv)) (catch :default _ nil))
                  true)
                (recur)))))))))

(defn- acquire-running-admission!
  [srv]
  (let [admission (:admission srv)]
    (loop []
      (let [old @admission]
        (if-not (= :open (:phase old))
          false
          (if (compare-and-set! admission old (update old :active inc))
            true
            (recur)))))))

(defn- release-running-admission!
  [srv]
  (let [admission (:admission srv)]
    (loop []
      (let [old @admission]
        (if (compare-and-set! admission old (update old :active dec))
          nil
          (recur))))))

(defn- with-running-admission
  "Run one ordinary reactor action only while server admission remains open.

  Admission and stop share one CAS state rather than a thread-owned lock. Jolt
  futures can share host-thread bookkeeping, so a ReentrantLock may treat two
  logical tasks as the same reentrant owner and cannot establish this ordering."
  [srv f]
  (when (acquire-running-admission! srv)
    (try
      (f)
      (finally (release-running-admission! srv)))))

;; --- flags -----------------------------------------------------------------
(defn- flags-of  [ctx] (long @(:flags ctx)))
(defn- flag?     [ctx flag] (not (zero? (bit-and (flags-of ctx) (long flag)))))
;; An atom, updated with swap! rather than a volatile guarded by the socket
;; lock. The lock version was recursive: the public `write` takes the socket
;; lock, calls queue-write, which calls set-flag!, which took the *same* lock
;; again. Under contention that path failed outright — a runtime witness
;; completed only 143 of 801 critical sections, with "not lock owner" mutex
;; errors and blocked workers, which is the mutex-release failure behind the
;; quarantined stream wedge. swap! gives the same atomic bit update with no lock
;; at all.
(defn- set-flag!   [ctx flag] (swap! (:flags ctx) bit-or (long flag)))
(defn- unset-flag! [ctx flag] (swap! (:flags ctx) bit-and-not (long flag)))

(defn- reserve-write-limit! [ctx n]
  (loop []
    (let [cur (long @(:write-limit ctx))]
      (if (< cur (long n))
        (throw (ex-write-queue-over-capacity))
        (when-not (compare-and-set! (:write-limit ctx) cur (- cur (long n))) (recur))))))

(defn- acquire-write-admission!
  [ctx]
  (let [admission (:write-admission ctx)]
    (loop []
      (let [old @admission]
        (if-not (= :open (:phase old))
          false
          (if (compare-and-set! admission old (update old :active inc))
            true
            (recur)))))))

(defn- release-write-admission!
  [ctx]
  (let [admission (:write-admission ctx)]
    (loop []
      (let [old @admission]
        (if (compare-and-set! admission old (update old :active dec))
          nil
          (recur))))))

(defn- close-write-admission!
  "Close write admission and wait for every already-admitted producer to leave.

  A producer only reserves byte credit, performs one queue CAS, and publishes a
  wake while admitted, so this bounded barrier never waits for socket I/O or a
  user callback. Once it returns, the reactor can drain and settle the queue
  without racing a late enqueue."
  [ctx]
  (let [admission (:write-admission ctx)]
    (loop []
      (let [old @admission]
        (cond
          (= :closed (:phase old)) nil
          (compare-and-set! admission old (assoc old :phase :closed)) nil
          :else (recur))))
    (loop []
      (when (pos? (long (:active @admission)))
        (Thread/yield)
        (recur)))))

;; --- Socket protocol -------------------------------------------------------
(defprotocol Socket
  "A client socket. See [[write]], [[close]], [[pause-reads]], [[resume-reads]]."
  (try-write [socket buffer]
    "Attempt an immediate non-blocking write. In jolt-tcp all socket I/O is on
    the reactor thread, so this always returns false (the write is queued).")
  (queue-control [socket control callback]
    "Queue a control event (::pause-reads / ::resume-reads).")
  (queue-write [socket buffer callback]
    "Queue a Buffer (or ::close) to be written; callback fires when fully written.")
  (socket-info [socket]
    "Map with numeric {:host :port :family} local/remote endpoints and diagnostic :fd.")
  (socket-lock [socket]
    "The socket's ReentrantLock, for atomic multi-writes."))

(defprotocol CompletionSocket
  "Optional additive SPI for outcome-aware writes.

  Keeping this separate from [[Socket]] preserves existing Socket
  implementations; only callers of [[write-completion]] need this capability."
  (queue-write-completion [socket buffer completion]
    "Queue a Buffer and settle `completion` with a write outcome."))

(defrecord Context [srv socket token interests fd generation lock flags state read-buffer read-view socket-info
                    write-queue write-limit write-queue-cap write-admission
                    control-queue control-queue-cap close-ex close-callback])

(defn- queue-write-entry!
  [ctx buffer callback completion]
  (when-not (acquire-write-admission! ctx)
    (throw (ex-socket-closed ctx)))
  (try
    (let [n (if (instance? teensyp.buffer.Buffer buffer)
              (long (buf/remaining buffer))
              0)]
      (when (pos? n) (reserve-write-limit! ctx n))
      (if (q-offer! (:write-queue ctx) (:write-queue-cap ctx)
                    [buffer callback completion])
        (do
          (set-flag! ctx WRITING)
          (mark-pending! ctx)
          (wake-server! (:srv ctx)))
        (do
          ;; Byte credit is reserved before the slot CAS so both limits remain
          ;; independently linearizable. Losing the slot race returns exactly
          ;; the credit this attempted entry reserved.
          (when (pos? n) (swap! (:write-limit ctx) + n))
          (throw (ex-write-queue-full)))))
    (finally
      (release-write-admission! ctx))))

(extend-type Context
  Socket
  (try-write [_ _] false)
  (queue-control [ctx event callback]
    (when-not (q-offer! (:control-queue ctx) (:control-queue-cap ctx)
                        [event callback])
      (throw (ex-control-queue-full ctx)))
    (mark-pending! ctx)
    (wake-server! (:srv ctx)))
  (queue-write [ctx buffer callback]
    (queue-write-entry! ctx buffer callback nil))
  (socket-info [ctx] (:socket-info ctx))
  (socket-lock [ctx] (:lock ctx))

  CompletionSocket
  (queue-write-completion [ctx buffer completion]
    (queue-write-entry! ctx buffer nil completion)))

;; --- public convenience fns (identical surface to JVM teensyp) -------------
(defn write
  "Write a Buffer to a socket. Optional zero-arg callback fires once all bytes
  are written. Throws ExceptionInfo if the write-queue/byte limits are exceeded."
  ([socket buffer] (write socket buffer nil))
  ([socket buffer callback]
   (with-lock (socket-lock socket)
     (if (try-write socket buffer)
       (when callback (callback))
       (queue-write socket buffer callback)))))

(defn write-completion
  "Write a Buffer and return a promise that settles exactly once.

  A successful write delivers `{:status :written}` after every byte has been
  accepted by the native socket. A connection/write failure delivers
  `{:status :failed :exception ex}` before the connection is retired. Queue
  admission errors remain synchronous exceptions. When admission finds an
  already-closed socket, the first recorded close failure is retained as the
  synchronous `::socket-closed` exception's cause.

  This is the failure-aware completion surface for blocking adapters. The
  optional zero-arg callback accepted by [[write]] remains success-only for
  backward compatibility."
  [socket buffer]
  (let [completion (promise)]
    (with-lock (socket-lock socket)
      (if (try-write socket buffer)
        (deliver completion {:status :written})
        (queue-write-completion socket buffer completion)))
    completion))

(defn close
  "Queue the socket to be closed. Optional zero-arg callback fires after close."
  ([socket] (close socket nil))
  ([socket callback]
   (with-lock (socket-lock socket)
     (queue-write socket ::close callback))))

(defn pause-reads
  "Pause reads for this socket (control event, limited by :control-queue-size)."
  ([socket]          (queue-control socket ::pause-reads nil))
  ([socket callback] (queue-control socket ::pause-reads callback)))

(defn resume-reads
  "Resume reads; forces a read-handler call if buffered data waits."
  ([socket]          (queue-control socket ::resume-reads nil))
  ([socket callback] (queue-control socket ::resume-reads callback)))

(defn peer-eof-notified?
  "True once the peer has half-closed **and** the handler's terminal read
  notification for it is current: the read view has been refreshed, so every
  byte that arrived before EOF is visible in the view this invocation was
  handed, and none is hidden behind a later one.

  This is the predicate a protocol wants when deciding to release a connection.
  [[peer-closed?]] answers a different and weaker question — the reactor has
  *observed* EOF — and can become true while an older handler invocation is
  still running against a view taken before EOF. Acting on that is how a server
  closes a connection whose remaining buffered requests it has not yet parsed,
  or preempts a decision the current invocation was about to make correctly.

  In short: [[peer-closed?]] means \"no more bytes will arrive\";
  [[peer-eof-notified?]] means \"and you are now looking at all of them\"."
  [socket]
  (not (zero? (bit-and (long @(:flags socket)) (long EOF-SEEN)))))

(defn peer-closed?
  "True once the peer has closed its write side, so no further data will arrive.

  The peer may still be reading, so this is not a signal to stop writing — it
  means the current exchange is the last one on this connection. The read arity
  is called once when this becomes true (possibly with an empty buffer), and it
  is the handler's job to close the socket once it has finished responding."
  [socket]
  (not (zero? (bit-and (long @(:flags socket)) (long EOF)))))

;; --- read/write buffer plumbing -------------------------------------------
(defn- compact-by!
  "Drop the first `n` (consumed) bytes of read-buffer `b` in place, shifting the
  live received-unconsumed bytes to the front; position -= n, limit unchanged."
  [b n]
  (when (pos? (long n))
    (let [^bytes arr (:arr b) pos (buf/position b) n (long n) keep (- pos n)]
      (loop [i 0] (when (< i keep) (aset arr i (aget arr (+ n i))) (recur (inc i))))
      (buf/set-position! b keep))))

(defn- recv-into!
  "Read into read-buffer's free space through jolt.net. Returns a positive byte
  count, ::net/eof, or ::net/would-block. EOF remains distinct because a peer
  that half-closes its write side is still waiting for a response."
  [ctx]
  (let [rb (:read-buffer ctx) cap (long (buf/capacity rb)) pos (buf/position rb)
        space (- cap pos)]
    (if (<= space 0)
      0
      (let [n (net/try-read-bytes! (:socket ctx) (:arr rb) pos space)]
        (if (number? n)
          (do (buf/set-position! rb (+ pos (long n))) (long n))
          n)))))

(defn- drop-consumed!
  "Drop the bytes the handler consumed from the read buffer and rebase the
  read-view onto the compacted buffer.

  Every site that hands the handler a *new* view must call this first, or the
  view is rebuilt over bytes that were already delivered and the handler sees
  them twice. Zeroing the view's position (and shifting its limit by the same
  amount) keeps the view's coordinates consistent with the compacted buffer and
  makes a second call a no-op, so the sites that compact before recv and the
  sites that compact before submitting can both call it without double-dropping.

  Only ever called when the connection is not WORKING, so no handler holds the
  view while it is rebased."
  [ctx]
  (let [rv (:read-view ctx) n (buf/position rv)]
    (when (pos? n)
      (compact-by! (:read-buffer ctx) n)
      (buf/set-limit! rv (- (buf/limit rv) n))
      (buf/set-position! rv 0))))

(defn- update-read-view! [ctx]
  (let [rb (:read-buffer ctx) rv (:read-view ctx)]
    (buf/set-position! rv 0)
    (buf/set-limit! rv (buf/position rb))))

(defn- finish-work! [ctx]
  (unset-flag! ctx WORKING)
  (mark-pending! ctx)
  (wake-server! (:srv ctx)))

(defn- submit-marked-handler!
  "Submit one accept/read handler arity whose WORKING reservation is published.

  Executor rejection is synchronous and means the task will never run its
  `finally`. Treat it like any other handler failure, but clear WORKING here so
  the reactor can retire the connection and stop cannot wedge forever waiting
  for a worker that was never admitted."
  [srv ctx f]
  (try
    (submit srv
            (fn []
              (try
                (f)
                (catch :default e (handle-close ctx e))
                (finally (finish-work! ctx)))))
    true
    (catch :default e
      (report-error srv e)
      (handle-close ctx e)
      (finish-work! ctx)
      false)))

(defn- submit-handler!
  "Publish WORKING and submit one read handler arity."
  [srv ctx f]
  (set-flag! ctx WORKING)
  (submit-marked-handler! srv ctx f))

(defn- submit-read-handler [srv ctx]
  (update-read-view! ctx)
  ;; The view now shows every byte that arrived before EOF, so this is the exact
  ;; moment the terminal notification becomes true for the handler — which is
  ;; what [[peer-eof-notified?]] reports. Setting it here rather than at each
  ;; call site keeps "view refreshed" and "EOF observable" inseparable, and
  ;; WORKING (set immediately below) is what stops a second invocation being
  ;; scheduled for the same notification.
  (when (flag? ctx EOF) (set-flag! ctx EOF-SEEN))
  (submit-handler!
   srv ctx
   #(vswap! (:state ctx) (:handler (:opts srv)) ctx (:read-view ctx))))

(defn- send-chunk!
  "Send `item` directly from its mutable backing array. The exact Buffer remains
  queued and therefore owned by the server until its completion callback fires."
  [ctx item]
  (let [pos (buf/position item)
        sent (net/try-write-bytes! (:socket ctx) (:arr item)
                                   pos (buf/remaining item))]
    (if (number? sent)
      (do (buf/set-position! item (+ pos (long sent))) sent)
      sent)))

;; --- reactor-side event handlers -------------------------------------------
(defn- remember-close-ex!
  "Retain the first connection failure for close arity and late-write causes."
  [ctx ex]
  (swap! (:close-ex ctx) (fn [current] (or current ex))))

(defn- handle-close
  "Request closure and mark CLOSED. Safe to call from a worker or the reactor.

  Deliberately does not close the owned socket: only handle-pending-close on the
  reactor retires its current registration and handle."
  ([ctx ex] (handle-close ctx ex nil))
  ([ctx ex callback]
   (remember-close-ex! ctx ex)
   (when callback
     (swap! (:close-callback ctx) (fn [c] (or c callback))))
   ;; Closing write admission before publishing CLOSED prevents a producer that
   ;; observed the old flags from enqueueing after failure settlement drains the
   ;; queue.
   (close-write-admission! ctx)
   (set-flag! ctx CLOSED)
   (mark-pending! ctx)
   (wake-server! (:srv ctx))))

(defn- ex-write-aborted [ctx]
  (ex-info "Queued write did not complete before socket close"
           {:err ::write-aborted
            :fd (:fd ctx)
            :generation (:generation ctx)}))

(defn- fail-pending-writes!
  "Settle every queued outcome as failed and return all reserved byte credit.

  Legacy zero-arg callbacks remain success-only and are deliberately not run.
  A queued close callback is retained for the ordinary close boundary."
  [srv ctx ex]
  (let [failure {:status :failed
                 :exception (or ex (ex-write-aborted ctx))}]
    (unset-flag! ctx WRITING)
    (loop []
      (when-some [[item callback completion] (q-poll! (:write-queue ctx))]
        (if (= item ::close)
          (when callback
            (swap! (:close-callback ctx) (fn [c] (or c callback))))
          (do
            (when (instance? teensyp.buffer.Buffer item)
              (swap! (:write-limit ctx) + (long (buf/remaining item))))
            (when completion (deliver completion failure))))
        (recur)))))

(defn- fail-connection!
  "Reactor-owned connection failure path: revoke writes, settle outcomes, close."
  [srv ctx ex]
  ;; Publish the cause before write admission becomes closed. A concurrent
  ;; write-completion then throws synchronously, as documented, while still
  ;; retaining this first failure through ex-cause.
  (remember-close-ex! ctx ex)
  (close-write-admission! ctx)
  (fail-pending-writes! srv ctx ex)
  (handle-close ctx ex))

(defn- handle-eof!
  "The peer half-closed its write side: no more data is coming, but it is still
  reading and is owed a response.

  This does **not** close the connection. Closing on EOF discards the response
  to the request that was just sent — an ordinary client pattern (send request,
  `shutdown(SHUT_WR)`, read reply) that many HTTP clients use. Nor can the
  reactor tell when it is safe to close on the application's behalf: a handler
  running on a worker thread has no flag raised here, so \"nothing queued and
  nobody working\" does not mean \"no response is owed\".

  Instead the handler is notified (via a final read-arity call, where it can see
  [[peer-closed?]]) and owns the decision to close."
  [srv ctx]
  (set-flag! ctx EOF)
  (when-not (flag? ctx WORKING)
    ;; Must drop the consumed prefix first. handle-read only compacts when it
    ;; saw WORKING clear on entry; if the handler finished in the window between
    ;; that check and the recv that returned :eof, nothing has been compacted and
    ;; the view below would re-deliver every byte the handler just consumed. That
    ;; race showed up as a client receiving more bytes than it sent.
    (drop-consumed! ctx)
    (submit-read-handler srv ctx))
  ;; If the handler *was* working, the notification is not lost: handle-pending-read
  ;; delivers it once the handler finishes. Nothing else would — no more data is
  ;; coming, so the "new data arrived" path there never fires again.
  (mark-pending! ctx)
  (wake-server! srv))

(defn- handle-read [srv ctx]
  ;; A half-closed socket stays readable forever; once EOF is recorded there is
  ;; nothing more to read and re-entering here would spin.
  (when-not (flag? ctx EOF)
    (let [working? (flag? ctx WORKING)
          rb (:read-buffer ctx) rv (:read-view ctx)]
      (when-not working?
        (drop-consumed! ctx))
      (when (< (buf/position rb) (long (buf/capacity rb)))
        (let [n (recv-into! ctx)]
          (cond
            (net/eof? n)         (handle-eof! srv ctx)
            (net/would-block? n) nil
            (= n 0)              nil
            :else (do (when (>= (buf/position rb) (long (buf/capacity rb))) (set-flag! ctx FULL))
                      (when-not working? (submit-read-handler srv ctx)))))))))

(defn- handle-write [srv ctx]
  (unset-flag! ctx WRITING)
  (loop []
    (when-some [[item callback completion] (q-peek (:write-queue ctx))]
      (if (= item ::close)
        (do (q-poll! (:write-queue ctx))
            (handle-close ctx nil callback))
        (if (zero? (buf/remaining item))
          (do
            (q-poll! (:write-queue ctx))
            (when callback (submit-callback srv callback))
            (when completion (deliver completion {:status :written}))
            (recur))
          (let [r (send-chunk! ctx item)]
            (cond
              (net/would-block? r) (set-flag! ctx WRITING)
              :else (do (swap! (:write-limit ctx) + (long r))
                        (if (buf/has-remaining? item)
                          (set-flag! ctx WRITING)
                          (do
                            (q-poll! (:write-queue ctx))
                            (when callback (submit-callback srv callback))
                            (when completion
                              (deliver completion {:status :written}))
                            (recur))))))))))
  nil)

(defn- has-read-data? [ctx]
  (> (buf/position (:read-buffer ctx)) (buf/position (:read-view ctx))))

(defn- handle-pending-read [srv ctx]
  (let [rb (:read-buffer ctx) rv (:read-view ctx)]
    (when (pos? (buf/position rv)) (unset-flag! ctx FULL))
    (cond
      ;; Data arrived while the handler was busy.
      (> (buf/position rb) (buf/limit rv))
      (do (drop-consumed! ctx)
          (submit-read-handler srv ctx))

      ;; EOF arrived while the handler was busy. The contract promises the read
      ;; arity is called once when peer-closed? becomes true, and this is the only
      ;; place that can still honour it: no further data will arrive, so the
      ;; branch above never fires again and the handler would never learn the peer
      ;; had gone. A handler waiting for peer-closed? before replying then hangs
      ;; the connection until the client gives up.
      (and (flag? ctx EOF) (not (flag? ctx EOF-SEEN)))
      (do (drop-consumed! ctx)
          (submit-read-handler srv ctx)))))

(defn- current-context?
  "True only when ctx is still the registered socket ownership generation."
  [srv ctx]
  (identical? ctx (get @(:conns srv) (:generation ctx))))

(defn- track-task!
  "Submit f and retain only its active completion promise.

  Cleanup freezes admission before awaiting the active set, so completed server
  history is neither retained nor repeatedly traversed."
  [srv executor tracker f]
  (let [done (promise)
        tracked? (register-task! tracker done)
        run (fn []
              (try (f)
                   (catch :default e (report-error srv e))
                   (finally
                     (deliver done :done)
                     (when tracked? (finish-task! tracker done)))))]
    (if-not tracked?
      (run)
      (try
        (execute! executor run)
        (catch :default e
          ;; A caller-supplied executor may have been shut down independently.
          ;; Report that ownership violation, but preserve the close/callback
          ;; contract and completion boundary instead of leaking a promise.
          (report-error srv e)
          (run))))
    done))

(defn- submit-handler-close! [srv ctx]
  (track-task! srv (:executor srv) (:handler-closes srv)
               #((:handler (:opts srv)) @(:state ctx) @(:close-ex ctx))))

(defn- handle-pending-close
  "Finalize one current connection on the reactor: remove its identity before
  closing its owned socket, then schedule public close callbacks."
  [srv ctx]
  (when (and (flag? ctx CLOSED) (not (flag? ctx WORKING))
             (current-context? srv ctx))
    (fail-pending-writes! srv ctx @(:close-ex ctx))
    (swap! (:conns srv) dissoc (:generation ctx))
    (try (net/remove-registration! (:poller srv) @(:token ctx))
         (catch :default e (report-error srv e)))
    (try (net/close! (:socket ctx))
         (catch :default e (report-error srv e)))
    (when-let [callback @(:close-callback ctx)]
      (submit-callback srv callback))
    (submit-handler-close! srv ctx)))

(defn- handle-control [srv ctx]
  (loop [resumed? false]
    (if-some [[event callback] (q-poll! (:control-queue ctx))]
      (case event
        ::pause-reads  (do (set-flag! ctx PAUSED)   (when callback (submit-callback srv callback)) (recur resumed?))
        ::resume-reads (do (unset-flag! ctx PAUSED) (when callback (submit-callback srv callback)) (recur true)))
      ;; Resubmit when there is buffered input *or* the terminal EOF
      ;; notification has already been delivered. Without the second case a
      ;; protocol that answers on a worker thread has no way to be re-entered
      ;; after its response: nothing new will arrive, so the has-read-data?
      ;; branch never fires again, and the connection is never released. That is
      ;; what forced jolt-http's off-thread responder to close the socket
      ;; itself, which it cannot do safely.
      (when (and resumed? (not (flag? ctx WORKING))
                 (or (has-read-data? ctx) (flag? ctx EOF-SEEN)))
        (drop-consumed! ctx)
        (submit-read-handler srv ctx)))))

(defn- handle-pending [srv ctx]
  (if (flag? ctx WORKING)
    false
    (do (if (flag? ctx CLOSED)
          (handle-pending-close srv ctx)
          (do (handle-pending-read srv ctx)
              (handle-control srv ctx)))
        true)))

;; --- accept + context creation ---------------------------------------------
(defn- portable-endpoint
  "Remove jolt.net's implementation namespace from the public TCP boundary."
  [endpoint]
  {:host (:jolt.net/host endpoint)
   :port (:jolt.net/port endpoint)
   :family (:jolt.net/family endpoint)})

(defn- new-context [srv socket token]
  (let [{:keys [read-buffer-size write-buffer-size write-queue-size control-queue-size]} (:opts srv)
        rb (buf/buffer read-buffer-size)
        generation (:jolt.net/generation token)
        fd (net/native-handle socket)
        local (portable-endpoint (net/local-endpoint socket))
        remote (portable-endpoint (net/peer-endpoint socket))]
    (map->Context
     {:srv srv :socket socket :token (atom token) :interests (atom #{:read})
      :fd fd :generation generation
      :lock (java.util.concurrent.locks.ReentrantLock.)
      :flags (atom 0) :state (volatile! nil)
      ;; read-view starts empty (position 0, limit 0), like teensyp's
      ;; (.. read-buffer duplicate flip). A capacity-limit here would make
      ;; handle-pending-read's (> rb.position rv.limit) guard never fire when a
      ;; read arrives while the accept arity is still WORKING.
      :read-buffer rb :read-view (buf/flip (buf/duplicate rb))
      :socket-info {:local-address local
                    :remote-address remote
                    :fd fd}
      :write-queue (atom clojure.lang.PersistentQueue/EMPTY) :write-limit (atom write-buffer-size)
      :write-queue-cap write-queue-size
      :write-admission (atom {:phase :open :active 0})
      :control-queue (atom clojure.lang.PersistentQueue/EMPTY) :control-queue-cap control-queue-size
      :close-ex (atom nil)
      :close-callback (atom nil)})))

(defn- accepting? [srv]
  (and @(:running? srv)
       (= :open (:phase @(:admission srv)))))

(defn- with-accept-publication
  "Run one accept publication wholly before or after the stop phase CAS."
  [srv f]
  (with-cas-gate (:accept-gate srv)
                 #(when (accepting? srv) (f))))

(defn- discard-accepted!
  [srv socket token]
  (when token
    (try (net/remove-registration! (:poller srv) token)
         (catch :default _ nil)))
  (try (net/close! socket) (catch :default _ nil))
  nil)

(defn- admit-accepted!
  "Register and publish one accepted socket, rolling back every pre-publication
  resource if registration, metadata construction, or stop cancellation wins."
  [srv socket]
  (if-not (accepting? srv)
    (do (discard-accepted! srv socket nil) false)
    (let [token* (atom nil)
          published? (atom false)]
      (try
        (let [token (net/register! (:poller srv) socket #{:read})]
          (reset! token* token)
          (let [ctx (new-context srv socket token)]
            (boolean
             (when-let
              [published-ctx
               (with-accept-publication
                 srv
                 (fn []
                   ;; Registration and complete diagnostic metadata precede
                   ;; exposure to either the registry or the accept handler.
                   ;;
                   ;; Reserve handler ownership before publishing the Context.
                   ;; Stop may linearize as soon as this short gate is released,
                   ;; so it must never observe the newly visible Context as idle
                   ;; in the gap before executor admission.
                   (set-flag! ctx WORKING)
                   (swap! (:conns srv) assoc (:generation ctx) ctx)
                   (reset! published? true)
                   ctx))]
               ;; Executor.execute is deliberately outside accept-gate. Executor
               ;; permits a legal direct implementation that invokes the handler
               ;; inline; that handler may request stop, which must be able to
               ;; acquire the same gate and reach public stop's bounded wait.
               (submit-marked-handler!
                srv published-ctx
                #(vreset! (:state published-ctx)
                          ((:handler (:opts srv)) published-ctx)))))))
        (finally
          (when-not @published?
            (discard-accepted! srv socket @token*)))))))

(defn- do-accept [srv]
  (let [batch-size (long (:accept-batch-size (:opts srv)))]
    (loop [accepted 0]
      (when (and (< accepted batch-size) (accepting? srv))
        (let [socket (net/try-accept (:listener srv) {:no-delay? true})]
          (when-not (net/would-block? socket)
            (admit-accepted! srv socket)
            (recur (inc accepted))))))))

;; --- interest + reactor loop -----------------------------------------------
(defn- interest [ctx]
  (let [f (flags-of ctx)]
    (cond-> #{}
      (zero? (bit-and f READ-MASK)) (conj :read)
      (and (not (zero? (bit-and f WRITING)))
           (zero? (bit-and f CLOSED))) (conj :write))))

(defn- default-error-logger [e]
  (binding [*out* *err*] (prn e)))

(defn- report-error
  "Report a reactor-side error without letting it escape. The reactor runs in a
  `future`, so an escaping exception would be swallowed with no diagnostic."
  [srv e]
  (try ((:error-logger (:opts srv)) e) (catch :default _ nil)))

(defn- cleanup-call
  "Run one cleanup action without preventing the remaining resources from being
  released. Cleanup errors are reported through the server's ordinary logger."
  [srv f]
  (try (f) (catch :default e (report-error srv e))))

(defn- freeze-and-await-tasks!
  "Close task admission, then wait until the active set is stably empty."
  [tracker]
  (swap! tracker assoc :accepting? false)
  (loop []
    (let [active (:active @tracker)]
      (when (seq active)
        (doseq [done active] (deref done))
        (recur))))
  nil)

(defn- handle-shutdown-controls!
  "Complete control operations published by an already-active handler.

  Resume changes the flag but deliberately does not submit a read arity: stop
  has revoked new application work. Completion callbacks still run because an
  active handler is allowed to use one as its release fence."
  [srv ctx]
  (loop []
    (when-some [[event callback] (q-poll! (:control-queue ctx))]
      (case event
        ::pause-reads  (set-flag! ctx PAUSED)
        ::resume-reads (unset-flag! ctx PAUSED))
      (when callback (submit-callback srv callback))
      (recur))))

(defn- prepare-shutdown-active!
  "Publish write interests and control callbacks for currently active handlers.

  This is reactor-only. It intentionally omits handle-pending-read and
  handle-pending-close: shutdown must not admit a new read arity, and socket
  retirement remains the cleanup phase after every active arity returns."
  [srv]
  (drain-pending! (:pending srv))
  (doseq [ctx (vals @(:conns srv))]
    (when (and (flag? ctx WORKING) (current-context? srv ctx))
      (try
        (handle-shutdown-controls! srv ctx)
        (reconcile-shutdown-registration! srv ctx)
        (catch :default e
          (report-error srv e)
          (try
            (fail-connection! srv ctx e)
            (reconcile-shutdown-registration! srv ctx)
            (catch :default _ nil)))))))

(defn- handle-shutdown-events!
  "Service only writes needed by an already-active handler.

  Read/hangup readiness is intentionally ignored: delivering it would schedule
  new application work after stop. A write failure is still contained to the
  connection by the same failure path used by the ordinary reactor."
  [srv ctx events]
  (try
    (when (and (flag? ctx WORKING)
               (not (flag? ctx CLOSED))
               (contains? events :write))
      (handle-write srv ctx))
    (reconcile-shutdown-registration! srv ctx)
    (catch :default e
      (report-error srv e)
      (try
        (fail-connection! srv ctx e)
        (reconcile-shutdown-registration! srv ctx)
        (catch :default _ nil)))))

(defn- quiesce-handlers!
  "Drain writes/control callbacks needed by active accept/read arities until no
  handler owns a Context.

  Stop has already retired the listener. The reactor remains the sole
  registration and socket-I/O owner during this phase, but it neither accepts
  nor dispatches new read arities. This prevents a handler blocked on its own
  write callback from deadlocking cleanup."
  [srv]
  (loop []
    (when (some #(flag? % WORKING) (vals @(:conns srv)))
      (prepare-shutdown-active! srv)
      (doseq [{:keys [token events]} (net/await-ready (:poller srv) 1000)]
        (when-let [ctx (get @(:conns srv)
                            (:jolt.net/generation token))]
          (when (= token @(:token ctx))
            (handle-shutdown-events! srv ctx events))))
      (recur))))

(defn- shutdown-and-await! [executor]
  (.shutdown executor)
  (loop []
    (when-not (.isTerminated executor)
      (Thread/yield)
      (recur))))

(defn- retire-registration!
  [srv token socket]
  (when token
    (cleanup-call srv #(net/remove-registration! (:poller srv) token)))
  (cleanup-call srv #(net/close! socket)))

(defn- cleanup-server!
  "Stop exposure, quiesce handlers, retire registrations and sockets, run and
  await close arities, close the poller and owned executors, then publish stop."
  [srv]
  (when (compare-and-set! (:cleanup-started? srv) false true)
    (try
      (reset! (:running? srv) false)

      ;; The reactor is the only caller of registration mutations. Stop listener
      ;; exposure first, while the poller remains available to active workers.
      (retire-registration! srv @(:listener-token srv) (:listener srv))

      (quiesce-handlers! srv)

      (let [ctxs (vals @(:conns srv))]
        (reset! (:conns srv) {})
        (doseq [ctx ctxs]
          (handle-close ctx @(:close-ex ctx))
          (fail-pending-writes! srv ctx @(:close-ex ctx))
          (retire-registration! srv @(:token ctx) (:socket ctx))
          (when-let [callback @(:close-callback ctx)]
            (submit-callback srv callback))
          (submit-handler-close! srv ctx)))

      ;; A close arity is last in its handler sequence and stop does not complete
      ;; until every one has returned, including closes scheduled before stop.
      (freeze-and-await-tasks! (:handler-closes srv))
      (freeze-and-await-tasks! (:callback-tasks srv))

      (cleanup-call srv #(net/close! (:poller srv)))

      (let [shutdown-ex? (:shutdown-executor? srv)
            shutdown-cb? (:shutdown-callback-executor? srv)]
        (when shutdown-ex?
          (cleanup-call srv #(shutdown-and-await! (:executor srv))))
        (when (and shutdown-cb?
                   (not (and shutdown-ex?
                             (identical? (:executor srv)
                                         (:callback-executor srv)))))
          (cleanup-call srv #(shutdown-and-await! (:callback-executor srv)))))
      (finally
        (deliver (:stopped srv) :stopped)))))

(defn- cleanup-startup!
  "Release resources acquired before a complete server can be handed to the
  reactor. Caller-supplied executors are borrowed by default; an explicit
  shutdown opt-in authorizes cleanup here as well as after a successful start."
  [state]
  (when (compare-and-set! (:cleanup-started? state) false true)
    (doseq [owned (remove nil? [(:listener state) (:poller state)])]
      (try (net/close! owned) (catch :default _ nil)))
    (when (:shutdown-executor? state)
      (try (shutdown-and-await! (:executor state)) (catch :default _ nil)))
    (when (and (:shutdown-callback-executor? state)
               (not (and (:shutdown-executor? state)
                         (identical? (:executor state)
                                     (:callback-executor state)))))
      (try (shutdown-and-await! (:callback-executor state))
           (catch :default _ nil)))))

(defn- run-after-reactor-start!
  "Run `f` only after construction commits the server handoff.

  A constructor failure after future-call returns delivers :abort instead. The
  catch path then owns cleanup, and the scheduled future must not touch the
  owned sockets or poller it just released."
  [reactor-start f]
  (when (= :start (deref reactor-start))
    (f)))

(defn- reconcile-registration!
  "Apply one Context's flag-derived interest set. Reactor thread only."
  [srv ctx]
  (when (and (current-context? srv ctx) (not (flag? ctx CLOSED)))
    (let [desired (interest ctx)]
      (when (not= desired @(:interests ctx))
        (let [token (net/update-registration! (:poller srv)
                                              @(:token ctx) desired)]
          (reset! (:token ctx) token)
          (reset! (:interests ctx) desired))))))

(defn- reconcile-shutdown-registration!
  "During stop, retain only write readiness needed by an active handler.

  Removing :read is both the admission boundary (no new reads after stop) and a
  liveness detail: unread data or EOF is level-triggered, so leaving :read
  registered while intentionally ignoring it would make the shutdown loop spin."
  [srv ctx]
  (when (current-context? srv ctx)
    (let [desired (if (and (not (flag? ctx CLOSED))
                           (flag? ctx WRITING))
                    #{:write}
                    #{})]
      (when (not= desired @(:interests ctx))
        (let [token (net/update-registration! (:poller srv)
                                              @(:token ctx) desired)]
          (reset! (:token ctx) token)
          (reset! (:interests ctx) desired))))))

(defn- handle-conn-events
  "Run one connection's poll events, containing any failure to that connection.

  A throw here used to propagate out of the reactor loop and silently kill the
  whole server: the future swallowed it, `running?` stayed true, and every
  connection — existing and new — stopped being served with no log. One bad
  connection must only ever cost that connection."
  [srv ctx events]
  (try
    (when-not (flag? ctx CLOSED)
      ;; Read first when POLLIN and HUP arrive together. try-read drains the final
      ;; bytes before recording EOF, so hangup never discards a request tail.
      (when (or (contains? events :read)
                (contains? events :hangup)
                (contains? events :error))
        (handle-read srv ctx))
      (when (and (not (flag? ctx CLOSED))
                 (contains? events :write))
        (handle-write srv ctx))
      (reconcile-registration! srv ctx))
    (catch :default e
      (report-error srv e)
      (try (fail-connection! srv ctx e) (catch :default _ nil)))))

(defn- process-pending! [srv]
  (doseq [generation (drain-pending! (:pending srv))]
    (when-let [ctx (get @(:conns srv) generation)]
      (try
        (let [handled? (handle-pending srv ctx)]
          ;; WORKING only serializes handler arities. It must not defer readiness
          ;; changes: a handler is allowed to wait for its own write callback,
          ;; which requires the reactor to enable :write while that handler is
          ;; still running.
          (reconcile-registration! srv ctx)
          (when-not handled?
            (swap! (:pending srv) conj generation)))
        (catch :default e
          (report-error srv e)
          (try (fail-connection! srv ctx e) (catch :default _ nil)))))))

(defn- reactor-loop [srv]
  (try
    ;; Registration is performed by the reactor before run-server exposes its
    ;; handle. Every subsequent register/update/remove happens on this thread.
    (let [listener-token (net/register! (:poller srv) (:listener srv) #{:read})]
      (reset! (:listener-token srv) listener-token)
      (deliver (:reactor-ready srv) {:ok true})
      (loop []
        (when @(:running? srv)
          (with-running-admission srv #(process-pending! srv))
          (when @(:running? srv)
            (doseq [{:keys [token events]}
                    (net/await-ready (:poller srv) 1000)]
              (with-running-admission
                srv
                (fn []
                  (if (= token @(:listener-token srv))
                    (when (contains? events :read)
                      (try (do-accept srv)
                           (catch :default e (report-error srv e))))
                    (when-let [ctx (get @(:conns srv)
                                        (:jolt.net/generation token))]
                      (when (= token @(:token ctx))
                        (handle-conn-events srv ctx events))))))))
          (recur))))
    (catch :default e
      (deliver (:reactor-ready srv) {:error e})
      (report-error srv e))
    (finally
      (cleanup-server! srv))))

;; --- public entry point ----------------------------------------------------
(defn run-server
  "Start a TCP server. Options (map or kwargs):
    :port (required)             :handler (required, 3-arity fn)
    :read-buffer-size   8192     :write-buffer-size 32768
    :write-queue-size   64       :control-queue-size 32
    :accept-batch-size  64       max accepts serviced per readiness turn
    :reuse-address?     false    :recv-buffer-size  (SO_RCVBUF)
    :executor (custom pool)      :pool-size 4  (default fixed-pool workers;
                                                jolt's availableProcessors is 1)
    :error-logger  (fn [ex])     called on a reactor-side error; the affected
                                 connection is closed but the server keeps
                                 running (defaults to printing to *err*)
    :callback-executor           runs write/control completion callbacks; kept
                                 separate from :executor so a handler blocked on
                                 its own write cannot starve the callback that
                                 releases it; the same object is rejected
    :callback-pool-size 2        size of the default dedicated fixed pool
    :shutdown-executor? false    adopt and shut down a supplied :executor;
                                 server-created executors are always shut down
    :shutdown-callback-executor? false
                                 adopt and shut down a supplied callback pool;
                                 server-created pools are always shut down
    :stop-timeout-ms    5000     maximum time stop-server waits for reactor
                                 cleanup before throwing ::stop-timeout

  Returns a handle map usable with `stop-server` and `with-open` (its :close
  runs stop). See the namespace doc for the handler contract."
  [& opts]
  (let [{:keys [port handler read-buffer-size write-buffer-size write-queue-size
                control-queue-size accept-batch-size
                reuse-address? recv-buffer-size executor pool-size
                error-logger callback-executor callback-pool-size
                shutdown-executor? shutdown-callback-executor?
                stop-timeout-ms]
         :or   {read-buffer-size 8192 write-buffer-size 32768 write-queue-size 64
                control-queue-size 32 accept-batch-size 64
                pool-size 4 callback-pool-size 2
                shutdown-executor? false
                shutdown-callback-executor? false
                stop-timeout-ms 5000
                error-logger default-error-logger}
         :as _m}
        (if (and (= 1 (count opts)) (map? (first opts))) (first opts) (apply hash-map opts))]
    (assert (int? port) ":port is required and must be an int")
    (assert (fn? handler) ":handler is required")
    (assert (and (number? stop-timeout-ms) (pos? (long stop-timeout-ms)))
            ":stop-timeout-ms must be a positive number")
    (assert (and (int? pool-size) (pos? (long pool-size)))
            ":pool-size must be a positive int")
    (assert (and (int? callback-pool-size)
                 (pos? (long callback-pool-size)))
            ":callback-pool-size must be a positive int")
    (assert (and (int? accept-batch-size)
                 (pos? (long accept-batch-size)))
            ":accept-batch-size must be a positive int")
    (let [cleanup-started? (atom false)
          startup (atom {:cleanup-started? cleanup-started?})]
      (try
        (let [listener (net/listen
                        (net/endpoint "127.0.0.1" port)
                        {:reuse-address? reuse-address?
                         :recv-buffer-size recv-buffer-size})]
          (swap! startup assoc :listener listener)
          (let [poller (net/open-poller)]
            (swap! startup assoc :poller poller)
            (let [owns-executor? (nil? executor)
                  ex (or executor
                         (java.util.concurrent.Executors/newFixedThreadPool
                          pool-size))
                  shutdown-executor-on-cleanup?
                  (or owns-executor? (true? shutdown-executor?))]
              (swap! startup assoc
                     :executor ex
                     :shutdown-executor? shutdown-executor-on-cleanup?)
              (let [owns-callback-executor? (nil? callback-executor)
                    cb-ex (or callback-executor
                              (java.util.concurrent.Executors/newFixedThreadPool
                               callback-pool-size))
                    shutdown-callback-on-cleanup?
                    (or owns-callback-executor?
                        (true? shutdown-callback-executor?))]
                (swap! startup assoc
                       :callback-executor cb-ex
                       :shutdown-callback-executor?
                       shutdown-callback-on-cleanup?)
                (when (identical? ex cb-ex)
                  (throw
                   (ex-info
                    ":executor and :callback-executor must be different pools"
                    {:err ::shared-handler-callback-executor})))
                (let [running? (atom true)
                    admission (atom {:phase :open :active 0})
                    accept-gate (atom false)
                    stopped (promise)
                    reactor-start (promise)
                    reactor-ready (promise)
                    listener-token (atom nil)
                    actual-port (:jolt.net/port (net/local-endpoint listener))
                    listen-fd (net/native-handle listener)
                    srv {:conns (atom {}) :pending (atom #{})
                         :executor ex :callback-executor cb-ex
                         :shutdown-executor?
                         shutdown-executor-on-cleanup?
                         :shutdown-callback-executor?
                         shutdown-callback-on-cleanup?
                         :listener listener :listener-token listener-token
                         :poller poller
                         :running? running? :admission admission
                         :accept-gate accept-gate
                         :stopped stopped
                         :cleanup-started? cleanup-started?
                         :handler-closes (task-tracker)
                         :callback-tasks (task-tracker)
                         :reactor-ready reactor-ready
                         :stop-timeout-ms (long stop-timeout-ms)
                         :opts {:handler handler
                                :read-buffer-size read-buffer-size
                                :write-buffer-size write-buffer-size
                                :write-queue-size write-queue-size
                                :control-queue-size control-queue-size
                                :accept-batch-size accept-batch-size
                                :error-logger error-logger}}]
                (swap! startup assoc
                       :callback-executor cb-ex
                       :shutdown-callback-executor?
                       shutdown-callback-on-cleanup?
                       :srv srv
                       :reactor-start reactor-start)
                (let [reactor-future
                      (future
                        (run-after-reactor-start!
                         reactor-start
                         #(reactor-loop srv)))]
                  (swap! startup assoc :reactor-future reactor-future)
                  (deliver reactor-start :start)
                  (swap! startup assoc :reactor-owned? true)
                  (let [ready (deref reactor-ready)]
                    (when-let [e (:error ready)]
                      (throw e)))
                  (let [stop-fn
                        (fn []
                          (request-stop! srv)
                          (when (= ::stop-timeout
                                   (deref stopped (long stop-timeout-ms)
                                          ::stop-timeout))
                            (throw
                             (ex-info
                              (str "Timed out waiting for server cleanup after "
                                   stop-timeout-ms "ms")
                              {:err ::stop-timeout
                               :timeout-ms (long stop-timeout-ms)
                               :port actual-port})))
                          nil)]
                    {:teensyp/server true :srv srv
                     :listener listener
                     :listen-fd listen-fd
                     :port actual-port
                     :stop stop-fn :close stop-fn
                     :running? running? :stopped stopped
                     :reactor-future reactor-future})))))))
        (catch :default e
          (if (:reactor-owned? @startup)
            (do
              (request-stop! (:srv @startup))
              (deref (:stopped (:srv @startup))))
            (cleanup-startup! @startup))
          (throw e))))))

(defn stop-server
  "Stop a server started by [[run-server]]: stop accepting, close the listen
  socket, quiesce active handlers, run and await each open connection's
  close-arity, and wait up to `:stop-timeout-ms` for reactor-owned jolt.net
  resources and owned executors. Repeated calls wait on the same completion."
  [server] ((:stop server)) nil)
