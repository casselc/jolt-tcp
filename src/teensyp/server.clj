(ns teensyp.server
  "A teensyp-compatible TCP server for jolt, built on a single poll(2) reactor
  thread over non-blocking BSD sockets (teensyp.ffi-net) plus a worker pool.

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
   - `socket-info` returns {:local-address nil :remote-address nil :fd fd}
     (jolt has no InetSocketAddress; addresses can be added via getpeername later)."
  (:refer-clojure :exclude [])
  (:require [teensyp.ffi-net :as net]
            [teensyp.buffer :as buf]
            [teensyp.concurrent :refer [with-lock]]
            [jolt.ffi :as ffi]))

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

(declare handle-close submit-read-handler)

;; --- small helpers ---------------------------------------------------------
(defn- submit [srv f] (.execute (:executor srv) f))

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
  (.execute (:callback-executor srv) f))

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

;; PersistentQueue-in-an-atom: single producer (the working thread) + single
;; consumer (the reactor), each op an atomic CAS/swap, so no lock needed.
(defn- q-count [qa] (count @qa))
(defn- q-add!  [qa x] (swap! qa conj x))
(defn- q-peek  [qa] (peek @qa))
(defn- q-poll! [qa]
  (loop [] (let [q @qa] (when (seq q) (if (compare-and-set! qa q (pop q)) (peek q) (recur))))))

(defn- drain-pending! [pa]
  (loop [] (let [cur @pa] (if (compare-and-set! pa cur #{}) cur (recur)))))

(defn- pending-key [ctx]
  [(:fd ctx) (:generation ctx)])

(defn- mark-pending! [ctx]
  (swap! (:pending (:srv ctx)) conj (pending-key ctx)))

(defn- acquire-wake-gate!
  "Acquire the short wake ownership gate without depending on thread identity.

  Jolt futures can inherit the same Thread/currentThread bookkeeping cell.
  ReentrantLock consequently may mistake two futures for one reentrant owner,
  admit both, and later fail unlock with `thread does not own mutex`. An atom
  CAS is owner-independent; wake!/close! are non-blocking, so contention lasts
  only for one native call."
  [gate]
  (loop []
    (when-not (compare-and-set! gate false true)
      (Thread/yield)
      (recur))))

(defn- with-wake-gate [srv f]
  (let [gate (:wake-gate srv)]
    (acquire-wake-gate! gate)
    (try
      (f)
      (finally
        (reset! gate false)))))

(defn- wake-server!
  "Admit and perform one ordinary worker wake under the wake ownership gate.

  The non-blocking pipe write stays inside the gate so cleanup cannot close and
  allow reuse of wake-w after this call has observed the server as running."
  [srv]
  (with-wake-gate
    srv
    (fn []
      (when (and @(:running? srv) @(:wake-open? srv))
        (net/wake! (:wake-w srv))))))

(defn- request-stop!
  "Atomically stop ordinary wake admission and perform the one terminal wake.
  Returns true only for the caller that transitioned the server to stopped."
  [srv]
  (with-wake-gate
    srv
    (fn []
      (when (compare-and-set! (:running? srv) true false)
        (when @(:wake-open? srv)
          ;; A failed terminal wake is harmless: poll's finite timeout is the
          ;; fallback, and cleanup still owns the descriptors.
          (try (net/wake! (:wake-w srv)) (catch :default _ nil)))
        true))))

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
    "Map with :local-address, :remote-address (nil on jolt), and :fd.")
  (socket-lock [socket]
    "The socket's ReentrantLock, for atomic multi-writes."))

(defrecord Context [srv fd generation lock flags state read-buffer read-view socket-info
                    write-queue write-limit write-queue-cap
                    control-queue control-queue-cap close-ex close-callback])

(extend-type Context
  Socket
  (try-write [_ _] false)
  (queue-control [ctx event callback]
    (when (>= (q-count (:control-queue ctx)) (long (:control-queue-cap ctx)))
      (throw (ex-control-queue-full ctx)))
    (q-add! (:control-queue ctx) [event callback])
    (mark-pending! ctx)
    (wake-server! (:srv ctx)))
  (queue-write [ctx buffer callback]
    (when (>= (q-count (:write-queue ctx)) (long (:write-queue-cap ctx)))
      (throw (ex-write-queue-full)))
    (when (instance? teensyp.buffer.Buffer buffer)
      (reserve-write-limit! ctx (buf/remaining buffer)))
    (q-add! (:write-queue ctx) [buffer callback])
    (set-flag! ctx WRITING)
    (wake-server! (:srv ctx)))
  (socket-info [ctx] (:socket-info ctx))
  (socket-lock [ctx] (:lock ctx)))

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

(defn close
  "Queue the socket to be closed. Optional zero-arg callback fires after close."
  ([socket]          (queue-write socket ::close nil))
  ([socket callback] (queue-write socket ::close callback)))

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
  "recv into read-buffer's free space. Returns the number of bytes received
  (>=0), or :eof / :error. `:eof` is kept distinct from `:error` because a peer
  that half-closes its write side is still waiting for a response."
  [srv ctx]
  (let [rb (:read-buffer ctx) cap (long (buf/capacity rb)) pos (buf/position rb)
        space (- cap pos)]
    (if (<= space 0)
      0
      (let [nb (:recv-buf srv) r (net/recv* (:fd ctx) nb space)]
        (cond
          (= r :eof)    :eof
          (= r :error)  :error
          (= r :eagain) 0
          :else (do (buf/put-bytes! rb (jolt.ffi/read-array nb r) 0 r) (long r)))))))

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

(defn- submit-read-handler [srv ctx]
  (update-read-view! ctx)
  ;; The view now shows every byte that arrived before EOF, so this is the exact
  ;; moment the terminal notification becomes true for the handler — which is
  ;; what [[peer-eof-notified?]] reports. Setting it here rather than at each
  ;; call site keeps "view refreshed" and "EOF observable" inseparable, and
  ;; WORKING (set immediately below) is what stops a second invocation being
  ;; scheduled for the same notification.
  (when (flag? ctx EOF) (set-flag! ctx EOF-SEEN))
  (set-flag! ctx WORKING)
  (submit srv
          (fn []
            (try (vswap! (:state ctx) (:handler (:opts srv)) ctx (:read-view ctx))
                 (catch :default e (handle-close ctx e))
                 (finally (finish-work! ctx))))))

(defn- send-chunk!
  "Send up to send-buf-size bytes of `item`'s remaining from its position.
  Returns bytes sent (advancing item's position), :eagain, or :error."
  [srv ctx item]
  (let [nb (:send-buf srv) chunk (long (:send-buf-size srv))
        rem (buf/remaining item) n (min rem chunk)
        ^bytes arr (:arr item) pos (buf/position item)
        chunkbytes (java.util.Arrays/copyOfRange arr (int pos) (int (+ pos n)))]
    (jolt.ffi/write-array nb chunkbytes)
    (let [sent (net/send* (:fd ctx) nb n)]
      (if (number? sent)
        (do (buf/set-position! item (+ pos (long sent))) sent)
        sent))))

;; --- reactor-side event handlers -------------------------------------------
(defn- handle-close
  "Request closure and mark CLOSED. Safe to call from a worker or the reactor.

  Deliberately does not call close(2): only handle-pending-close on the reactor
  owns the raw fd. Otherwise POSIX may reuse the number while stale work still
  retains this context, and a later close from that work can kill the replacement
  connection."
  ([ctx ex] (handle-close ctx ex nil))
  ([ctx ex callback]
   (vswap! (:close-ex ctx) (fn [c] (or c ex)))
   (when callback
     (swap! (:close-callback ctx) (fn [c] (or c callback))))
   (set-flag! ctx CLOSED)
   (mark-pending! ctx)
   (wake-server! (:srv ctx))))

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
        (let [n (recv-into! srv ctx)]
          (cond
            (= n :error) (handle-close ctx nil)
            (= n :eof)   (handle-eof! srv ctx)
            (= n 0)      nil
            :else (do (when (>= (buf/position rb) (long (buf/capacity rb))) (set-flag! ctx FULL))
                      (when-not working? (submit-read-handler srv ctx)))))))))

(defn- handle-write [srv ctx]
  (unset-flag! ctx WRITING)
  (loop []
    (when-some [[item callback] (q-peek (:write-queue ctx))]
      (if (= item ::close)
        (do (q-poll! (:write-queue ctx))
            (handle-close ctx nil callback))
        (if (zero? (buf/remaining item))
          (do (q-poll! (:write-queue ctx)) (when callback (submit-callback srv callback)) (recur))
          (let [r (send-chunk! srv ctx item)]
            (cond
              (= r :eagain) (set-flag! ctx WRITING)
              (= r :error)  (handle-close ctx nil)
              :else (do (swap! (:write-limit ctx) + (long r))
                        (if (buf/has-remaining? item)
                          (set-flag! ctx WRITING)
                          (do (q-poll! (:write-queue ctx)) (when callback (submit-callback srv callback)) (recur))))))))))
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
  "True only when ctx is still the registered generation for its raw fd."
  [srv ctx]
  (let [current (get @(:conns srv) (:fd ctx))]
    (and current (= (:generation current) (:generation ctx)))))

(defn- handle-pending-close
  "Finalize one current connection on the reactor: remove its identity before
  close(2), close exactly once, then schedule the public close callbacks."
  [srv ctx]
  (when (and (flag? ctx CLOSED) (not (flag? ctx WORKING))
             (current-context? srv ctx))
    (swap! (:conns srv) dissoc (:fd ctx))
    (net/close! (:fd ctx))
    (when-let [callback @(:close-callback ctx)]
      (submit-callback srv callback))
    (submit srv (fn [] ((:handler (:opts srv)) @(:state ctx) @(:close-ex ctx))))))

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
(defn- new-context [srv fd]
  (let [{:keys [read-buffer-size write-buffer-size write-queue-size control-queue-size]} (:opts srv)
        rb (buf/buffer read-buffer-size)
        generation (swap! (:next-generation srv) inc)]
    (map->Context
     {:srv srv :fd fd :generation generation
      :lock (java.util.concurrent.locks.ReentrantLock.)
      :flags (atom 0) :state (volatile! nil)
      ;; read-view starts empty (position 0, limit 0), like teensyp's
      ;; (.. read-buffer duplicate flip). A capacity-limit here would make
      ;; handle-pending-read's (> rb.position rv.limit) guard never fire when a
      ;; read arrives while the accept arity is still WORKING.
      :read-buffer rb :read-view (buf/flip (buf/duplicate rb))
      :socket-info {:local-address nil :remote-address nil :fd fd}
      :write-queue (atom clojure.lang.PersistentQueue/EMPTY) :write-limit (atom write-buffer-size)
      :write-queue-cap write-queue-size
      :control-queue (atom clojure.lang.PersistentQueue/EMPTY) :control-queue-cap control-queue-size
      :close-ex (volatile! nil)
      :close-callback (atom nil)})))

(defn- do-accept [srv listen-fd]
  (loop []
    (let [fd (net/accept* listen-fd)]
      (when (number? fd)
        (let [ctx (new-context srv fd)]
          (swap! (:conns srv) assoc fd ctx)
          (set-flag! ctx WORKING)
          (submit srv (fn []
                        (try (vreset! (:state ctx) ((:handler (:opts srv)) ctx))
                             (catch :default e (handle-close ctx e))
                             (finally (finish-work! ctx)))))
          (recur))))))

;; --- interest + reactor loop -----------------------------------------------
(defn- interest [ctx]
  (let [f (flags-of ctx)]
    (bit-or (if (zero? (bit-and f READ-MASK)) net/POLLIN 0)
            (if (and (not (zero? (bit-and f WRITING))) (zero? (bit-and f CLOSED))) net/POLLOUT 0))))

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

(defn- retire-wake-w!
  "Close wake-w exactly once under the same gate used by all wake writers.

  wake-open? is cleared before close, so a close failure cannot admit another
  write. The read end remains open until after this returns, preventing SIGPIPE
  for the wake that may already hold the gate."
  [srv]
  (let [close-error (volatile! nil)]
    (with-wake-gate
      srv
      (fn []
        (reset! (:running? srv) false)
        (when (compare-and-set! (:wake-open? srv) true false)
          (try (net/close! (:wake-w srv))
               (catch :default e (vreset! close-error e))))))
    ;; Never call user logging code while holding the ownership gate.
    (when-let [e @close-error]
      (report-error srv e))))

(defn- cleanup-server!
  "Release every resource owned by a fully constructed server, exactly once.

  The reactor is the normal caller (from its outermost `finally`). The guard
  also makes this safe for the narrow failure window between constructing the
  server state and starting its future."
  [srv]
  (when (compare-and-set! (:cleanup-started? srv) false true)
    (try
      ;; Retire the write end before any other cleanup. Every admitted ordinary
      ;; or terminal wake has completed before this close can run, and later
      ;; workers observe wake-open? false under the same gate.
      (retire-wake-w! srv)

      ;; Stop accepting before releasing any reactor storage. stop-server only
      ;; flips running? and wakes poll; the reactor owns all raw descriptors.
      (cleanup-call srv #(net/close! (:listen-fd srv)))

      ;; Remove identities before close(2), just like the ordinary connection
      ;; finalizer, so late worker work cannot act on an fd that POSIX reuses.
      (let [ctxs (vals @(:conns srv))]
        (reset! (:conns srv) {})
        (doseq [ctx ctxs]
          (cleanup-call srv #(net/close! (:fd ctx)))
          (cleanup-call srv #((:handler (:opts srv)) @(:state ctx) nil))))

      (cleanup-call srv #(net/close! (:wake-r srv)))
      (cleanup-call srv #(ffi/free (:recv-buf srv)))
      (cleanup-call srv #(ffi/free (:send-buf srv)))

      ;; Pools created by the server are always reaped. Caller-supplied pools
      ;; are borrowed unless their corresponding explicit shutdown option opted
      ;; them into server ownership.
      (let [shutdown-ex? (:shutdown-executor? srv)
            shutdown-cb? (:shutdown-callback-executor? srv)]
        (when shutdown-ex?
          (cleanup-call srv #(.shutdown (:executor srv))))
        (when (and shutdown-cb?
                   (not (and shutdown-ex?
                             (identical? (:executor srv)
                                         (:callback-executor srv)))))
          (cleanup-call srv #(.shutdown (:callback-executor srv)))))
      (finally
        ;; Completion means descriptors, native buffers, and executor shutdown
        ;; requests have all been handled. Always release waiters, even if a
        ;; host cleanup primitive itself failed.
        (deliver (:stopped srv) :stopped)))))

(defn- cleanup-startup!
  "Release resources acquired before a complete server can be handed to the
  reactor. Caller-supplied executors are borrowed by default; an explicit
  shutdown opt-in authorizes cleanup here as well as after a successful start."
  [state]
  (when (compare-and-set! (:cleanup-started? state) false true)
    (doseq [fd (remove nil? [(:listen-fd state) (:wake-r state) (:wake-w state)])]
      (try (net/close! fd) (catch :default _ nil)))
    (doseq [ptr (remove nil? [(:recv-buf state) (:send-buf state)])]
      (try (ffi/free ptr) (catch :default _ nil)))
    (when (:shutdown-executor? state)
      (try (.shutdown (:executor state)) (catch :default _ nil)))
    (when (and (:shutdown-callback-executor? state)
               (not (and (:shutdown-executor? state)
                         (identical? (:executor state)
                                     (:callback-executor state)))))
      (try (.shutdown (:callback-executor state)) (catch :default _ nil)))))

(defn- run-after-reactor-start!
  "Run `f` only after construction commits the server handoff.

  A constructor failure after future-call returns delivers :abort instead. The
  catch path then owns cleanup, and the scheduled future must not touch the
  descriptors or native buffers it just released."
  [reactor-start f]
  (when (= :start (deref reactor-start))
    (f)))

(defn- handle-conn-events
  "Run one connection's poll events, containing any failure to that connection.

  A throw here used to propagate out of the reactor loop and silently kill the
  whole server: the future swallowed it, `running?` stayed true, and every
  connection — existing and new — stopped being served with no log. One bad
  connection must only ever cost that connection."
  [srv ctx re]
  (try
    (when-not (flag? ctx CLOSED)
      (when (pos? (bit-and re (bit-or net/POLLIN net/POLLHUP net/POLLERR)))
        (handle-read srv ctx))
      (when (and (not (flag? ctx CLOSED))
                 (pos? (bit-and re net/POLLOUT)))
        (handle-write srv ctx)))
    (catch :default e
      (report-error srv e)
      (try (handle-close ctx e) (catch :default _ nil)))))

(defn- reactor-loop [srv]
  (let [listen-fd (:listen-fd srv)
        wake-r    (:wake-r srv)
        running?  (:running? srv)]
    (try
      (loop []
        (when @running?
          (let [ctxs (vec (remove #(flag? % CLOSED) (vals @(:conns srv))))
                n    (+ 2 (count ctxs))
                arr  (net/alloc-pollfds n)]
            (try
              (net/set-pollfd! arr 0 listen-fd net/POLLIN)
              (net/set-pollfd! arr 1 wake-r    net/POLLIN)
              (dotimes [i (count ctxs)]
                (net/set-pollfd! arr (+ i 2) (:fd (nth ctxs i)) (interest (nth ctxs i))))
              ;; The self-pipe (wake-r) makes poll return promptly on any state
              ;; change; the 1s timeout is a defensive safety net against a missed
              ;; wake, not the primary mechanism.
              (when (pos? (net/poll* arr n 1000))
                (when (pos? (bit-and (net/pollfd-revents arr 0) net/POLLIN))
                  (try (do-accept srv listen-fd)
                       (catch :default e (report-error srv e))))
                (when (pos? (bit-and (net/pollfd-revents arr 1) net/POLLIN))
                  (net/drain! wake-r))
                (dotimes [i (count ctxs)]
                  (let [ctx (nth ctxs i) re (net/pollfd-revents arr (+ i 2))]
                    (when (pos? re)
                      (handle-conn-events srv ctx re)))))
              ;; Last-resort guard: whatever else goes wrong in an iteration, the
              ;; reactor keeps running rather than dying silently.
              (catch :default e (report-error srv e))
              (finally (ffi/free arr)))
            (doseq [[fd generation :as key] (drain-pending! (:pending srv))]
              (when-let [ctx (get @(:conns srv) fd)]
                ;; A stale worker/control event may arrive after POSIX has reused
                ;; its fd. Drop it unless it names the currently registered
                ;; generation; fd alone is never a connection identity.
                (when (= generation (:generation ctx))
                  (try
                    (when-not (handle-pending srv ctx)
                      (swap! (:pending srv) conj key))
                    (catch :default e
                      (report-error srv e)
                      (try (handle-close ctx e) (catch :default _ nil)))))))
            (recur))))
      (catch :default e
        ;; Errors outside an individual iteration are still visible, and the
        ;; outer finally remains the non-negotiable lifecycle boundary.
        (report-error srv e))
      (finally
        (cleanup-server! srv)))))

;; --- public entry point ----------------------------------------------------
(defn run-server
  "Start a TCP server. Options (map or kwargs):
    :port (required)             :handler (required, 3-arity fn)
    :read-buffer-size   8192     :write-buffer-size 32768
    :write-queue-size   64       :control-queue-size 32
    :reuse-address?     false    :recv-buffer-size  (SO_RCVBUF)
    :executor (custom pool)      :pool-size 4  (default fixed-pool workers;
                                                jolt's availableProcessors is 1)
    :error-logger  (fn [ex])     called on a reactor-side error; the affected
                                 connection is closed but the server keeps
                                 running (defaults to printing to *err*)
    :callback-executor           runs write/control completion callbacks; kept
                                 separate from :executor so a handler blocked on
                                 its own write cannot starve the callback that
                                 releases it (defaults to a dedicated pool)
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
                control-queue-size reuse-address? recv-buffer-size executor pool-size
                error-logger callback-executor
                shutdown-executor? shutdown-callback-executor?
                stop-timeout-ms]
         :or   {read-buffer-size 8192 write-buffer-size 32768 write-queue-size 64
                control-queue-size 32 pool-size 4
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
    (let [startup (atom {:cleanup-started? (atom false)})]
      (try
        (let [listen-fd (net/listen-socket port {:reuse-address? reuse-address?
                                                 :recv-buffer-size recv-buffer-size})]
          (swap! startup assoc :listen-fd listen-fd)
          (let [[wake-r wake-w] (net/make-pipe)]
            (swap! startup assoc :wake-r wake-r :wake-w wake-w)
            (let [recv-buf (ffi/alloc read-buffer-size)]
              (swap! startup assoc :recv-buf recv-buf)
              (let [send-buf (ffi/alloc 65536)]
                (swap! startup assoc :send-buf send-buf)
                (let [owns-executor? (nil? executor)
                      ex (or executor
                             (java.util.concurrent.Executors/newFixedThreadPool pool-size))
                      shutdown-executor-on-cleanup?
                      (or owns-executor? (true? shutdown-executor?))]
                  (swap! startup assoc
                         :executor ex
                         :owns-executor? owns-executor?
                         :shutdown-executor? shutdown-executor-on-cleanup?)
                  (let [owns-callback-executor? (nil? callback-executor)
                        ;; Separate from the handler pool on purpose — see
                        ;; submit-callback.
                        cb-ex (or callback-executor
                                  (java.util.concurrent.Executors/newCachedThreadPool))
                        shutdown-callback-on-cleanup?
                        (or owns-callback-executor?
                            (true? shutdown-callback-executor?))]
                    (swap! startup assoc
                           :callback-executor cb-ex
                           :owns-callback-executor? owns-callback-executor?
                           :shutdown-callback-executor?
                           shutdown-callback-on-cleanup?)
                    (let [running? (atom true)
                          stopped (promise)
                          ;; A newly scheduled future may begin before
                          ;; future-call returns to this thread. Keep it outside
                          ;; reactor-loop until the handle is complete.
                          reactor-start (promise)
                          srv {:conns (atom {}) :pending (atom #{})
                               :next-generation (atom 0)
                               :executor ex :callback-executor cb-ex
                               :owns-executor? owns-executor?
                               :owns-callback-executor? owns-callback-executor?
                               :shutdown-executor?
                               shutdown-executor-on-cleanup?
                               :shutdown-callback-executor?
                               shutdown-callback-on-cleanup?
                               :listen-fd listen-fd :wake-r wake-r :wake-w wake-w
                               :wake-gate (atom false)
                               :wake-open? (atom true)
                               :recv-buf recv-buf
                               :send-buf send-buf :send-buf-size 65536
                               :running? running? :stopped stopped
                               :cleanup-started? (atom false)
                               :stop-timeout-ms (long stop-timeout-ms)
                               :opts {:handler handler :read-buffer-size read-buffer-size
                                      :write-buffer-size write-buffer-size
                                      :write-queue-size write-queue-size
                                      :control-queue-size control-queue-size
                                      :error-logger error-logger}}]
                      (swap! startup assoc
                             :srv srv
                             :reactor-start reactor-start)
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
                                   :port port})))
                              nil)
                            reactor-future
                            (future
                              (run-after-reactor-start!
                               reactor-start
                               #(reactor-loop srv)))
                            handle {:teensyp/server true :srv srv
                                    :listen-fd listen-fd :port port
                                    :stop stop-fn :close stop-fn
                                    :running? running? :stopped stopped
                                    :reactor-future reactor-future}]
                        (swap! startup assoc :reactor-future reactor-future)
                        (deliver reactor-start :start)
                        handle))))))))
        (catch :default e
          (if-let [srv (:srv @startup)]
            (do
              (request-stop! srv)
              ;; If future construction succeeded but a later constructor step
              ;; failed, release its start gate after revoking wake admission.
              ;; cleanup-server!'s guard makes the catch/reactor race harmless.
              (deliver (:reactor-start @startup) :abort)
              (cleanup-server! srv))
            (cleanup-startup! @startup))
          (throw e))))))

(defn stop-server
  "Stop a server started by [[run-server]]: stop accepting, close the listen
  socket, run each open connection's close-arity, and wait up to
  `:stop-timeout-ms` for reactor-owned descriptors and native buffers to be
  released. Repeated calls are safe and wait on the same completion."
  [server] ((:stop server)) nil)
