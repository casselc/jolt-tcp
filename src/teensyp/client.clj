(ns teensyp.client
  "Blocking outbound TCP connections built exclusively on `jolt.net`.

  `connect` resolves once, then advances resolver candidates under one absolute
  monotonic deadline.  Every failed candidate is closed before the next begins.
  A successful connection owns two persistent pollers, one per direction, so
  blocking reads and writes can proceed concurrently without allocating a
  poller per operation.

  Operations in the same direction are FIFO-serialized by an owner-independent
  gate.  One reader and one writer may run concurrently.  `close!` may run from
  any thread; it closes both pollers to wake blocked operations and then closes
  the socket.  The returned connection is an opaque capability map: public
  inspection exposes endpoints and lifecycle state, never jolt.net sockets or
  native descriptors."
  (:require [jolt.net :as net]))

(def ^:const default-connect-timeout-ms 30000)

(def ^:private nanos-per-ms 1000000)
(def ^:private wait-quantum-ms 1000)
(def ^:private connection-marker ::connection)
(def ^:private close-fn-key ::close-fn)
(def ^:private closed-fn-key ::closed-fn)
(def ^:private info-fn-key ::info-fn)
(def ^:private send-fn-key ::send-fn)
(def ^:private receive-fn-key ::receive-fn)
(def ^:private shutdown-write-fn-key ::shutdown-write-fn)
(def ^:private gate-timeout ::gate-timeout)

(defn- invalid-ex
  [op message data]
  (ex-info (str "teensyp.client " (name op) ": " message)
           (merge {:err ::invalid
                   :teensyp.client/op op
                   :teensyp.client/kind :invalid}
                  data)))

(defn- closed-ex [op]
  (ex-info (str "teensyp.client " (name op) ": connection is closed")
           {:err ::closed
            :teensyp.client/op op
            :teensyp.client/kind :closed}))

(defn- write-shutdown-ex []
  (ex-info "teensyp.client send: the write side is shut down"
           {:err ::write-shutdown
            :teensyp.client/op :send
            :teensyp.client/kind :write-shutdown}))

(defn- timeout-ex
  [endpoint deadline last-ex]
  (ex-info "teensyp.client connect: monotonic deadline expired"
           {:err ::connect-timeout
            :teensyp.client/op :connect
            :teensyp.client/kind :timed-out
            :teensyp.client/deadline-nanos deadline
            :teensyp.client/endpoint endpoint}
           last-ex))

(defn- operation-timeout-ex
  [op deadline]
  (ex-info (str "teensyp.client " (name op)
                ": monotonic deadline expired")
           {:err ::timed-out
            :teensyp.client/op op
            :teensyp.client/kind :timed-out
            :teensyp.client/deadline-nanos deadline}))

(defn- portable-endpoint [ep]
  {:host (:jolt.net/host ep)
   :port (:jolt.net/port ep)
   :family (:jolt.net/family ep)})

(defn- check-slice! [op bytes off len]
  (when-not (bytes? bytes)
    (throw (invalid-ex op "expected a byte-array"
                       {:teensyp.client/value bytes})))
  (let [capacity (alength bytes)]
    (when (or (not (integer? off))
              (not (integer? len))
              (neg? off)
              (neg? len)
              (> (+ off len) capacity))
      (throw (invalid-ex op "byte-array slice is out of bounds"
                         {:teensyp.client/offset off
                          :teensyp.client/length len
                          :teensyp.client/capacity capacity})))))

(defn- remaining-ms
  "Ceiling conversion keeps a live sub-millisecond deadline from becoming a
  zero-timeout busy loop."
  [deadline now]
  (let [remaining (- deadline now)]
    (if (pos? remaining)
      (quot (+ remaining (dec nanos-per-ms)) nanos-per-ms)
      0)))

(defn- monotonic-now []
  (jolt.host/mono-nanos))

(defn- deadline-expired? [deadline now-fn]
  (and deadline (>= (now-fn) deadline)))

(defn- operation-deadline
  [op opts now]
  (when-not (map? opts)
    (throw (invalid-ex op "options must be a map"
                       {:teensyp.client/options opts})))
  (let [absolute? (contains? opts :deadline-nanos)
        relative? (contains? opts :timeout-ms)]
    (when (and absolute? relative?)
      (throw (invalid-ex
               op
               "choose either :deadline-nanos or :timeout-ms"
               nil)))
    (cond
      absolute?
      (let [deadline (:deadline-nanos opts)]
        (when-not (integer? deadline)
          (throw (invalid-ex op
                             ":deadline-nanos must be an integer"
                             {:teensyp.client/deadline-nanos deadline})))
        deadline)

      relative?
      (let [timeout (:timeout-ms opts)]
        (when-not (and (integer? timeout) (not (neg? timeout)))
          (throw (invalid-ex op
                             ":timeout-ms must be a non-negative integer"
                             {:teensyp.client/timeout-ms timeout})))
        (+ now (* timeout nanos-per-ms)))

      :else nil)))

;; A promise queue rather than ReentrantLock is intentional. Jolt logical tasks
;; may share host-thread identity, so a thread-owned reentrant lock is not a
;; reliable ownership boundary between them. The gate is FIFO and blocks
;; contenders without spinning while an I/O operation is parked in poll(2).
;;
;; A timed-out ticket is marked cancelled and left in the persistent queue until
;; the current owner releases. Release skips cancelled tickets atomically. This
;; makes operation deadlines include same-direction queue time without letting a
;; timed-out waiter steal ownership or wedge the next waiter.
(defn- operation-gate []
  (atom {:held? false
         :waiters clojure.lang.PersistentQueue/EMPTY}))

(defn- acquire-gate!
  ([gate]
   (acquire-gate! gate nil monotonic-now))
  ([gate deadline now-fn]
   (if (deadline-expired? deadline now-fn)
     false
     (let [ticket {:signal (promise)
                   :state (atom :waiting)}
           queued?
           (loop []
             (let [old @gate]
               (if (:held? old)
                 (if (compare-and-set! gate old
                                       (update old :waiters conj ticket))
                   true
                   (recur))
                 (if (compare-and-set! gate old (assoc old :held? true))
                   false
                   (recur)))))]
       (if-not queued?
         true
         (if-not deadline
           (do @(:signal ticket) true)
           (loop []
             (let [now (now-fn)]
               (if (>= now deadline)
                 (if (compare-and-set! (:state ticket) :waiting :cancelled)
                   false
                   ;; Release granted this ticket concurrently. Wait for its
                   ;; signal before returning ownership to the caller.
                   (do @(:signal ticket) true))
                 (let [result
                       (deref (:signal ticket)
                              (remaining-ms deadline now)
                              gate-timeout)]
                   (if (= gate-timeout result)
                     (recur)
                     true)))))))))))

(defn- release-gate! [gate]
  (loop []
    (let [old @gate
          waiters (:waiters old)]
      (when-not (:held? old)
        (throw (invalid-ex :gate "operation gate released while unheld" nil)))
      (if (seq waiters)
        (let [ticket (peek waiters)
              next-state (assoc old :waiters (pop waiters))]
          (if (compare-and-set! gate old next-state)
            (if (compare-and-set! (:state ticket) :waiting :granted)
              (deliver (:signal ticket) true)
              ;; A deadline cancelled the ticket before ownership transfer.
              ;; The gate remains held while we advance to the next waiter.
              (recur))
            (recur)))
        (when-not (compare-and-set! gate old (assoc old :held? false))
          (recur)))))
  nil)

(defn- with-gate
  ([gate f]
   (with-gate gate nil monotonic-now f))
  ([gate deadline now-fn f]
   (if-not (acquire-gate! gate deadline now-fn)
     gate-timeout
     (try
       (if (deadline-expired? deadline now-fn)
         gate-timeout
         (f))
       (finally (release-gate! gate))))))

(defn- real-connector-ops []
  {:now monotonic-now
   :resolve net/resolve
   :open-poller net/open-poller
   :try-connect net/try-connect
   :register net/register!
   :remove net/remove-registration!
   :await net/await-ready
   :finish net/finish-connect!
   :close net/close!})

(defn- safe-call! [f & args]
  (try
    (apply f args)
    (catch :default _ nil)))

(defn- retire-attempt!
  [ops poller token socket]
  ;; Removal first makes ownership transfer explicit. Socket close is still
  ;; sufficient if the token has already become stale or the poller is closing.
  (when token
    (safe-call! (:remove ops) poller token))
  (when socket
    (safe-call! (:close ops) socket))
  nil)

(defn- candidate-connect-error?
  "Only `finish-connect!` connect failures justify advancing after readiness.
  Poller/lifecycle errors describe connector infrastructure and fail fast."
  [exception]
  (= :connect (:jolt.net/op (ex-data exception))))

(defn- dial-candidates!
  "Deterministic connector state machine. `ops` is injectable so deadline,
  candidate advancement, and cleanup behavior can be tested without relying on
  kernel timing. Returns ownership of socket, poller, and write token only on
  success; on every throw it closes the poller and any current socket."
  [endpoint addresses socket-opts deadline ops]
  (let [poller ((:open-poller ops))
        poller-transferred? (atom false)]
    (try
      (let [result
            (loop [[address & more] addresses
                   last-ex nil]
              (when (deadline-expired? deadline (:now ops))
                (throw (timeout-ex endpoint deadline last-ex)))
              (let [attempt-result
                    (try
                      {:attempt ((:try-connect ops) address socket-opts)}
                      (catch :default e {:error e}))]
                (if-let [e (:error attempt-result)]
                  (if (deadline-expired? deadline (:now ops))
                    (throw (timeout-ex endpoint deadline e))
                    ;; Mirror jolt.net/try-connect's candidate-vector contract:
                    ;; every synchronous attempt failure is local to the
                    ;; address/options combination and its socket is already
                    ;; rolled back by jolt.net.
                    (if (seq more)
                      (recur more e)
                      (throw e)))
                  (let [attempt (:attempt attempt-result)
                        socket (:jolt.net/socket attempt)
                        status (:jolt.net/status attempt)]
                    (if (deadline-expired? deadline (:now ops))
                      (do
                        (retire-attempt! ops poller nil socket)
                        (throw (timeout-ex endpoint deadline last-ex)))
                      (let [token-result
                            (try
                              {:token ((:register ops) poller socket #{:write})}
                              (catch :default e {:error e}))]
                        (if-let [e (:error token-result)]
                          (do
                            (retire-attempt! ops poller nil socket)
                            (throw e))
                          (let [token (:token token-result)]
                            (cond
                              (= net/connected status)
                              (if (deadline-expired? deadline (:now ops))
                                (do
                                  (retire-attempt! ops poller token socket)
                                  (throw (timeout-ex endpoint deadline last-ex)))
                                {:socket socket
                                 :write-poller poller
                                 :write-token token})

                              (= net/in-progress status)
                              (let [completion
                                    (try
                                      (loop []
                                        (let [now ((:now ops))]
                                          (when (and deadline
                                                     (>= now deadline))
                                            (throw (timeout-ex endpoint deadline
                                                               last-ex)))
                                          (let [ready ((:await ops) poller
                                                       (if deadline
                                                         (remaining-ms deadline now)
                                                         wait-quantum-ms))]
                                            (if (empty? ready)
                                              (recur)
                                              (let [finished
                                                    ((:finish ops) socket)]
                                                (if (= net/in-progress finished)
                                                  (recur)
                                                  finished))))))
                                      (catch :default e {:error e}))]
                                (if (and (map? completion)
                                         (:error completion))
                                  (let [e (:error completion)]
                                    (retire-attempt! ops poller token socket)
                                    (if (= ::connect-timeout
                                           (:err (ex-data e)))
                                      (throw e)
                                      (if (deadline-expired? deadline (:now ops))
                                        (throw (timeout-ex endpoint deadline e))
                                        (if (and (seq more)
                                                 (candidate-connect-error? e))
                                          (recur more e)
                                          (throw e)))))
                                  (cond
                                    (not= net/connected completion)
                                    (do
                                      (retire-attempt! ops poller token socket)
                                      (throw
                                        (invalid-ex
                                          :connect
                                          "jolt.net returned an unknown completion status"
                                          {:teensyp.client/status completion})))

                                    (deadline-expired? deadline (:now ops))
                                    (do
                                      (retire-attempt! ops poller token socket)
                                      (throw (timeout-ex endpoint deadline
                                                         last-ex)))

                                    :else
                                    {:socket socket
                                     :write-poller poller
                                     :write-token token})))

                              :else
                              (do
                                (retire-attempt! ops poller token socket)
                                (throw
                                  (invalid-ex
                                    :connect
                                    "jolt.net returned an unknown connect status"
                                    {:teensyp.client/status status}))))))))))))]
        (reset! poller-transferred? true)
        result)
      (finally
        (when-not @poller-transferred?
          (safe-call! (:close ops) poller))))))

(defn- establish!
  "Resolve exactly once and establish a non-blocking connection under deadline.
  Kept as a source-level seam for deterministic semantic tests."
  [endpoint socket-opts deadline ops]
  (when (deadline-expired? deadline (:now ops))
    (throw (timeout-ex endpoint deadline nil)))
  (let [resolution
        (try
          {:addresses (vec ((:resolve ops) endpoint socket-opts))}
          (catch :default e {:error e}))]
    (if-let [e (:error resolution)]
      (if (deadline-expired? deadline (:now ops))
        (throw (timeout-ex endpoint deadline e))
        (throw e))
      (let [addresses (:addresses resolution)]
        (when (deadline-expired? deadline (:now ops))
          (throw (timeout-ex endpoint deadline nil)))
        (when (empty? addresses)
          (throw (invalid-ex :connect "endpoint resolved to no addresses"
                             {:teensyp.client/endpoint endpoint})))
        (dial-candidates! endpoint addresses socket-opts deadline ops)))))

(defn- await-operation!
  "Wait once. Returns false only when the deadline expires without a readiness
  event. A readiness event wins the race with the deadline so the caller gets
  one non-blocking probe that can preserve EOF, reset, or close."
  [deadline now-fn await-fn]
  (if-not deadline
    (do (await-fn wait-quantum-ms) true)
    (let [now (now-fn)]
      (if (>= now deadline)
        false
        (let [timeout-ms (min wait-quantum-ms
                              (remaining-ms deadline now))
              ready (await-fn timeout-ms)]
          (or (boolean (seq ready))
              (< (now-fn) deadline)))))))

(defn- send-all-loop!
  ([try-write! wait! src off len]
   (send-all-loop! try-write! wait! (constantly false) (fn [] nil)
                   src off len))
  ([try-write! wait! expired? timeout! src off len]
   (loop [offset off
          remaining len
          readiness-probe? false]
     (if (zero? remaining)
       nil
       (if (and (not readiness-probe?)
                (expired?))
         (timeout!)
         (let [n (try-write! src offset remaining)]
           (cond
             (net/would-block? n)
             (if (wait!)
               ;; Even if the deadline elapsed while readiness was reported,
               ;; probe once so a native reset/close is not rewritten as timeout.
               (recur offset remaining true)
               (timeout!))

             (and (integer? n) (pos? n) (<= n remaining))
             (recur (+ offset n) (- remaining n) false)

             :else
             (throw (invalid-ex
                      :send
                      "jolt.net returned an invalid non-blocking write result"
                      {:teensyp.client/result n
                       :teensyp.client/remaining remaining})))))))))

(defn- receive-loop!
  ([try-read! wait! dest off len]
   (receive-loop! try-read! wait! (constantly false) (fn [] nil)
                  dest off len))
  ([try-read! wait! expired? timeout! dest off len]
   (if (zero? len)
     0
     (loop [readiness-probe? false]
       (if (and (not readiness-probe?)
                (expired?))
         (timeout!)
         (let [n (try-read! dest off len)]
           (cond
             (net/would-block? n)
             (if (wait!)
               (recur true)
               (timeout!))

             (net/eof? n) nil
             (and (integer? n) (pos? n) (<= n len)) n
             :else
             (throw (invalid-ex
                      :receive
                      "jolt.net returned an invalid non-blocking read result"
                      {:teensyp.client/result n
                       :teensyp.client/length len})))))))))

(defn- ensure-open! [lifecycle op]
  (when-not (= :open (:phase @lifecycle))
    (throw (closed-ex op))))

(defn- begin-close! [lifecycle]
  (loop []
    (let [old @lifecycle]
      (if-not (= :open (:phase old))
        false
        (if (compare-and-set! lifecycle old (assoc old :phase :closing))
          true
          (recur))))))

(defn- close-resources!
  [lifecycle read-poller write-poller socket]
  (if-not (begin-close! lifecycle)
    false
    (let [first-error (atom nil)
          close-one!
          (fn [owned]
            (try
              (net/close! owned)
              (catch :default e
                (compare-and-set! first-error nil e))))]
      (try
        ;; Pollers first: blocked readers/writers wake before socket retirement.
        (close-one! read-poller)
        (close-one! write-poller)
        (close-one! socket)
        (finally
          (swap! lifecycle assoc :phase :closed)))
      (if-let [e @first-error]
        (throw e)
        true))))

(defn- build-connection!
  [{:keys [socket write-poller]}]
  (let [read-poller (atom nil)]
    (try
      (reset! read-poller (net/open-poller))
      (net/register! @read-poller socket #{:read})
      (let [local-address (portable-endpoint (net/local-endpoint socket))
            remote-address (portable-endpoint (net/peer-endpoint socket))
            lifecycle (atom {:phase :open
                             :read-eof? false
                             :write-shutdown? false})
            read-gate (operation-gate)
            write-gate (operation-gate)
            close-fn #(close-resources! lifecycle @read-poller write-poller socket)
            send-timeout!
            (fn [deadline]
              ;; Terminal lifecycle state wins a deadline race.
              (ensure-open! lifecycle :send)
              (when (:write-shutdown? @lifecycle)
                (throw (write-shutdown-ex)))
              (throw (operation-timeout-ex :send deadline)))
            receive-timeout!
            (fn [deadline]
              ;; EOF/close are transport outcomes, never timeout translations.
              (ensure-open! lifecycle :receive)
              (if (:read-eof? @lifecycle)
                nil
                (throw (operation-timeout-ex :receive deadline))))
            send-fn
            (fn [src off len deadline]
              (check-slice! :send src off len)
              ;; Check terminal state before deadline acquisition so a caller
              ;; never sees a closed or half-closed connection as a timeout.
              (ensure-open! lifecycle :send)
              (when (:write-shutdown? @lifecycle)
                (throw (write-shutdown-ex)))
              (if (zero? len)
                nil
                (let [result
                      (with-gate
                        write-gate deadline monotonic-now
                        (fn []
                          (ensure-open! lifecycle :send)
                          (when (:write-shutdown? @lifecycle)
                            (throw (write-shutdown-ex)))
                          (send-all-loop!
                            #(net/try-write-bytes! socket %1 %2 %3)
                            #(await-operation!
                               deadline monotonic-now
                               (fn [timeout-ms]
                                 (net/await-ready write-poller timeout-ms)))
                            #(deadline-expired? deadline monotonic-now)
                            #(send-timeout! deadline)
                            src off len)))]
                  (if (= gate-timeout result)
                    (send-timeout! deadline)
                    result))))
            receive-fn
            (fn [dest off len deadline]
              (check-slice! :receive dest off len)
              (ensure-open! lifecycle :receive)
              (cond
                (zero? len) 0
                (:read-eof? @lifecycle) nil
                :else
                (let [result
                      (with-gate
                        read-gate deadline monotonic-now
                        (fn []
                          (ensure-open! lifecycle :receive)
                          (cond
                            (zero? len) 0
                            (:read-eof? @lifecycle) nil
                            :else
                            (let [read-result
                                  (receive-loop!
                                    #(net/try-read-bytes! socket %1 %2 %3)
                                    #(await-operation!
                                       deadline monotonic-now
                                       (fn [timeout-ms]
                                         (net/await-ready @read-poller
                                                          timeout-ms)))
                                    #(deadline-expired? deadline monotonic-now)
                                    #(receive-timeout! deadline)
                                    dest off len)]
                              (when (nil? read-result)
                                (swap! lifecycle
                                       #(if (= :open (:phase %))
                                          (assoc % :read-eof? true)
                                          %)))
                              read-result))))]
                  (if (= gate-timeout result)
                    (receive-timeout! deadline)
                    result))))
            shutdown-write-fn
            (fn []
              (with-gate
                write-gate
                (fn []
                  (ensure-open! lifecycle :shutdown-write)
                  (if (:write-shutdown? @lifecycle)
                    false
                    (do
                      (net/shutdown! socket :write)
                      (swap! lifecycle
                             #(if (= :open (:phase %))
                                (assoc % :write-shutdown? true)
                                %))
                      true)))))
            info-fn
            (fn []
              (let [state @lifecycle]
                {:local-address local-address
                 :remote-address remote-address
                 :state (:phase state)
                 :closed? (not= :open (:phase state))
                 :read-eof? (:read-eof? state)
                 :write-shutdown? (:write-shutdown? state)}))]
        {connection-marker true
         :local-address local-address
         :remote-address remote-address
         close-fn-key close-fn
         closed-fn-key #(not= :open (:phase @lifecycle))
         info-fn-key info-fn
         send-fn-key send-fn
         receive-fn-key receive-fn
         shutdown-write-fn-key shutdown-write-fn
         ;; jolt's with-open recognizes a map's unqualified zero-arity :close.
         :close (fn [] (close-fn) nil)})
      (catch :default e
        (when @read-poller
          (safe-call! net/close! @read-poller))
        (safe-call! net/close! write-poller)
        (safe-call! net/close! socket)
        (throw e)))))

(defn- deadline-for [opts now]
  (let [absolute? (contains? opts :deadline-nanos)
        relative? (contains? opts :connect-timeout-ms)]
    (when (and absolute? relative?)
      (throw (invalid-ex
               :connect
               "choose either :deadline-nanos or :connect-timeout-ms"
               nil)))
    (if absolute?
      (let [deadline (:deadline-nanos opts)]
        (when-not (integer? deadline)
          (throw (invalid-ex :connect
                             ":deadline-nanos must be an integer"
                             {:teensyp.client/deadline-nanos deadline})))
        deadline)
      (let [timeout (if relative?
                      (:connect-timeout-ms opts)
                      default-connect-timeout-ms)]
        (cond
          (and relative? (nil? timeout))
          nil

          (and (integer? timeout) (not (neg? timeout)))
          (+ now (* timeout nanos-per-ms))

          :else
          (throw (invalid-ex
                   :connect
                   ":connect-timeout-ms must be nil or a non-negative integer"
                   {:teensyp.client/connect-timeout-ms timeout})))))))

(defn connect-endpoint
  "Connect to a `jolt.net/endpoint` and return an opaque blocking connection.

  Options accepted by jolt.net (for example `:no-delay?` and
  `:recv-buffer-size`) pass through. Deadline policy is either
  `:connect-timeout-ms` (default 30000), explicit
  `:connect-timeout-ms nil` for an unbounded connect, or an absolute
  `:deadline-nanos` in `jolt.host/mono-nanos` units. A bounded timeout
  covers resolution and every resolver candidate; it is never restarted
  between candidates."
  ([endpoint] (connect-endpoint endpoint {}))
  ([endpoint opts]
   (let [ops (real-connector-ops)
         now ((:now ops))
         deadline (deadline-for opts now)
         socket-opts (dissoc opts :connect-timeout-ms :deadline-nanos)
         established (establish! endpoint socket-opts deadline ops)]
     (build-connection! established))))

(defn connect
  "Connect to host and port. See [[connect-endpoint]] for options."
  ([host port] (connect host port {}))
  ([host port opts]
   (connect-endpoint (net/endpoint host port) opts)))

(defn connection? [x]
  (and (map? x) (true? (get x connection-marker))))

(defn- require-connection! [connection op]
  (when-not (connection? connection)
    (throw (invalid-ex op "expected a teensyp.client connection"
                       {:teensyp.client/value connection})))
  connection)

(defn send-all!
  "Block until every byte in the source slice has been written.

  Options are either relative `:timeout-ms` or absolute `:deadline-nanos`
  (mutually exclusive); omitting both is unbounded. The monotonic deadline
  begins at API entry and includes FIFO same-direction gate queue time. A reader
  may run concurrently."
  ([connection src]
   (send-all! connection src {}))
  ([connection src opts]
   (send-all! connection src 0 (if (bytes? src) (alength src) 0) opts))
  ([connection src off len]
   (send-all! connection src off len {}))
  ([connection src off len opts]
   (require-connection! connection :send)
   (let [deadline (operation-deadline :send opts (monotonic-now))]
     ((get connection send-fn-key) src off len deadline))))

(defn receive-into!
  "Block for one read into dest[off,off+len). Returns a positive count, 0 for a
  zero-length request, or nil at EOF. Same-direction reads are FIFO-serialized;
  a writer may run concurrently.

  Options are either relative `:timeout-ms` or absolute `:deadline-nanos`
  (mutually exclusive); omitting both is unbounded. The monotonic deadline
  begins at API entry and includes FIFO gate queue time."
  ([connection dest off len]
   (receive-into! connection dest off len {}))
  ([connection dest off len opts]
   (require-connection! connection :receive)
   (let [deadline (operation-deadline :receive opts (monotonic-now))]
     ((get connection receive-fn-key) dest off len deadline))))

(defn receive-at-most!
  "Block for and return at most `max-bytes` in a new byte-array, or nil at EOF.
  Accepts the same optional deadline map as [[receive-into!]]."
  ([connection max-bytes]
   (receive-at-most! connection max-bytes {}))
  ([connection max-bytes opts]
   (require-connection! connection :receive)
   ;; Compute before allocation so relative time includes all API-side work.
   (let [deadline (operation-deadline :receive opts (monotonic-now))]
     (when-not (and (integer? max-bytes) (not (neg? max-bytes)))
       (throw (invalid-ex :receive
                          "max-bytes must be a non-negative integer"
                          {:teensyp.client/max-bytes max-bytes})))
     (let [dest (byte-array max-bytes)
           n ((get connection receive-fn-key) dest 0 max-bytes deadline)]
       (cond
         (nil? n) nil
         (= n max-bytes) dest
         :else (java.util.Arrays/copyOfRange dest 0 (int n)))))))

(defn shutdown-write!
  "Half-close the write side while retaining the readable side. Returns true
  for the first successful half-close and false when already half-closed."
  [connection]
  (require-connection! connection :shutdown-write)
  ((get connection shutdown-write-fn-key)))

(defn close!
  "Close both persistent pollers and the socket. Idempotent: returns true only
  for the call that began close."
  [connection]
  (require-connection! connection :close)
  ((get connection close-fn-key)))

(defn closed?
  "True as soon as close begins."
  [connection]
  (require-connection! connection :closed?)
  ((get connection closed-fn-key)))

(defn connection-info
  "Return transport-neutral endpoint and lifecycle data. No socket, poller, or
  native descriptor is exposed."
  [connection]
  (require-connection! connection :connection-info)
  ((get connection info-fn-key)))
