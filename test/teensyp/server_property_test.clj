(ns teensyp.server-property-test
  "Generative properties driven over real loopback TCP.

  The acceptance suite in teensyp.server-test pins one input to one expected
  string, and — critically — one *split point*: `\"par\"` then `\"tial\\n\"`. But
  framing correctness depends on where TCP happens to break a stream, and a
  server is only correct if it holds for every split. These properties let Hegel
  choose the payload and the chunk boundaries.

  Fixture shape (see the jolt-hegel skill's guidance on sharing an expensive
  external service):

    - One server per property, started once around the whole `run-test!` call and
      kept alive until it returns, so generation, shrinking and final replay all
      run against it.
    - A fresh *connection* per case is the isolation unit: per-connection handler
      state, read buffer and write queue are all created on accept, so every case
      starts from equivalent observable server state and shrinking stays sound.
    - The connection is closed in a `finally`, including on failing cases.
    - Cases end on protocol signals, not sleeps: the client half-closes and
      drains to a real EOF, bounded in time. Hitting the bound is a failure, not
      a retry.

  This namespace uses h/run-test! directly and counts failures, matching the
  framework-less style of teensyp.server-test. Failures print the seed; replay
  with (parse-long seed) as :seed."
  (:require [clojure.string :as str]
            [hegel.core :as h]
            [hegel.generator :as g]
            [hegel.report :as report]
            [teensyp.buffer :as buf]
            [teensyp.ffi-net :as net]
            [teensyp.server :as tcp]
            [teensyp.stream :as stream]))

(def ^:private run-opts {:test-cases 40 :database "" :verbosity :quiet})

(defn- nonblank-env [name]
  (let [value (System/getenv name)]
    (when (and value (not (str/blank? value))) value)))

(defn- property-run-opts
  "Apply the optional CI/local replay seed to one Hegel run. Keeping this at
  the run boundary makes a printed seed directly replayable without editing
  source:

    TEENSYP_HEGEL_SEED=1767470842619
    TEENSYP_HEGEL_ONLY=line-framing jolt -M:test"
  [opts]
  (if-let [seed (nonblank-env "TEENSYP_HEGEL_SEED")]
    (assoc opts :seed (parse-long seed))
    opts))

;; Out-of-band failure log. A libhegel-detected nondeterministic run comes back
;; with :failures [] and no counterexample, so the only record of what actually
;; went wrong in the case that did not reproduce is what we noted as it happened.
(def ^:private events (atom []))

(defn- fail! [origin data]
  (swap! events conj [origin data])
  (throw (ex-info (str "property failed: " origin) (assoc data :hegel/origin origin))))

;; jolt-hegel's counting runner. It records pass/fail/error, counts both failed
;; results and thrown run errors, and never exits the process — teensyp.server-test
;; aggregates one failure total and exits at the end, so a single red property
;; must not abort the rest of the suite.
;;
;; The reporter adds what the default cannot know about this fixture: the
;; out-of-band `events` log. A libhegel-detected nondeterministic run comes back
;; as :status :error with :flaky? true and no counterexample at all, and for a
;; networked fixture that usually means the case isolation or the time bound is
;; wrong, not that the server is wrong.
(defn- reporter [{:keys [type description result error] :as ev}]
  (case type
    :pass  (println "ok   " description (str "(" (:valid-test-cases result) " cases)"))
    :fail  (do (println "FAIL " description)
               (println "   seed:    " (:seed result) " (replay with :seed (parse-long ...))")
               (when (:flaky? result)
                 (println "   flaky:    true — fix case isolation/timing before trusting the counterexample"))
               (when (:error result) (println "   error:   " (pr-str (:error result))))
               (println "   failures:" (pr-str (:n-failures result)))
               (println "   detail:  " (pr-str (:failures result)))
               (println "   observed:" (pr-str (frequencies (map first @events))))
               (println "   first:   " (pr-str (first @events))))
    :error (do (println "FAIL " description "(engine/setup error)")
               (println "   " (pr-str error)))
    (println (pr-str ev)))
  (reset! events [])
  (flush))

(def ^:private runner (report/counting-runner {:reporter reporter}))

(defn failure-count [] (report/failure-count runner))

(defn- guarded
  "Run one complete property through the counting runner. Setup, health-check
  and unexpected engine errors are counted rather than thrown, so the remaining
  properties still get to run."
  [label f]
  (report/run! runner label f))

(defn- ->ba
  "Vector of unsigned octets -> byte-array, at the I/O boundary only."
  ^bytes [octets]
  (let [n (count octets) a (byte-array n)]
    (dotimes [i n] (aset a i (unchecked-byte (long (nth octets i)))))
    a))

;; --- client helpers --------------------------------------------------------
(defn- utf8 [^String s] (.getBytes s "UTF-8"))
(defn- ->str [^bytes b] (when b (String. b "UTF-8")))

(defn- recv-until-eof
  "Drain to a real EOF, bounded in time and bytes. Reaching either bound is a
  failure of the property, never a retry."
  [fd max-bytes]
  (let [r (deref (future (loop [acc [] total 0]
                           (if (> total (long max-bytes))
                             :overrun
                             (if-let [b (net/client-recv fd 16384)]
                               (recur (conj acc b) (+ total (alength b)))
                               acc))))
                 8000 :timeout)]
    (cond
      (= r :timeout) :timeout
      (= r :overrun) :overrun
      :else (let [total (reduce + 0 (map alength r))
                  out (buf/buffer total)]
              ;; Concatenate through the public Buffer API; its bulk path uses
              ;; the fork's overlap-safe System/arraycopy primitive.
              (doseq [^bytes b r] (buf/put-bytes! out b 0 (alength b)))
              (:arr out)))))

(defn- exchange
  "One case: connect, send `chunks` in order, half-close, drain to EOF. The
  connection is closed in a finally, including when the property throws."
  ([port chunks] (exchange port chunks 1000000))
  ([port chunks max-bytes]
   (let [fd (net/connect-loopback port)]
     (try
       (doseq [^bytes c chunks] (net/client-send-all fd c))
       (net/shutdown-write! fd)
       (recv-until-eof fd max-bytes)
       (finally (net/close! fd))))))

(defn- check-drain! [origin got data]
  (when (= got :timeout) (fail! (str origin "/timeout") data))
  (when (= got :overrun) (fail! (str origin "/overrun") data)))

;; --- handlers --------------------------------------------------------------
;; Every handler closes once the peer has half-closed, which is what makes each
;; case terminate on its own signal rather than on a sleep.

(defn- echo-handler
  ([_sock] nil)
  ([state sock b]
   (let [n (buf/remaining b)]
     (when (pos? n) (tcp/write sock (buf/wrap (buf/get-bytes! b n))))
     ;; EOF can become observable while this invocation still has a pre-EOF
     ;; buffer view. Close only from the terminal notification, after every
     ;; byte that preceded EOF is present in the current view.
     (when (tcp/peer-eof-notified? sock) (tcp/close sock))
     state))
  ([_state _ex] nil))

(defn- line-reverse-handler
  ([_sock] nil)
  ([state sock b]
   (loop []
     (when-some [line (buf/read-line b "UTF-8")]
       (tcp/write sock (buf/str->buffer (str (str/join (reverse line)) "\n") "UTF-8"))
       (recur)))
   (when (tcp/peer-eof-notified? sock) (tcp/close sock))
   state)
  ([_state _ex] nil))

;; Reads are paused on accept and resumed shortly after, so every case has data
;; sitting in the kernel socket buffer while the reactor is not reading it.
(defn- pause-resume-echo-handler
  ([sock]
   (tcp/pause-reads sock)
   (future (Thread/sleep 60) (tcp/resume-reads sock))
   nil)
  ([state sock b]
   (let [n (buf/remaining b)]
     (when (pos? n) (tcp/write sock (buf/wrap (buf/get-bytes! b n))))
     (when (tcp/peer-eof-notified? sock) (tcp/close sock))
     state))
  ([_state _ex] nil))

;; Per-case plans. Safe as shared atoms because cases run one connection at a
;; time and each is set before connect and read on accept.
(def ^:private write-plan (atom []))
(def ^:private big-payload (atom (byte-array 0)))

(defn- chained-write-handler
  ;; Each write's completion callback queues the next, so the bytes can only
  ;; arrive in order if the callback chain fires in order.
  ([sock]
   (let [msgs @write-plan]
     (letfn [(step [[m & more]]
               (if m
                 (tcp/write sock (buf/str->buffer m "UTF-8") (fn [] (step more)))
                 (tcp/close sock)))]
       (step msgs)))
   nil)
  ([state _ _] state)
  ([_state _ex] nil))

(defn- single-big-write-handler
  ;; One write large enough to force partial sends, so the reactor must retain
  ;; the same mutable Buffer and ride out would-block as the socket buffer fills.
  ([sock]
   (tcp/write sock (buf/wrap @big-payload))
   (tcp/close sock)
   nil)
  ([state _ _] state)
  ([_state _ex] nil))

;; --- server lifecycle ------------------------------------------------------
(defn- with-server
  "Start one server around a whole run-test! call and keep it alive until the
  call returns — shrinking and final replay both need it."
  [opts f]
  (let [port (+ 19200 (rand-int 400))
        srv (tcp/run-server (assoc opts :port port :reuse-address? true))]
    (Thread/sleep 250)                       ; readiness, once — not per case
    (try (f port) (finally (tcp/stop-server srv) (Thread/sleep 150)))))

;; --- 1. echo conservation under arbitrary chunking -------------------------
;; :read-buffer-size 64 is the point: with the 8192 default, a test payload
;; never fills the read buffer, so compact-by!, the FULL flag and
;; handle-pending-read are effectively untested. At 64 bytes every case of more
;; than 64 bytes drives that path repeatedly.
(defn- prop-echo-conservation []
  (with-server {:handler echo-handler :read-buffer-size 64}
    (fn [port]
      (guarded
       "echo conserves bytes under arbitrary chunking (read-buffer-size 64)"
       (fn []
         (h/run-test!
          (property-run-opts (assoc run-opts :name "server/echo-chunking"))
          (fn [_]
            (g/let [payload (g/vector {:min-size 1 :max-size 2048} (g/octet))
                    chunks (g/chunkings payload)]
              (let [got (exchange port (map ->ba chunks))]
                (h/fprn :payload-len (count payload) :chunk-sizes (mapv count chunks))
                (check-drain! "server/echo-chunking" got
                              {:payload-len (count payload)})
                (when-not (= (vec payload) (vec (map #(bit-and (long %) 0xff) got)))
                  (fail! "server/echo-chunking/mismatch"
                         {:payload-len (count payload)
                          :got-len (alength ^bytes got)
                          :chunk-sizes (mapv count chunks)})))))))))))

;; --- 2. line framing is invariant under chunking ---------------------------
;; Generalizes the single hand-picked split in teensyp.server-test.
(defn- prop-line-framing-invariance []
  (with-server {:handler line-reverse-handler :read-buffer-size 128}
    (fn [port]
      (guarded
       "line framing is invariant under chunking"
       (fn []
         (h/run-test!
          (property-run-opts (assoc run-opts :name "server/line-framing"))
          (fn [_]
            (g/let [lines (g/vector {:min-size 1 :max-size 8}
                                    (g/string {:codec :ascii :max-size 24
                                               :exclude-characters "\r\n"}))
                    payload (vec (utf8 (apply str (map #(str % "\n") lines))))
                    chunks (g/chunkings payload)]
              (let [expected (apply str (map #(str (str/join (reverse %)) "\n") lines))
                    got (exchange port (map ->ba chunks))]
                (h/fprn :lines lines :chunk-sizes (mapv count chunks))
                (check-drain! "server/line-framing" got {:lines lines})
                (when-not (= expected (->str got))
                  (fail! "server/line-framing/mismatch"
                         {:lines lines :chunk-sizes (mapv count chunks)
                          :expected expected :got (->str got)})))))))))))

;; --- 3. chained write callbacks preserve order -----------------------------
(defn- prop-write-callback-ordering []
  (with-server {:handler chained-write-handler}
    (fn [port]
      (guarded
       "chained write callbacks deliver in queue order"
       (fn []
         (h/run-test!
          (property-run-opts
           (assoc run-opts :test-cases 25 :name "server/write-ordering"))
          (fn [_]
            (let [msgs (h/draw! (g/vector {:min-size 1 :max-size 10}
                                          (g/string {:codec :ascii :min-size 1 :max-size 40
                                                     :exclude-characters "\r\n"})))]
              (reset! write-plan msgs)
              (let [got (exchange port [])]
                (h/fprn :messages msgs)
                (check-drain! "server/write-ordering" got {:messages msgs})
                (when-not (= (apply str msgs) (->str got))
                  (fail! "server/write-ordering/mismatch"
                         {:messages msgs :got (->str got)})))))))))))

;; --- 4. backpressure: pause/resume loses nothing ---------------------------
;; Covers handle-control's resume path, which compacts the consumed prefix and
;; resubmits the handler. The acceptance suite exercises it with one fixed
;; 8-byte payload; here every case has arbitrary bytes arriving while reads are
;; paused, with a read buffer small enough that FULL and the resume path
;; interact.
(defn- prop-pause-resume-conserves []
  (with-server {:handler pause-resume-echo-handler :read-buffer-size 128}
    (fn [port]
      (guarded
       "paused reads lose nothing once resumed"
       (fn []
         (h/run-test!
          (property-run-opts
           (assoc run-opts :test-cases 25 :name "server/pause-resume"))
          (fn [_]
            (g/let [payload (g/vector {:min-size 1 :max-size 1024} (g/octet))
                    chunks (g/chunkings payload)]
              (let [got (exchange port (map ->ba chunks))]
                (h/fprn :payload-len (count payload) :chunk-sizes (mapv count chunks))
                (check-drain! "server/pause-resume" got {:payload-len (count payload)})
                (when-not (= (vec payload) (vec (map #(bit-and (long %) 0xff) got)))
                  (fail! "server/pause-resume/mismatch"
                         {:payload-len (count payload)
                          :got-len (alength ^bytes got)})))))))))))

;; --- 5. a single large write ------------------------------------------------
;; The jolt.net byte API may make partial progress or report would-block as the
;; socket buffer fills. The server must retain the exact Buffer, resume from its
;; advanced position, and credit write-limit capacity back per send.
(defn- prop-large-single-write []
  (with-server {:handler single-big-write-handler
                :write-buffer-size (* 4 1024 1024)}
    (fn [port]
      (guarded
       "a single oversized write arrives intact"
       (fn []
         (h/run-test!
          (property-run-opts
           (assoc run-opts :test-cases 15 :name "server/large-write"))
          (fn [_]
            (let [n (h/draw! (g/integer 65537 400000))
                  ;; A cheap position-dependent pattern: any duplication,
                  ;; truncation or reordering inside the send loop shows up as a
                  ;; byte mismatch, not just a length mismatch.
                  payload (byte-array n)]
              (dotimes [i n] (aset payload i (unchecked-byte (mod i 251))))
              (reset! big-payload payload)
              (let [got (exchange port [] 1000000)]
                (h/fprn :write-size n)
                (check-drain! "server/large-write" got {:write-size n})
                (when-not (= n (alength ^bytes got))
                  (fail! "server/large-write/length"
                         {:write-size n :got-len (alength ^bytes got)}))
                ;; Compare as unsigned octets on BOTH sides. Byte values can be
                ;; observed with different signed representations, so a raw
                ;; (= (seq a) (seq b)) reports a spurious mismatch at the first
                ;; byte above 127. teensyp.buffer masks with 0xff throughout for
                ;; exactly this reason.
                (let [diff (first (filter (fn [i] (not= (bit-and (long (aget ^bytes payload i)) 0xff)
                                                        (bit-and (long (aget ^bytes got i)) 0xff)))
                                          (range n)))]
                  (when diff
                    (fail! "server/large-write/corrupt"
                           {:write-size n :first-diff diff}))))))))))))

;; --- 6. the stream layer frames lines the same way ------------------------
;; teensyp.stream had no generative coverage at all, and its own conn-read-line
;; is a second, independent line scanner (over a core.async channel of chunks)
;; that must agree with the buffer-level one under arbitrary chunking.
;;
;; This used to be quarantined for a producer/consumer wedge and pre-EOF
;; truncation. The stream now closes its channel on peer-eof-notified? (after
;; every pre-EOF byte is visible), its finally releases a producer parked on
;; >!!, flags no longer recursively acquire the socket lock, and jolt.net token
;; identity plus owned-close retirement replace raw-fd lifecycle management. The
;; property is back in the gate after 30 consecutive 25-case stress runs
;; completed cleanly.
(defn- prop-stream-line-framing []
  (with-server {:handler (stream/stream-handler
                          (fn [conn]
                            (loop []
                              (when-let [line (stream/conn-read-line conn)]
                                (stream/conn-send conn (str "<" line ">\n"))
                                (recur)))))
                :read-buffer-size 128}
    (fn [port]
      (guarded
       "stream conn-read-line frames identically under chunking"
       (fn []
         (h/run-test!
          (property-run-opts
           (assoc run-opts :test-cases 25 :name "server/stream-lines"))
          (fn [_]
            (g/let [lines (g/vector {:min-size 1 :max-size 6}
                                    (g/string {:codec :ascii :min-size 1 :max-size 20
                                               :exclude-characters "\r\n"}))
                    payload (vec (utf8 (apply str (map #(str % "\n") lines))))
                    chunks (g/chunkings payload)]
              (let [expected (apply str (map #(str "<" % ">\n") lines))
                    got (exchange port (map ->ba chunks))]
                (h/fprn :lines lines :chunk-sizes (mapv count chunks))
                (check-drain! "server/stream-lines" got {:lines lines})
                (when-not (= expected (->str got))
                  (fail! "server/stream-lines/mismatch"
                         {:lines lines :chunk-sizes (mapv count chunks)
                          :expected expected :got (->str got)})))))))))))

(defn run-stream-property!
  "Run the stream-layer property by itself for focused stress/replay."
  []
  (println "\n-- teensyp.stream property --")
  (prop-stream-line-framing)
  (failure-count))

(defn run-properties!
  "Run the TCP properties. Returns the number of failed properties."
  []
  (println "\n-- teensyp.server generative properties (jolt-hegel) --")
  (case (nonblank-env "TEENSYP_HEGEL_ONLY")
    nil (do
          (prop-echo-conservation)
          (prop-line-framing-invariance)
          (prop-write-callback-ordering)
          (prop-pause-resume-conserves)
          (prop-large-single-write)
          (prop-stream-line-framing))
    "echo-chunking" (prop-echo-conservation)
    "line-framing" (prop-line-framing-invariance)
    "write-ordering" (prop-write-callback-ordering)
    "pause-resume" (prop-pause-resume-conserves)
    "large-write" (prop-large-single-write)
    "stream-lines" (prop-stream-line-framing)
    (throw (ex-info "Unknown TEENSYP_HEGEL_ONLY property"
                    {:value (nonblank-env "TEENSYP_HEGEL_ONLY")})))
  (failure-count))
