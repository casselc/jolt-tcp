(ns teensyp.rearm-latency-test
  "Real loopback regression for the reactor re-arm latency (task W6A.1).

  The defect was in jolt.net, not here. `await-ready` chose its own entry as the
  stale/fresh boundary for wake epochs, so a publication landing between the
  reactor's `process-pending!` drain and its `await-ready` call was consumed as
  though the reactor had already seen it. The reactor then parked until
  jolt.net's 1000 ms native safety tick. Bytes were never lost -- the tick
  eventually delivered the same pending generation -- so the only symptom was
  latency, and it arrived in whole multiples of about a second.

  Each exchange is the witness jolt-http's backpressure property reduced to
  plain teensyp: a 1 KB read buffer over a 93,388-byte payload, tens of read
  cycles, each handing a buffer to a worker and taking a `finish-work!`
  publication back. Every handoff is a chance to hit the window.

  Why this runs a batch, and what it is evidence OF
  -------------------------------------------------

  Measured against the unfixed jolt.net, one exchange hit the race in roughly
  one run in three: 1105 ms versus 78-80 ms. So a single exchange is a real but
  unreliable witness, and a total-time budget over one of them would be a flaky
  test in both directions. The batch fixes that. Under the defect the chance of
  a clean batch of 20 is about (2/3)^20, well under a thousandth.

  It is still a probabilistic oracle, and it is deliberately NOT the proof. The
  authoritative evidence is `test/jolt/net/wake_cursor_test.clj` in jolt-net,
  which hooks both the clock and the native `poll` call, forces the exact
  interleaving, and asserts on whether the wait was armed at native entry --
  with no clock reading anywhere. This namespace exists because that test proves
  a property of the poller, and someone still has to check that real bytes over
  a real socket through the real reactor behave accordingly.

  Two oracles, and the first is the one that keeps the second honest:

    1. EXACT BYTE CONSERVATION, order-sensitive, on every exchange. The payload
       is position-dependent, so truncation, duplication, and reordering all
       fail even at the correct total length. This passed with the defect
       present too -- delayed, not lost -- which is exactly why it is here: it
       proves the scenario really drove the reactor rather than passing
       trivially.

    2. NO EXCHANGE PARKED. Not a throughput budget: the question asked is
       whether any single exchange took a whole safety tick, because that is the
       defect's signature and nothing else in this path costs half a second. The
       threshold sits an order of magnitude above the observed healthy time and
       an order of magnitude below one tick, so it cannot be tuned into
       hiding a regression."
  (:require [teensyp.server :as tcp]
            [teensyp.buffer :as buf]
            [teensyp.ffi-net :as net]))

(def ^:private payload-size
  "The generated case from HTTP Hegel seed 9157075391771664454."
  93388)

(def ^:private read-buffer-size
  "The witness read buffer. Widening this was known to mask the defect, which is
  precisely why the regression pins it: a wider buffer means fewer read cycles
  and so fewer handoffs, not a fixed reactor."
  1024)

(def ^:private exchanges
  "Enough independent attempts that the defect cannot hide behind luck.

  Measured against the unfixed jolt.net, batches of 20 parked 1, 2, and 0 times
  -- about 5% per exchange, and that 0 is the point: 20 was NOT enough to gate
  on. At 60 the chance of a clean batch with the defect present is about
  0.95^60, near 5%, and the check is run alongside the deterministic jolt.net
  test rather than in place of it."
  60)

(def ^:private park-threshold-ms
  "An exchange slower than this parked. Healthy exchanges measured 78-105 ms and
  a lost wake costs a full 1000 ms tick, so this sits between two populations
  that are an order of magnitude apart on both sides."
  500)

(defn- pattern-bytes
  "A position-dependent payload, so truncation, duplication, and reordering are
  detectable rather than only total length."
  [n]
  (let [a (byte-array n)]
    (dotimes [i n]
      (aset-byte a i (unchecked-byte (bit-and (+ (* i 31) 7) 0xff))))
    a))

(defn- echo-handler
  ([_sock] {})
  ([state sock b]
   (let [n (buf/remaining b)]
     (when (pos? n) (tcp/write sock (buf/wrap (buf/get-bytes! b n))))
     state))
  ([_state _ex] nil))

(defn- first-difference
  "Index of the first differing byte, or nil when the arrays match exactly.
  Reported instead of a boolean so a conservation failure names where the stream
  diverged."
  [^bytes expected ^bytes actual]
  (let [n (min (alength expected) (alength actual))]
    (loop [i 0]
      (cond
        (= i n) (when (not= (alength expected) (alength actual)) n)
        (not= (aget expected i) (aget actual i)) i
        :else (recur (inc i))))))

(defn- exchange!
  "Send the payload and read exactly that many bytes back.

  Sending runs on its own future because a 93 KB echo does not fit in socket
  buffers: a client that sent everything before reading would deadlock against
  its own peer once both directions filled."
  [port ^bytes payload]
  (let [socket (net/connect-loopback port)]
    (try
      (let [total (alength payload)
            out (byte-array total)
            start (System/nanoTime)
            sender (future (net/client-send-all socket payload))
            finish (fn [got reads]
                     @sender
                     {:bytes (if (= got total)
                               out
                               (java.util.Arrays/copyOf out got))
                      :reads reads
                      :elapsed-ms (quot (- (System/nanoTime) start)
                                        1000000)})]
        (loop [got 0 reads 0]
          (if (>= got total)
            (finish got reads)
            (if-let [^bytes chunk (net/client-recv socket (- total got))]
              (do
                (System/arraycopy chunk 0 out got (alength chunk))
                (recur (+ got (alength chunk)) (inc reads)))
              ;; EOF before the full echo: report the truncation through the
              ;; conservation check rather than hanging here.
              (finish got reads)))))
      (finally (net/close! socket)))))

(defn- exchange-on-fresh-server!
  "One exchange against a reactor that has just started and has nothing else to
  do.

  The fresh server is load-bearing, not hygiene. Measured against the unfixed
  jolt.net, a batch of exchanges over one warm server never parked, because a
  reactor with a continuously readable socket returns from `poll` on readiness
  and never has to rely on the wake at all -- the lost wake is there, but it is
  masked. The defect surfaces when the worker's publication is the ONLY thing
  that could wake the reactor, which is an otherwise-idle one. That is also the
  shape jolt-http hit: a request arriving at a quiet server."
  [^bytes payload]
  (let [srv (tcp/run-server :port 0
                            :handler echo-handler
                            :reuse-address? true
                            :read-buffer-size read-buffer-size
                            :write-queue-size 2)]
    (try
      (exchange! (:port srv) payload)
      (finally (tcp/stop-server srv)))))

(defn run!
  "Run the batch and return a summary map. `check` is supplied by the caller's
  harness so this namespace stays framework-less like the rest of the suite."
  [check]
  (let [payload (pattern-bytes payload-size)
        results (vec (for [_ (range exchanges)]
                       (exchange-on-fresh-server! payload)))
        bad-length (remove #(= payload-size (alength ^bytes (:bytes %))) results)
        diverged (keep #(first-difference payload (:bytes %)) results)
        total-reads (reduce + (map :reads results))
        latencies (mapv :elapsed-ms results)
        parked (filterv #(>= % park-threshold-ms) latencies)]
    (check "re-arm: every exchange echoed exactly the payload length"
           [] (vec bad-length))
    (check "re-arm: every byte is conserved in order, in every exchange"
           [] (vec diverged))
    ;; With a 1 KB read buffer these payloads cannot arrive in a handful of
    ;; reads. If they did, the scenario would not be exercising the reactor
    ;; handoff at all and the latency oracle below would be meaningless.
    (check "re-arm: the batch really took many read cycles"
           true (>= total-reads (* 8 exchanges)))
    (check "re-arm: no exchange parked to the 1000 ms safety tick"
           [] parked)
    {:exchanges exchanges
     :bytes-each payload-size
     :total-reads total-reads
     :max-ms (apply max latencies)
     :median-ms (nth (sort latencies) (quot (count latencies) 2))
     :parked (count parked)}))
