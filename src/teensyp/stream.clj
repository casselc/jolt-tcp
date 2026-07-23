(ns teensyp.stream
  "A jolt-native blocking-connection adapter over a teensyp.server handler — the
  counterpart to JVM teensyp.stream. jolt cannot subclass java.io.InputStream/
  OutputStream (its `proxy` is just `reify`, and its io coercions reject reified
  streams), so instead of Input/OutputStream this exposes a `conn` object with
  blocking byte-array and line operations, backed by a core.async channel that
  the read handler feeds.

  Backpressure: the read handler pushes each chunk with a blocking >!!. While it
  is parked on a full channel the socket stays WORKING, so the reactor stops
  reading and TCP flow-controls the sender — no explicit pause/resume needed.
  (One worker-pool thread is held per slow consumer; size :pool-size on the
  server accordingly, or raise :buffer-size here.)"
  (:require [clojure.core.async :as a]
            [teensyp.server :as tcp]
            [teensyp.buffer :as buf]))

(defprotocol Conn
  (conn-recv [conn]
    "Block for the next incoming byte-array chunk; nil at end of stream.")
  (conn-read-line [conn]
    "Block for the next LF-delimited line (trailing CR/LF stripped); nil at EOF
    (a final unterminated fragment is returned once before nil).")
  (conn-send [conn data]
    "Queue a byte-array or String to be written to the peer.")
  (conn-close [conn]
    "Close the connection."))

(defn- lf-index ^long [^bytes a]
  (loop [i 0]
    (cond (>= i (alength a)) -1
          (= 10 (bit-and (aget a i) 0xff)) i
          :else (recur (inc i)))))

(defn- concat-bytes ^bytes [^bytes a ^bytes b]
  (let [la (alength a) lb (alength b) r (byte-array (+ la lb))]
    (dotimes [i la] (aset r i (aget a i)))
    (dotimes [i lb] (aset r (+ la i) (aget b i)))
    r))

(defrecord StreamConn [sock in leftover charset]
  Conn
  (conn-recv [_] (a/<!! in))
  (conn-send [_ data]
    (tcp/write sock (buf/wrap (if (string? data) (.getBytes ^String data charset) data))))
  (conn-close [_] (tcp/close sock))
  (conn-read-line [_]
    (loop []
      (let [^bytes lo @leftover idx (lf-index lo)]
        (if (>= idx 0)
          (let [end  (if (and (> idx 0) (= 13 (bit-and (aget lo (dec idx)) 0xff))) (dec idx) idx)
                line (String. (java.util.Arrays/copyOfRange lo 0 (int end)) charset)]
            (reset! leftover (java.util.Arrays/copyOfRange lo (int (inc idx)) (int (alength lo))))
            line)
          (if-let [^bytes chunk (a/<!! in)]
            (do (reset! leftover (concat-bytes lo chunk)) (recur))
            (when (pos? (alength lo))
              (reset! leftover (byte-array 0))
              (String. lo charset))))))))

(defn stream-handler
  "Produce a teensyp.server handler that runs `(f conn)` on its own thread, where
  `conn` supports conn-recv / conn-read-line / conn-send / conn-close. `f`'s
  return value is ignored; the socket is closed when `f` returns.

  Options: :buffer-size (channel depth, default 256), :charset (default UTF-8)."
  ([f] (stream-handler f {}))
  ([f {:keys [buffer-size charset] :or {buffer-size 256 charset "UTF-8"}}]
   (fn
     ([sock]
      (let [in   (a/chan buffer-size)
            conn (->StreamConn sock in (atom (byte-array 0)) charset)]
        ;; Closing `in` when f exits is not just tidiness: the read arity feeds
        ;; this channel with a blocking >!! on a worker-pool thread. If f has
        ;; gone away — returned early, or thrown — nothing will ever drain the
        ;; channel again, so once its buffer fills that put parks forever and
        ;; the worker thread is gone for good. :pool-size of those and the
        ;; server stops serving every connection, permanently. close! releases
        ;; parked puts (they return false), so the worker always gets out.
        (future (try (f conn)
                     (catch :default _ nil)
                     (finally (a/close! in) (tcp/close sock))))
        {:in in}))
     ([state sock b]
      (let [n (buf/remaining b)]
        (when (pos? n) (a/>!! (:in state) (buf/get-bytes! b n))))
      ;; The peer half-closed its write side: no more data will ever arrive, so
      ;; end the stream. Without this, conn-recv/conn-read-line block forever on
      ;; a channel nothing will ever feed again — the connection is not closed
      ;; on EOF (the peer is still reading and is owed a response), so the
      ;; close-arity that would close the channel never runs either. A client
      ;; doing the ordinary send-request / shutdown(SHUT_WR) / read-reply dance
      ;; hung until it gave up.
      ;;
      ;; Closing the channel is what lets `f` see end-of-stream, return, and hit
      ;; its (finally (tcp/close sock)) — and since ::close is queued behind any
      ;; writes `f` already made, the reply still flushes before the close.
      ;; peer-eof-notified?, not peer-closed?: the latter can be true while
      ;; this invocation holds a view taken before EOF, and closing the channel
      ;; then strands pre-EOF bytes the next invocation would have delivered.
      (when (tcp/peer-eof-notified? sock) (a/close! (:in state)))
      state)
     ([state _ex]
      (a/close! (:in state))
      nil))))
