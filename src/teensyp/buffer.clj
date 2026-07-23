(ns teensyp.buffer
  "A ByteBuffer-equivalent for jolt (which has no java.nio.ByteBuffer) plus the
  teensyp.buffer utility API. A Buffer wraps a Clojure byte-array with mutable
  `position` and `limit` (volatiles), mirroring java.nio.ByteBuffer's relative
  read/write model: 0 <= position <= limit <= capacity. Backing by a byte-array
  (rather than raw ffi memory) keeps handler-created buffers GC-managed and
  leak-safe; the server copies to/from native memory only at the socket edge.

  Charset note: jolt has no java.nio.charset.Charset, so charset arguments are
  charset-NAME strings (e.g. \"UTF-8\"); nil defaults to UTF-8."
  (:refer-clojure :exclude [read-line]))

(defrecord Buffer [^bytes arr ^long capacity position limit])

(defn- cs ^String [charset] (if (string? charset) charset "UTF-8"))
(defn- ub ^long [b] (bit-and (long b) 0xff))          ; signed byte -> unsigned 0..255

;; --- construction ----------------------------------------------------------
(defn buffer
  "Create a Buffer of `size` bytes, position 0, limit = capacity."
  [size]
  (->Buffer (byte-array size) size (volatile! 0) (volatile! (long size))))

(defn wrap
  "Wrap an existing byte-array as a Buffer (position 0, limit = length)."
  [^bytes arr]
  (->Buffer arr (alength arr) (volatile! 0) (volatile! (alength arr))))

;; --- ByteBuffer-style accessors --------------------------------------------
(defn position       [b] (long @(:position b)))
(defn limit          [b] (long @(:limit b)))
(defn capacity       [b] (:capacity b))
(defn remaining      [b] (- (long @(:limit b)) (long @(:position b))))
(defn has-remaining? [b] (< (long @(:position b)) (long @(:limit b))))
(defn set-position!  [b n] (vreset! (:position b) (long n)) b)
(defn set-limit!     [b n] (vreset! (:limit b) (long n)) b)
(defn get-byte
  "Absolute (signed) byte at index `i` — does not advance position."
  [b i] (aget ^bytes (:arr b) (int i)))

(defn clear
  "Reset to position 0, limit = capacity."
  [b] (vreset! (:position b) 0) (vreset! (:limit b) (long (:capacity b))) b)

(defn flip
  "limit = position; position = 0. Prepares a just-written buffer for reading."
  [b] (vreset! (:limit b) (long @(:position b))) (vreset! (:position b) 0) b)

(defn duplicate
  "A view sharing the same backing array, with independent position/limit
  copied from `b`. Used for the handler-facing read-view."
  [b] (->Buffer (:arr b) (:capacity b) (volatile! (long @(:position b)))
                (volatile! (long @(:limit b)))))

(defn compact
  "Move the unread region [position,limit) to the front (in place, since a
  read-view may share this array); position = old remaining, limit = capacity."
  [b]
  (let [^bytes arr (:arr b) pos (long @(:position b)) lim (long @(:limit b)) n (- lim pos)]
    (loop [i 0] (when (< i n) (aset arr i (aget arr (+ pos i))) (recur (inc i))))  ; dest<src, forward-safe
    (vreset! (:position b) n)
    (vreset! (:limit b) (long (:capacity b)))
    b))

;; --- bulk moves ------------------------------------------------------------
(defn put-bytes!
  "Copy `len` bytes of `src` (from `off`) into `b` at its position; advance
  position. Used by the server to move recv'd bytes into the read buffer."
  [b ^bytes src off len]
  (let [^bytes arr (:arr b) pos (long @(:position b)) off (long off) len (long len)]
    (loop [i 0] (when (< i len) (aset arr (+ pos i) (aget src (+ off i))) (recur (inc i))))
    (vreset! (:position b) (+ pos len))
    b))

(defn get-bytes!
  "Read `len` bytes relative from position into a fresh byte-array; advance
  position. Also usable by the server to extract bytes to send."
  ^bytes [b len]
  (let [pos (long @(:position b)) len (long len)
        out (java.util.Arrays/copyOfRange ^bytes (:arr b) (int pos) (int (+ pos len)))]
    (vreset! (:position b) (+ pos len))
    out))

(defn- put!
  "Copy src's remaining into dest at dest.position; advance both."
  [dest src]
  (let [^bytes darr (:arr dest) ^bytes sarr (:arr src)
        dpos (long @(:position dest)) spos (long @(:position src))
        n (- (long @(:limit src)) spos)]
    (loop [i 0] (when (< i n) (aset darr (+ dpos i) (aget sarr (+ spos i))) (recur (inc i))))
    (vreset! (:position src) (+ spos n))
    (vreset! (:position dest) (+ dpos n))
    dest))

;; --- teensyp.buffer utility API --------------------------------------------
(defn str->buffer
  "Convert a String into a Buffer (charset name string)."
  [s charset] (wrap (.getBytes ^String s (cs charset))))

(defn buffer->str
  "Read a String from a Buffer's remaining bytes (charset name string)."
  [b charset] (String. (get-bytes! b (remaining b)) (cs charset)))

(defn index-of
  "First index of byte `needle` at/after position, else -1."
  ^long [b needle]
  (let [^bytes arr (:arr b) lim (long @(:limit b)) needle (ub needle)]
    (loop [i (long @(:position b))]
      (if (< i lim)
        (if (= needle (ub (aget arr i))) i (recur (inc i)))
        -1))))

(defn- matches-tail? [b ^long index ^bytes needle]
  (loop [i (inc index) j 1]
    (if (< j (alength needle))
      (when (= (ub (get-byte b i)) (ub (aget needle j))) (recur (inc i) (inc j)))
      true)))

(defn index-of-array
  "First index of the byte-array `needle` at/after position, else -1. The match
  must lie wholly inside [position,limit)."
  ^long [b ^bytes needle]
  (let [m (alength needle)]
    (if (zero? m)
      -1
      ;; `end` is the LAST legal start index: a match starting later would run
      ;; past the limit. Bounding the loop by it (rather than by end-1) is what
      ;; lets a needle in the final m bytes be found, and it keeps both the
      ;; get-byte probe and matches-tail? inside the live region — an empty or
      ;; too-short region makes end < position, so the loop returns -1 without
      ;; ever reading the array.
      (let [b0 (ub (aget needle 0)) end (- (limit b) m)]
        (loop [i (long (position b))]
          (if (> i end)
            -1
            (if (and (= b0 (ub (get-byte b i))) (matches-tail? b i needle))
              i
              (recur (inc i)))))))))

(defn read-line
  "Next line up to LF (trailing CR/LF stripped), advancing position; nil if no
  LF is present yet (so callers can wait for more data)."
  [b charset]
  (let [CR 0x0D LF 0x0A
        start (position b)
        index (index-of b LF)]
    (when (not= index -1)
      ;; The CR probe must stay at/after `start`: for an empty line the LF is at
      ;; `start` itself, and (dec index) would read either index -1 (a leading
      ;; blank line, the common case since the server resets the read-view to
      ;; position 0) or a byte already consumed by the previous read-line.
      (let [len (if (and (> index start) (= CR (ub (get-byte b (dec index)))))
                  (- index start 1)
                  (- index start))
            bs  (get-bytes! b len)]
        (set-position! b (inc index))
        (String. bs (cs charset))))))

(defn read-line-strict
  "Like [[read-line]], but a line **must** be terminated by CRLF.

  Returns the line, `nil` when no LF is present yet (so callers can wait for
  more data, exactly as [[read-line]] does), or `::bare-lf` when a complete line
  was found whose terminator was a lone LF.

  This exists because [[read-line]] strips the terminator, so a caller cannot
  tell CRLF from a bare LF afterwards — and for some grammars that distinction
  is a security property, not a nicety. RFC 9112 2.2 lets an HTTP recipient
  treat a lone LF as a line terminator in the start-line and field lines, but
  the chunked grammar in 7.1 has no such latitude, and a front end that requires
  CRLF while a back end accepts LF will frame two different messages out of one
  byte stream (request smuggling).

  Deliberately a separate function returning a three-way result rather than
  [[read-line]] returning a [line terminator] pair: this is the hot path of any
  line protocol — one call per request line and one per header field — and a
  tuple would allocate a throwaway vector on every one of them. The success path
  here allocates exactly what [[read-line]] allocates."
  [b charset]
  (let [CR 0x0D LF 0x0A
        start (position b)
        index (index-of b LF)]
    (when (not= index -1)
      ;; The CR probe must stay at/after `start`, for the same reason as in
      ;; read-line: for an empty line the LF is at `start` itself, and
      ;; (dec index) would read a byte the previous read already consumed.
      (if-not (and (> index start) (= CR (ub (get-byte b (dec index)))))
        (do (set-position! b (inc index)) ::bare-lf)
        (let [bs (get-bytes! b (- index start 1))]
          (set-position! b (inc index))
          (String. bs (cs charset)))))))

(defn copy
  "Copy src's remaining into dest until dest is full or src is exhausted;
  advance both positions. Returns the number of bytes copied."
  [src dest]
  (let [total (min (remaining src) (remaining dest))]
    (when (pos? total)
      (let [diff (- (remaining src) (remaining dest))]
        (if (pos? diff)
          (let [lim (limit src)]
            (set-limit! src (- lim diff))
            (put! dest src)
            (set-limit! src lim))
          (put! dest src))))
    total))
