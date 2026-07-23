(ns teensyp.buffer-property-test
  "Generative properties for teensyp.buffer, run through jolt-hegel (Hegel's
  native generation/shrinking engine).

  teensyp.buffer is a hand-rolled java.nio.ByteBuffer replacement: mutable
  position/limit volatiles, in-place compact, a shared-array duplicate used as
  the server's read-view, and byte/array/line scanning. That is index arithmetic,
  which is exactly where hand-picked examples pass and off-by-ones survive — so
  these encode the contract from each function's docstring and let the engine
  choose the bytes and the boundaries.

  Each property runs under hegel.clojure-test/with, which shrinks a failure to a
  minimal counterexample and reports only the final replay assertions. Failures
  print a seed; replay it with (parse-long seed) as :seed."
  (:require [clojure.test :refer [deftest is]]
            [hegel.clojure-test :refer [with]]
            [hegel.core :as h]
            [hegel.generator :as g]
            [hegel.stateful :as hs]
            [teensyp.buffer :as buf]))

(def ^:private opts
  {:test-cases 200 :database "" :verbosity :quiet})

;; --- model helpers ---------------------------------------------------------
;; The model is a plain vector of unsigned byte values. Keeping it independent of
;; teensyp.buffer is the point: agreement between the two is the property.

(defn- ba->vec
  "Byte-array -> vector of unsigned 0..255 ints."
  [^bytes bs]
  (loop [i 0 acc []]
    (if (< i (alength bs))
      (recur (inc i) (conj acc (bit-and (long (aget bs i)) 0xff)))
      acc)))

(defn- live
  "The model's live region [position,limit) as a vector."
  [v pos lim] (subvec v pos lim))

(defn- naive-index-of
  "Independent reimplementation of buf/index-of."
  [v pos lim needle]
  (loop [i (long pos)]
    (cond (>= i (long lim))     -1
          (= (long needle) (nth v i)) i
          :else                 (recur (inc i)))))

(defn- naive-index-of-array
  "Independent reimplementation of buf/index-of-array. A match must lie wholly
  inside [position,limit); an empty needle is -1, matching the documented
  behavior."
  [v pos lim needle]
  (let [m (count needle)]
    (if (zero? m)
      -1
      (loop [i (long pos)]
        (if (> (+ i m) (long lim))
          -1
          (if (= needle (subvec v i (+ i m))) i (recur (inc i))))))))

;; A buffer built from a generated array with a generated position/limit inside
;; it. Drawn as one unit so the bounds are always valid by construction rather
;; than by rejection (h/assume! would throw away most cases here).
(defn- region-gen [max-cap]
  (g/let [cap (g/integer 1 max-cap)
          arr (g/bytes cap cap)
          lim (g/integer 0 cap)
          pos (g/integer 0 lim)]
    {:arr arr :cap cap :lim lim :pos pos}))

(defn- region->buffer [{:keys [arr lim pos]}]
  (doto (buf/wrap arr) (buf/set-limit! lim) (buf/set-position! pos)))

;; --- 1. text round trip ----------------------------------------------------
(deftest str-buffer-round-trip
  (with (assoc opts :name "buffer/str-round-trip")
        [s (g/string {:codec :utf-8 :max-size 200})]
        (h/fprn :minimal-string s)
        (let [b (buf/str->buffer s "UTF-8")]
          (is (= s (buf/buffer->str b "UTF-8")))
          (is (zero? (buf/remaining b))))))

;; --- 2. wrap / get-bytes! --------------------------------------------------
(deftest wrap-get-bytes-round-trip
  (with (assoc opts :name "buffer/wrap-get-bytes")
        [bs (g/bytes 0 256)]
        (let [b (buf/wrap bs)
              expected (ba->vec bs)]
          (is (= (alength bs) (buf/capacity b)))
          (is (= (alength bs) (buf/remaining b)))
          (is (= expected (ba->vec (buf/get-bytes! b (alength bs)))))
          (is (= (alength bs) (buf/position b))))))

;; --- 3. put-bytes! chunks, flip, read back ---------------------------------
;; The server fills the read buffer with successive recv chunks, then flips a
;; view over it; concatenation must be preserved across chunk boundaries.
(deftest put-chunks-then-read-back
  (with (assoc opts :name "buffer/put-chunks")
        [chunks (g/vector {:max-size 8} (g/bytes 0 64))]
        (let [total (reduce + 0 (map alength chunks))
              b (buf/buffer total)]
          (doseq [^bytes c chunks] (buf/put-bytes! b c 0 (alength c)))
          (is (= total (buf/position b)))
          (buf/flip b)
          (is (zero? (buf/position b)))
          (is (= total (buf/limit b)))
          (is (= (vec (mapcat ba->vec chunks)) (ba->vec (buf/get-bytes! b total)))))))

;; --- 4. the documented bounds invariant ------------------------------------
;; "0 <= position <= limit <= capacity", from the ns docstring, must survive any
;; of the position-mutating operations.
(deftest bounds-invariant-holds
  (with (assoc opts :name "buffer/bounds")
        [region (region-gen 128)
         op (g/sampled-from [:clear :flip :compact :none])]
        (let [b (region->buffer region)]
          (case op
            :clear   (buf/clear b)
            :flip    (buf/flip b)
            :compact (buf/compact b)
            :none    nil)
          (h/fprn :minimal-region (dissoc region :arr) :op op)
          (is (<= 0 (buf/position b)) "position is non-negative")
          (is (<= (buf/position b) (buf/limit b)) "position <= limit")
          (is (<= (buf/limit b) (buf/capacity b)) "limit <= capacity"))))

;; --- 5. compact moves the unread region to the front -----------------------
(deftest compact-preserves-unread-region
  (with (assoc opts :name "buffer/compact")
        [{:keys [arr cap lim pos] :as region} (region-gen 128)]
        (let [b (region->buffer region)
              expected (live (ba->vec arr) pos lim)   ;; captured before the in-place move
              n (- lim pos)]
          (buf/compact b)
          (h/fprn :minimal-region (dissoc region :arr))
          (is (= n (buf/position b)) "position = old remaining")
          (is (= cap (buf/limit b)) "limit = capacity")
          (is (= expected (subvec (ba->vec (:arr b)) 0 n)) "unread bytes moved intact"))))

;; --- 6. index-of agrees with a naive scan ----------------------------------
(deftest index-of-matches-model
  (with (assoc opts :name "buffer/index-of")
        [{:keys [arr lim pos] :as region} (region-gen 64)
         needle (g/octet)]
        (let [b (region->buffer region)
              expected (naive-index-of (ba->vec arr) pos lim needle)]
          (h/fprn :minimal-region (dissoc region :arr) :needle needle)
          (is (= expected (buf/index-of b (unchecked-byte needle)))))))

;; --- 7. index-of-array agrees with a naive scan ----------------------------
;; Expected to fail before the fix: the loop bound stops one index short, so a
;; needle sitting in the last needle-length bytes is never tested.
(deftest index-of-array-matches-model
  (with (assoc opts :name "buffer/index-of-array")
        [{:keys [arr lim pos] :as region} (region-gen 64)
         nlen (g/integer 1 4)
         needle (g/bytes nlen nlen)]
        (let [b (region->buffer region)
              expected (naive-index-of-array (ba->vec arr) pos lim (ba->vec needle))]
          (h/fprn :minimal-region (dissoc region :arr)
                  :needle (ba->vec needle) :expected expected)
          (is (= expected (buf/index-of-array b needle))))))

;; Regression: the minimal counterexamples Hegel shrank to on the first run,
;; kept as fixed assertions so a reintroduced off-by-one fails fast and
;; readably, without waiting for generation to rediscover it.
(deftest index-of-array-regressions
  ;; The loop bound used to be (limit - needle-length - 1), so a needle sitting
  ;; in the final needle-length bytes was never tested.
  (is (= 2 (buf/index-of-array (buf/str->buffer "abc" "UTF-8") (.getBytes "c" "UTF-8"))))
  (is (= 1 (buf/index-of-array (buf/str->buffer "abc" "UTF-8") (.getBytes "bc" "UTF-8"))))
  (is (= 0 (buf/index-of-array (buf/str->buffer "abc" "UTF-8") (.getBytes "abc" "UTF-8"))))
  ;; Shrunk case: capacity 1, limit 0 — an empty live region scanned anyway,
  ;; reading a byte past the limit and reporting a match at 0.
  (is (= -1 (buf/index-of-array (doto (buf/wrap (byte-array 1)) (buf/set-limit! 0))
                                (byte-array 1))))
  (is (= -1 (buf/index-of-array (buf/str->buffer "ab" "UTF-8") (.getBytes "abc" "UTF-8")))))

;; --- 8. read-line returns exactly the lines that were written --------------
;; Expected to fail before the fix: a leading empty line puts the LF at index 0,
;; and read-line then probes (dec index) = -1.
(deftest read-line-recovers-written-lines
  (with (assoc opts :name "buffer/read-line")
        [lines (g/vector {:max-size 6}
                         (g/string {:codec :ascii :max-size 12
                                    :exclude-characters "\r\n"}))
         crlf? (g/boolean)]
        (let [term (if crlf? "\r\n" "\n")
              text (apply str (map #(str % term) lines))
              b (buf/str->buffer text "UTF-8")
              got (loop [acc []]
                    (if-some [l (buf/read-line b "UTF-8")] (recur (conj acc l)) acc))]
          (h/fprn :minimal-lines lines :crlf? crlf?)
          (is (= lines got)))))

(deftest read-line-nil-without-terminator
  (with (assoc opts :name "buffer/read-line-partial")
        [s (g/string {:codec :ascii :max-size 24 :exclude-characters "\r\n"})]
    ;; No LF yet: the caller must be told to wait for more data, not handed a
    ;; truncated line.
        (let [b (buf/str->buffer s "UTF-8")]
          (is (nil? (buf/read-line b "UTF-8")))
          (is (zero? (buf/position b)) "position must not advance"))))

(deftest read-line-regressions
  ;; A leading blank line put the LF at index 0, so the CR probe read index -1.
  ;; Reachable from any client that opens with a blank line.
  (is (= "" (buf/read-line (buf/str->buffer "\nfoo\n" "UTF-8") "UTF-8")))
  (let [b (buf/str->buffer "\nfoo\n" "UTF-8")]
    (is (= ["" "foo"] [(buf/read-line b "UTF-8") (buf/read-line b "UTF-8")])))
  (is (= "" (buf/read-line (buf/str->buffer "\r\nfoo\r\n" "UTF-8") "UTF-8")))
  ;; Blank lines mid-stream, both terminators — the CRLFCRLF header terminator.
  (let [b (buf/str->buffer "a\r\n\r\nb\r\n" "UTF-8")]
    (is (= ["a" "" "b"] (repeatedly 3 #(buf/read-line b "UTF-8"))))))

;; --- 9. copy conservation --------------------------------------------------
(deftest copy-conserves-bytes
  (with (assoc opts :name "buffer/copy")
        [src-region (region-gen 96)
         dst-region (region-gen 96)]
        (let [src (region->buffer src-region)
              dst (region->buffer dst-region)
              src-rem (buf/remaining src)
              dst-rem (buf/remaining dst)
              src-pos (buf/position src)
              dst-pos (buf/position dst)
              expected (live (ba->vec (:arr src-region)) src-pos (:lim src-region))
              n (min src-rem dst-rem)
              copied (buf/copy src dst)]
          (h/fprn :src (dissoc src-region :arr) :dst (dissoc dst-region :arr))
          (is (= n copied) "returns min(remaining src, remaining dest)")
          (is (= (+ src-pos n) (buf/position src)) "src advanced by copied")
          (is (= (+ dst-pos n) (buf/position dst)) "dest advanced by copied")
          (is (= (subvec expected 0 n)
                 (subvec (ba->vec (:arr dst)) dst-pos (+ dst-pos n)))
              "dest received exactly those bytes"))))

;; --- 10. duplicate is an independent cursor over a shared array ------------
;; This is the read-view contract: the server hands the handler a duplicate and
;; relies on the handler's position changes not disturbing the read buffer.
(deftest duplicate-is-independent-cursor
  (with (assoc opts :name "buffer/duplicate")
        [{:keys [lim pos] :as region} (region-gen 64)
         new-pos (g/integer 0 64)]
        (let [b (region->buffer region)
              d (buf/duplicate b)]
          (is (= pos (buf/position d)) "duplicate starts at the same position")
          (is (= lim (buf/limit d)) "duplicate starts at the same limit")
          (is (identical? (:arr b) (:arr d)) "backing array is shared")
          (buf/set-position! d (min new-pos lim))
          (is (= pos (buf/position b)) "original position unaffected"))))

;; --- stateful model --------------------------------------------------------
;; The pure properties above test one call at a time. The server's read path is a
;; *sequence*: put-bytes! from recv, a handler consuming part of the view, then
;; compact, repeatedly (server.clj handle-read / handle-pending-read). This runs
;; generated operation sequences against a model that tracks the bytes, position
;; and limit independently.
;;
;; The Buffer is constructed inside the property body, so each generated case
;; gets a fresh one.

(defn- model-bytes [b] (ba->vec (:arr b)))

(defn- sync-check
  "The model's view of the buffer must match the buffer's own accessors."
  [{:keys [sut pos lim cap]}]
  (and (= pos (buf/position sut))
       (= lim (buf/limit sut))
       (= cap (buf/capacity sut))))

(deftest buffer-stateful-model
  (with (assoc opts :test-cases 150 :name "buffer/stateful")
        [cap (g/integer 8 64)]
        (let [sut (buf/buffer cap)]
          (hs/run!
           {:initial-state {:sut sut :bytes (vec (repeat cap 0)) :pos 0 :lim cap :cap cap}
            :rules
            [;; write recv'd bytes into the free space, as recv-into! does
             (hs/rule :put-bytes
                      {:precondition (fn [{:keys [pos lim]}] (< pos lim))}
                      (fn [{:keys [sut pos lim bytes] :as state}]
                        (let [room (- lim pos)
                              chunk (h/draw! (g/bytes 1 room))
                              n (alength ^bytes chunk)]
                          (buf/put-bytes! sut chunk 0 n)
                          (assoc state
                                 :bytes (reduce (fn [v i]
                                                  (assoc v (+ pos i) (nth (ba->vec chunk) i)))
                                                bytes (range n))
                                 :pos (+ pos n)))))
         ;; a handler consuming part of what it was handed
             (hs/rule :get-bytes
                      {:precondition (fn [{:keys [pos lim]}] (< pos lim))}
                      (fn [{:keys [sut pos lim] :as state}]
                        (let [n (h/draw! (g/integer 1 (- lim pos)))]
                          (buf/get-bytes! sut n)
                          (assoc state :pos (+ pos n)))))
             (hs/rule :flip
                      (fn [{:keys [sut pos] :as state}]
                        (buf/flip sut)
                        (assoc state :lim pos :pos 0)))
             (hs/rule :clear
                      (fn [{:keys [sut cap] :as state}]
                        (buf/clear sut)
                        (assoc state :pos 0 :lim cap)))
         ;; the server's compaction step
             (hs/rule :compact
                      (fn [{:keys [sut pos lim bytes cap] :as state}]
                        (let [n (- lim pos)]
                          (buf/compact sut)
                          (assoc state
                             ;; bytes [0,n) are the moved region; the tail beyond
                             ;; n is untouched by compact, so keep the old values
                                 :bytes (into (subvec bytes pos lim) (subvec bytes n cap))
                                 :pos n :lim cap))))
             (hs/rule :set-position
                      (fn [{:keys [sut lim] :as state}]
                        (let [p (h/draw! (g/integer 0 lim))]
                          (buf/set-position! sut p)
                          (assoc state :pos p))))
             (hs/rule :set-limit
                      (fn [{:keys [sut pos cap] :as state}]
                        (let [l (h/draw! (g/integer pos cap))]
                          (buf/set-limit! sut l)
                          (assoc state :lim l))))]
            :invariants
            [(hs/invariant :bounds
                           (fn [{:keys [sut cap]}]
                             (and (<= 0 (buf/position sut))
                                  (<= (buf/position sut) (buf/limit sut))
                                  (<= (buf/limit sut) cap))))
             (hs/invariant :accessors-agree sync-check)
             (hs/invariant :remaining-agrees
                           (fn [{:keys [sut pos lim]}]
                             (= (- lim pos) (buf/remaining sut))))
             (hs/invariant :live-bytes-agree
                           (fn [{:keys [sut bytes pos lim]}]
                             (= (subvec bytes pos lim)
                                (subvec (model-bytes sut) pos lim))))]})
          (is true "stateful sequence completed without invariant violation"))))
