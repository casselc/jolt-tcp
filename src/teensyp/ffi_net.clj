(ns teensyp.ffi-net
  "POSIX socket + poll(2) transport for the jolt-tcp reactor, bound through
  jolt.ffi. Linux + macOS, loopback (127.0.0.1). Provides:

    - non-blocking listen/accept/recv/send helpers that report :eagain / :eof
      / :error by consulting errno,
    - a pollfd-array abstraction (jolt has no :short, so each 8-byte
      struct pollfd {int fd; short events; short revents} packs events/revents
      into the 32-bit slot at offset+4 and masks the low/high 16 bits),
    - a self-pipe (pipe(2)) to wake a parked poll() from any thread, since
      poll has no Selector.wakeup equivalent,
    - a tiny blocking client (connect/recv/send) for the test harness.

  SIGPIPE guard: a send() to a peer that has closed raises SIGPIPE, which would
  kill the whole jolt process (the JVM masks this; jolt does not). We pass
  MSG_NOSIGNAL on Linux and set SO_NOSIGPIPE on macOS accepted sockets."
  (:require [clojure.string :as str]
            [jolt.ffi :as ffi]))

(def ^:private os-name (str/lower-case (or (System/getProperty "os.name") "")))
(def macos? (str/includes? os-name "mac"))

;; Resolve libc symbols from the running process before the defcfn bindings
;; below are evaluated at namespace load.
(ffi/load-library)

;; --- C bindings ------------------------------------------------------------
(ffi/defcfn c-socket     "socket"     [:int :int :int] :int)
(ffi/defcfn c-bind       "bind"       [:int :pointer :int] :int)
(ffi/defcfn c-listen     "listen"     [:int :int] :int)
(ffi/defcfn c-setsockopt "setsockopt" [:int :int :int :pointer :int] :int)
(ffi/defcfn c-accept     "accept"     [:int :pointer :pointer] :int)
(ffi/defcfn c-connect    "connect"    [:int :pointer :int] :int)
(ffi/defcfn c-recv       "recv"       [:int :pointer :size_t :int] :ssize_t)
(ffi/defcfn c-send       "send"       [:int :pointer :size_t :int] :ssize_t)
(ffi/defcfn c-close      "close"      [:int] :int)
(ffi/defcfn c-shutdown   "shutdown"   [:int :int] :int)
(ffi/defcfn c-fcntl      "fcntl"      [:int :int :int] :int)
(ffi/defcfn c-pipe       "pipe"       [:pointer] :int)
(ffi/defcfn c-poll       "poll"       [:pointer :int :int] :int :blocking)
(ffi/defcfn c-read       "read"       [:int :pointer :size_t] :ssize_t)
(ffi/defcfn c-write      "write"      [:int :pointer :size_t] :ssize_t)

;; Blocking variants for the test client (GC-collect-safe while parked).
(ffi/defcfn c-recv-b     "recv"       [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn c-send-b     "send"       [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn c-connect-b  "connect"    [:int :pointer :int] :int :blocking)

;; errno lives behind a per-platform accessor returning int*.
(if macos?
  (ffi/defcfn c-errno-loc "__error"          [] :pointer)
  (ffi/defcfn c-errno-loc "__errno_location" [] :pointer))

(defn errno [] (ffi/read (c-errno-loc) :int))

;; --- constants (platform-branched data, as in jolt.nrepl) ------------------
(def ^:const AF-INET     2)
(def ^:const SOCK-STREAM 1)
(def sol-socket   (if macos? 0xffff 1))
(def so-reuseaddr (if macos? 4      2))
(def so-rcvbuf    (if macos? 0x1002 8))
(def so-nosigpipe 0x1022)                       ; macOS only
(def ^:const ipproto-tcp 6)
(def tcp-nodelay  (if macos? 4 1))
(def ^:const f-getfl 3)
(def ^:const f-setfl 4)
(def o-nonblock   (if macos? 0x0004 0x800))
(def eagain       (if macos? 35 11))            ; EAGAIN / EWOULDBLOCK
(def msg-nosignal (if macos? 0 0x4000))         ; Linux MSG_NOSIGNAL

(def ^:const POLLIN   0x001)
(def ^:const POLLOUT  0x004)
(def ^:const POLLERR  0x008)
(def ^:const POLLHUP  0x010)
(def ^:const POLLNVAL 0x020)

;; --- helpers ---------------------------------------------------------------
(defn- setsockopt-int! [fd level opt val]
  (let [p (ffi/alloc 4)]
    (try (ffi/write p :int 0 val) (c-setsockopt fd level opt p 4)
         (finally (ffi/free p)))))

(defn set-nonblocking! [fd]
  (let [fl (c-fcntl fd f-getfl 0)]
    (c-fcntl fd f-setfl (bit-or fl o-nonblock))))

;; A 16-byte sockaddr_in for 127.0.0.1:port. macOS byte0 = sin_len, byte1 =
;; family; Linux bytes0-1 = family little-endian. Port in network byte order.
(defn make-sockaddr [port]
  (let [sa (ffi/alloc 16)]
    (dotimes [i 16] (ffi/write sa :uint8 i 0))
    (if macos?
      (do (ffi/write sa :uint8 0 16) (ffi/write sa :uint8 1 AF-INET))
      (ffi/write sa :uint8 0 AF-INET))
    (ffi/write sa :uint8 2 (bit-and (bit-shift-right port 8) 0xff))
    (ffi/write sa :uint8 3 (bit-and port 0xff))
    (ffi/write sa :uint8 4 127) (ffi/write sa :uint8 7 1)   ; 127.0.0.1
    sa))

(defn close! [fd] (c-close fd))

(defn listen-socket
  "Create a non-blocking loopback listen socket on `port`. opts:
  :reuse-address? :recv-buffer-size. Throws ex-info on failure."
  [port {:keys [reuse-address? recv-buffer-size]}]
  (let [fd (c-socket AF-INET SOCK-STREAM 0)]
    (when (neg? fd) (throw (ex-info "socket() failed" {:errno (errno)})))
    (when reuse-address?   (setsockopt-int! fd sol-socket so-reuseaddr 1))
    (when recv-buffer-size (setsockopt-int! fd sol-socket so-rcvbuf recv-buffer-size))
    (let [sa (make-sockaddr port) r (c-bind fd sa 16)]
      (ffi/free sa)
      (when (neg? r) (c-close fd) (throw (ex-info (str "bind() failed on port " port) {:errno (errno)}))))
    (when (neg? (c-listen fd 64)) (c-close fd) (throw (ex-info "listen() failed" {:errno (errno)})))
    (set-nonblocking! fd)
    fd))

(defn accept*
  "Accept one pending connection on non-blocking `listen-fd`. Returns the new
  connection fd (non-blocking, TCP_NODELAY, SIGPIPE-guarded), :eagain when none
  is pending, or :error."
  [listen-fd]
  (let [fd (c-accept listen-fd ffi/null ffi/null)]
    (cond
      (>= fd 0) (do (set-nonblocking! fd)
                    (setsockopt-int! fd ipproto-tcp tcp-nodelay 1)
                    (when macos? (setsockopt-int! fd sol-socket so-nosigpipe 1))
                    fd)
      (= (errno) eagain) :eagain
      :else :error)))

(defn recv*
  "recv up to `len` bytes into native pointer `ptr`. Returns a positive count,
  :eof (peer closed), :eagain (nothing available now), or :error."
  [fd ptr len]
  (let [n (c-recv fd ptr len 0)]
    (cond
      (pos? n)           n
      (zero? n)          :eof
      (= (errno) eagain) :eagain
      :else              :error)))

(defn send*
  "send up to `len` bytes from native pointer `ptr` (MSG_NOSIGNAL on Linux).
  Returns bytes written (>= 0), :eagain, or :error."
  [fd ptr len]
  (let [n (c-send fd ptr len msg-nosignal)]
    (cond
      (>= n 0)           n
      (= (errno) eagain) :eagain
      :else              :error)))

;; --- pollfd array ----------------------------------------------------------
(def ^:const POLLFD-SIZE 8)

(defn alloc-pollfds [n] (ffi/alloc (* n POLLFD-SIZE)))

(defn set-pollfd!
  "Write struct pollfd[i] = {fd, events, revents=0}. events occupy the low 16
  bits of the 32-bit slot at offset+4; the kernel fills revents in the high 16."
  [arr i fd events]
  (ffi/write arr :int (* i POLLFD-SIZE) fd)
  (ffi/write arr :int (+ (* i POLLFD-SIZE) 4) (bit-and events 0xffff)))

(defn pollfd-revents [arr i]
  (bit-and (bit-shift-right (ffi/read arr :int (+ (* i POLLFD-SIZE) 4)) 16) 0xffff))

(defn poll*
  "Block in poll() over `n` pollfds for up to `timeout-ms` (-1 = infinite).
  Returns the number of ready fds, 0 on timeout, or -1 on error/EINTR."
  [arr n timeout-ms]
  (c-poll arr n timeout-ms))

;; --- self-pipe (wake a parked poll) ----------------------------------------
(defn make-pipe
  "Create a non-blocking pipe. Returns [read-fd write-fd]."
  []
  (let [p (ffi/alloc 8)]
    (try
      (when (neg? (c-pipe p)) (throw (ex-info "pipe() failed" {:errno (errno)})))
      (let [rfd (ffi/read p :int 0) wfd (ffi/read p :int 4)]
        (set-nonblocking! rfd) (set-nonblocking! wfd)
        [rfd wfd])
      (finally (ffi/free p)))))

(def ^:private wake-byte (doto (ffi/alloc 1) (ffi/write :uint8 0 33)))
(def ^:private drain-buf (ffi/alloc 4096))

(defn wake!  [wfd] (c-write wfd wake-byte 1))
(defn drain! [rfd] (loop [] (when (pos? (c-read rfd drain-buf 4096)) (recur))))

;; --- tiny blocking client (for the test harness) ---------------------------
(def ^:private SHUT-WR 1)

(defn shutdown-write!
  "Half-close: shut down this socket's write side, so the peer sees EOF while we
  keep reading. This is what an HTTP client does when it sends a request and
  then waits for the response, and servers must still answer."
  [fd]
  (c-shutdown fd SHUT-WR))
(defn connect-loopback
  "Blocking connect to 127.0.0.1:`port`. Returns a connected (blocking) fd, or
  throws ex-info."
  [port]
  (let [fd (c-socket AF-INET SOCK-STREAM 0)]
    (when (neg? fd) (throw (ex-info "socket() failed" {:errno (errno)})))
    (let [sa (make-sockaddr port) r (c-connect-b fd sa 16)]
      (ffi/free sa)
      (when (neg? r) (c-close fd) (throw (ex-info "connect() failed" {:errno (errno)})))
      fd)))

(defn client-send-all
  "Send all bytes of `^bytes data` on a blocking fd."
  [fd data]
  (let [n (alength data) buf (ffi/alloc (max 1 n))]
    (try
      (ffi/write-array buf data)
      (loop [off 0]
        (when (< off n)
          (let [sent (c-send-b fd (+ buf off) (- n off) msg-nosignal)]
            (when (pos? sent) (recur (+ off sent))))))
      (finally (ffi/free buf)))))

(defn client-recv
  "Blocking recv of up to `len` bytes. Returns a byte-array (possibly shorter
  than len), or nil on EOF."
  [fd len]
  (let [buf (ffi/alloc len)]
    (try
      (let [n (c-recv-b fd buf len 0)]
        (when (pos? n) (ffi/read-array buf n)))
      (finally (ffi/free buf)))))
