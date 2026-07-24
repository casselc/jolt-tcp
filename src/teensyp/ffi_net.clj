(ns teensyp.ffi-net
  "Compatibility helpers for the real-loopback test harness.

  This namespace no longer binds libc or exposes raw descriptors. Every socket,
  readiness wait, half-close, and byte transfer delegates through jolt.net's
  owned-handle API. Production teensyp.server depends on jolt.net directly."
  (:require [jolt.net :as net]))

(defn- await!
  [socket interests]
  (let [poller (net/open-poller)]
    (try
      (net/register! poller socket interests)
      (loop []
        (when (empty? (net/await-ready poller 1000))
          (recur)))
      (finally (net/close! poller)))))

(defn connect-loopback
  "Connect an owned jolt.net socket to IPv4 loopback."
  [port]
  (net/connect (net/endpoint "127.0.0.1" port) {:no-delay? true}))

(defn close! [socket]
  (net/close! socket))

(defn shutdown-write!
  "Half-close the socket's write side while retaining its readable side."
  [socket]
  (net/shutdown! socket :write))

(defn client-send-all
  "Send all bytes through jolt.net's non-blocking byte API."
  [socket data]
  (let [n (alength data)]
    (loop [off 0]
      (when (< off n)
        (let [sent (net/try-write-bytes! socket data off (- n off))]
          (if (net/would-block? sent)
            (do (await! socket #{:write}) (recur off))
            (recur (+ off sent)))))))
  nil)

(defn client-recv
  "Wait for and receive at most len bytes. Returns nil at EOF."
  [socket len]
  (let [data (byte-array len)]
    (loop []
      (let [n (net/try-read-bytes! socket data 0 len)]
        (cond
          (net/would-block? n) (do (await! socket #{:read}) (recur))
          (net/eof? n) nil
          (= n len) data
          :else (java.util.Arrays/copyOfRange data 0 (int n)))))))
