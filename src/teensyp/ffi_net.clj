(ns teensyp.ffi-net
  "Compatibility helpers for the real-loopback test harness.

  This namespace no longer binds libc or exposes raw descriptors. Every socket,
  readiness wait, half-close, and byte transfer delegates through the public
  `teensyp.client` API. New consumers should require that namespace directly."
  (:require [teensyp.client :as client]))

(defn connect-loopback
  "Connect an opaque blocking client to IPv4 loopback."
  [port]
  (client/connect "127.0.0.1" port {:no-delay? true}))

(defn close! [socket]
  (client/close! socket))

(defn shutdown-write!
  "Half-close the socket's write side while retaining its readable side."
  [socket]
  (client/shutdown-write! socket))

(defn client-send-all
  "Send all bytes through the production blocking client."
  [socket data]
  (client/send-all! socket data))

(defn client-recv
  "Wait for and receive at most len bytes. Returns nil at EOF."
  [socket len]
  (client/receive-at-most! socket len))
