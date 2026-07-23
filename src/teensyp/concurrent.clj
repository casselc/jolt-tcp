(ns teensyp.concurrent
  "Utility macros for concurrent programming. (No java.util.concurrent.locks.Lock
  type hint here — jolt is dynamically dispatched and has no such interface class,
  but ReentrantLock's .lock/.unlock resolve fine.)")

(defmacro with-lock
  "Lock `lock` before running the body, and finally unlock it once the body
  completes or throws."
  [lock & body]
  `(let [lock# ~lock]
     (.lock lock#)
     (try ~@body (finally (.unlock lock#)))))
