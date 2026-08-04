(ns teensyp.windows-arm64-preview
  "Native ARM64 source-mode preview for TCP namespace and poller support."
  (:require [clojure.test :as t]
            [jolt.net :as net]
            [jolt.net.target :as target]
            [teensyp.buffer-property-test]
            [teensyp.client]
            [teensyp.server]))

(defn- check-public-poller! []
  (let [poller (net/open-poller)]
    (try
      (when-not (= [] (net/await-ready poller 0))
        (throw
          (ex-info "empty Windows ARM64 poll returned unexpected events"
                   {})))
      (let [cursor (net/wake-cursor poller)
            _ (net/wake! poller)
            waiter (future (net/await-ready poller 120000 cursor))
            ready (deref waiter 15000 ::wake-timeout)]
        (when (= ::wake-timeout ready)
          (throw
            (ex-info "Windows ARM64 wake-cursor wait parked after publication"
                     {:cursor cursor
                      :watchdog-ms 15000})))
        (when-not (= [] ready)
          (throw
            (ex-info "Windows ARM64 wake-cursor wait returned unexpected events"
                     {:cursor cursor
                      :ready ready}))))
      (finally
        (net/close! poller)))))

(defn -main [& _]
  (let [observed (jolt.host/target)
        descriptor (target/descriptor observed)
        _ (check-public-poller!)
        result (t/run-tests 'teensyp.buffer-property-test)
        failed (+ (:fail result 0) (:error result 0))]
    (when-not (= [:windows :aarch64 64]
                 [(:os observed) (:arch observed) (:pointer-bits observed)])
      (throw
        (ex-info "portable TCP preview did not run on native Windows ARM64"
                 {:target observed})))
    (when-not (and (target/supported-target? observed)
                   (= :windows (:platform descriptor))
                   (= :probed (:evidence descriptor))
                   (= descriptor (net/target-descriptor)))
      (throw
        (ex-info "jolt.net did not select its probed Windows ARM64 descriptor"
                 {:target observed
                  :descriptor descriptor
                  :public-descriptor (net/target-descriptor)})))
    (when-not (pos? (:test result 0))
      (throw
        (ex-info "portable buffer property selection was vacuous"
                 {:result result})))
    (flush)
    (System/exit (if (zero? failed) 0 1))))
