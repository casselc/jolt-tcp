(ns teensyp.windows-arm64-preview
  "Native ARM64 source-mode preview for descriptor-independent TCP code."
  (:require [clojure.test :as t]
            [jolt.net.target :as target]
            [teensyp.buffer-property-test]))

(defn -main [& _]
  (let [observed (target/current-target)
        descriptor-error (try
                           (target/descriptor observed)
                           nil
                           (catch :default cause cause))
        result (t/run-tests 'teensyp.buffer-property-test)
        failed (+ (:fail result 0) (:error result 0))]
    (when-not (= [:windows :aarch64 64]
                 [(:os observed) (:arch observed) (:pointer-bits observed)])
      (throw
        (ex-info "portable TCP preview did not run on native Windows ARM64"
                 {:target observed})))
    (when-not (= :unsupported-target
                 (:jolt.net/kind (ex-data descriptor-error)))
      (throw
        (ex-info "jolt.net did not fail closed before its ARM64 descriptor"
                 {:target observed :error descriptor-error})))
    (when-not (pos? (:test result 0))
      (throw
        (ex-info "portable buffer property selection was vacuous"
                 {:result result})))
    (flush)
    (System/exit (if (zero? failed) 0 1))))
