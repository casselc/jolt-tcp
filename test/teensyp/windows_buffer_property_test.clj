(ns teensyp.windows-buffer-property-test
  "Native Windows gate for the descriptor-independent buffer properties."
  (:require [clojure.test :as t]
            [jolt.net.target :as target]
            [teensyp.buffer-property-test]))

(defn- test-vars-in [namespace]
  (->> (ns-interns namespace)
       vals
       (filter #(contains? (meta %) :test))
       (sort-by #(str (:name (meta %))))))

(defn -main [& _]
  (let [observed (target/current-target)
        buffer-vars (vec (test-vars-in 'teensyp.buffer-property-test))]
    (when-not (= :windows (:os observed))
      (throw
        (ex-info "buffer property gate did not run on Windows"
                 {:target observed})))
    (when-not (= 14 (count buffer-vars))
      (throw
        (ex-info "buffer property inventory changed"
                 {:buffer-count (count buffer-vars)
                  :tests (mapv #(-> % meta :name) buffer-vars)})))
    (t/test-vars buffer-vars)
    (let [failed (+ (t/n-fail) (t/n-error))]
      (when-not (pos? (t/n-pass))
        (throw
          (ex-info "buffer property selection was vacuous" {})))
      (println "portable buffer property gate:"
               (count buffer-vars) "tests,"
               (t/n-pass) "assertions passed,"
               (t/n-fail) "failures," (t/n-error) "errors")
      (flush)
      (System/exit (if (zero? failed) 0 1)))))
