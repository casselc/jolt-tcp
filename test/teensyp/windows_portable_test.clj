(ns teensyp.windows-portable-test
  "Native Windows x86-64 gate for the portable TCP logic.

  This intentionally excludes the real-loopback test until jolt.net has a
  reviewed Windows readiness backend. It loads the production namespaces, runs
  every other client model without optional test dependencies, and requires
  the current readiness boundary to fail closed."
  (:require [clojure.test :as t]
            [jolt.net :as net]
            [teensyp.client-test]
            [teensyp.server]))

(defn- test-vars-in [namespace]
  (->> (ns-interns namespace)
       vals
       (filter #(contains? (meta %) :test))
       (sort-by #(str (:name (meta %))))))

(defn -main [& _]
  (let [client-vars (test-vars-in 'teensyp.client-test)
        loopback-vars
        (filterv #(= 'public-client-loopback-contract (:name (meta %)))
                 client-vars)
        portable-client-vars
        (filterv #(not= 'public-client-loopback-contract (:name (meta %)))
                 client-vars)]
    (when-not (and (= 1 (count loopback-vars))
                   (= 13 (count portable-client-vars)))
      (throw
        (ex-info "portable test inventory changed"
                 {:loopback (mapv #(-> % meta :name) loopback-vars)
                  :client-count (count portable-client-vars)})))
    (let [poller-result
          (try
            {:poller (net/open-poller)}
            (catch :default error {:error error}))]
      (when-let [poller (:poller poller-result)]
        (net/close! poller))
      (t/test-vars portable-client-vars)
      (let [failed (+ (t/n-fail) (t/n-error))]
        (when-not (= :windows (:os (jolt.host/target)))
          (throw
            (ex-info "portable gate did not run on Windows"
                     {:target (jolt.host/target)})))
        (when-not (= :unsupported-target
                     (:jolt.net/kind (ex-data (:error poller-result))))
          (throw
            (ex-info "Windows readiness did not fail closed"
                     poller-result)))
        (when-not (pos? (t/n-pass))
          (throw (ex-info "portable test selection was vacuous" {})))
        (println "portable TCP gate:"
                 (count portable-client-vars)
                 "tests," (t/n-pass) "assertions passed,"
                 (t/n-fail) "failures," (t/n-error) "errors")
        (flush)
        (System/exit (if (zero? failed) 0 1))))))
