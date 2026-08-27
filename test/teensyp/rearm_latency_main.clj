(ns teensyp.rearm-latency-main
  "Standalone entry point for the re-arm latency regression, so it can be run
  against a specific jolt.net revision on its own:

    jolt -M:rearm-latency-test

  The same namespace also runs inside the full `-M:test` suite; this alias just
  makes a single-scenario before/after comparison cheap."
  (:require [teensyp.rearm-latency-test :as rearm]))

(def ^:private failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "ok   " label)
    (do (swap! failures inc)
        (println "FAIL " label)
        (println "       expected:" (pr-str expected))
        (println "       actual:  " (pr-str actual)))))

(defn -main [& _]
  (println "reactor re-arm latency scenario")
  (let [result (rearm/run! check)]
    (println (str "  exchanges=" (:exchanges result)
                  " bytes-each=" (:bytes-each result)
                  " total-reads=" (:total-reads result)
                  " median-ms=" (:median-ms result)
                  " max-ms=" (:max-ms result)
                  " parked=" (:parked result)))
    (flush)
    (System/exit (if (pos? @failures) 1 0))))
