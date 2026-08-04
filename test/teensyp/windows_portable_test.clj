(ns teensyp.windows-portable-test
  "Native Windows x86-64 gate for the production TCP client.

  The pinned jolt.net has a reviewed WSAPoll backend, so this loads the
  production namespaces, opens and closes the public poller, and runs every
  client model including the real server/client loopback contract."
  (:require [clojure.test :as t]
            [jolt.net :as net]
            [teensyp.client-test]
            [teensyp.server]))

(defn- test-vars-in [namespace]
  (->> (ns-interns namespace)
       vals
       (filter #(contains? (meta %) :test))
       (sort-by #(str (:name (meta %))))))

(defn- check-public-poller-wake! []
  (let [poller (net/open-poller)]
    (try
      (let [cursor (net/wake-cursor poller)
            _ (net/wake! poller)
            waiter (future (net/await-ready poller 120000 cursor))
            ready (deref waiter 15000 ::wake-timeout)]
        (when (= ::wake-timeout ready)
          (throw
            (ex-info "Windows wake-cursor wait parked after publication"
                     {:cursor cursor
                      :watchdog-ms 15000})))
        (when-not (= [] ready)
          (throw
            (ex-info "Windows wake-cursor wait returned unexpected events"
                     {:cursor cursor
                      :ready ready}))))
      (finally
        (net/close! poller)))))

(defn -main [& _]
  (let [client-vars (test-vars-in 'teensyp.client-test)
        loopback-vars
        (filterv #(= 'public-client-loopback-contract (:name (meta %)))
                 client-vars)]
    (when-not (and (= 1 (count loopback-vars))
                   (= 14 (count client-vars)))
      (throw
        (ex-info "Windows client test inventory changed"
                 {:loopback (mapv #(-> % meta :name) loopback-vars)
                  :client-count (count client-vars)})))
    (let [poller (net/open-poller)]
      (when-not (true? (net/close! poller))
        (throw (ex-info "Windows poller did not close exactly once" {})))
      (check-public-poller-wake!)
      (t/test-vars client-vars)
      (let [failed (+ (t/n-fail) (t/n-error))]
        (when-not (= :windows (:os (jolt.host/target)))
          (throw
            (ex-info "TCP client gate did not run on Windows"
                     {:target (jolt.host/target)})))
        (when-not (= :windows (:platform (net/target-descriptor)))
          (throw
            (ex-info "jolt.net did not select its Windows descriptor"
                     {:descriptor (net/target-descriptor)})))
        (when-not (pos? (t/n-pass))
          (throw (ex-info "Windows client test selection was vacuous" {})))
        (println "native Windows TCP client gate:"
                 (count client-vars)
                 "tests," (t/n-pass) "assertions passed,"
                 (t/n-fail) "failures," (t/n-error) "errors")
        (flush)
        (System/exit (if (zero? failed) 0 1))))))
