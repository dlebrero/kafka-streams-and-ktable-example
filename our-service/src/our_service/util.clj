(ns our-service.util
  (:require [clojure.tools.logging :as log]
            [franzy.admin.topics :as topics]
            [franzy.admin.zookeeper.client :as client])
  (:import (org.apache.kafka.streams.kstream Reducer KeyValueMapper Predicate)
           (java.net Socket)))

(defmacro reducer [kv & body]
  `(reify Reducer
     (apply [_# ~(first kv) ~(second kv)]
       ~@body)))

(defmacro kv-mapper [kv & body]
  `(reify KeyValueMapper
     (apply [_# ~(first kv) ~(second kv)]
       ~@body)))

(defmacro pred [kv & body]
  `(reify Predicate
     (test [_# ~(first kv) ~(second kv)]
       ~@body)))


(defn for-ever
  [msg time thunk]
  (loop []
    (if-let [result (try
                      [(thunk)]
                      (catch Exception e
                        (log/info msg)
                        (log/debug e msg)
                        (Thread/sleep time)))]
      (result 0)
      (recur))))

(defn wait-for-kafka [host port]
  (for-ever "waiting for kafka" 3000
    #(with-open [_ (Socket. host (int port))]
      true)))

(defn wait-for-topic [topic]
  (for-ever "waiting for topics" 3000
    #(let [config {:servers                 ["zoo1:2181"]
                   :connection-timeout      30000
                   :operation-retry-timeout 10}]
      (with-open [zk (client/make-zk-utils config true)]
        (when-not (some #{topic} (topics/all-topics zk))
          (log/info "Topic" topic "not in" (topics/all-topics zk))
          (throw (RuntimeException.)))))))