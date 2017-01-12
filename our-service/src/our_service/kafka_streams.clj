(ns our-service.kafka-streams
  (:require
    [our-service.util :as k]
    [franzy.serialization.deserializers :as deserializers]
    [franzy.serialization.serializers :as serializers]
    [clojure.set :as set]
    [clojure.tools.logging :as log])
  (:gen-class)
  (:import (java.util Properties)
           (org.apache.kafka.streams StreamsConfig KafkaStreams KeyValue)
           (org.apache.kafka.common.serialization Serde Serdes)
           (org.apache.kafka.clients.consumer ConsumerConfig)
           (org.apache.kafka.streams.kstream KStreamBuilder KStream)
           (org.apache.kafka.streams.state QueryableStoreTypes)))

;;;
;;; Serialization stuff
;;;

;; Can be global as they are thread-safe
(def serializer (serializers/edn-serializer))
(def deserializer (deserializers/edn-deserializer))

(deftype EdnSerde []
  Serde
  (configure [this map b])
  (close [this])
  (serializer [this]
    serializer)
  (deserializer [this]
    deserializer))

;;;
;;; Application
;;;

(defn kafka-config []
  (doto
    (Properties.)
    (.put StreamsConfig/APPLICATION_ID_CONFIG "example-consumer")
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka1:9092")
    (.put StreamsConfig/ZOOKEEPER_CONNECT_CONFIG "zoo1:2181")
    (.put StreamsConfig/CACHE_MAX_BYTES_BUFFERING_CONFIG 0)
    (.put StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 100000)
    (.put StreamsConfig/KEY_SERDE_CLASS_CONFIG (class (Serdes/String)))
    (.put StreamsConfig/VALUE_SERDE_CLASS_CONFIG EdnSerde)
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")))

;;;
;;; Create topology, but do not start it
;;;
(defn create-kafka-stream-topology []
  (let [^KStreamBuilder builder (KStreamBuilder.)
        ref-data (.table builder "shares-ref-data" "shares-ref-data-store")
        ^KStream x (.stream builder (into-array ["share-holders"]))
        [nils no-nils]
        (.branch x (into-array org.apache.kafka.streams.kstream.Predicate
                               [(k/pred [key value]
                                  (log/info "First test" key value)
                                  (nil? value))
                                (k/pred [key value]
                                  (log/info "Second test" key value)
                                  true)]))
        _
        (.to nils "share-holders-with-ref-data")
        _
        (-> no-nils
            (.selectKey (k/kv-mapper [key value]
                          (log/info "SelectKey" key value)
                          (:ticker value)))
            (.leftJoin ref-data (k/val-joiner [holder ref-info]
                                              (log/info "joining" holder "with" ref-info)
                                              (assoc holder :exchange (:exchange ref-info))))
            (.selectKey (k/kv-mapper [key value]
                          (log/info "Re SelectKey" key value)
                          (:id value)))
            (.to "share-holders-with-ref-data"))

        us-share-holders
        (->
          (.table builder "share-holders-with-ref-data" "share-holders-store")
          (.filter (k/pred [key position]
                     (log/info "Filtering" key position)
                     (= "NASDAQ" (:exchange position))))
          (.groupBy (k/kv-mapper [key position]
                      (log/info "Grouping" key position)
                      (KeyValue/pair (:client position)
                                     #{(:id position)})))
          (.reduce (k/reducer [value1 value2]
                     (log/info "adding" value1 value2)
                     (set/union value1 value2))
                   (k/reducer [value1 value2]
                     (log/info "removing" value1 value2)
                     (let [result (set/difference value1 value2)]
                       (when-not (empty? result)
                         result)))
                   "us-share-holders"))]
    [builder us-share-holders]))

(defn get-all-in-local-store [kafka-streams]
  (fn []
    (with-open [all (.all (.store kafka-streams "us-share-holders" (QueryableStoreTypes/keyValueStore)))]
      (doall
        (map (fn [x] {:key   (.key x)
                      :value (.value x)})
             (iterator-seq all))))))

(defn start-kafka-streams []
  (let [[builder us-share-holders] (create-kafka-stream-topology)
        kafka-streams (KafkaStreams. builder (kafka-config))]
    (.print us-share-holders)
    (.start kafka-streams)
    [kafka-streams (get-all-in-local-store kafka-streams)]))