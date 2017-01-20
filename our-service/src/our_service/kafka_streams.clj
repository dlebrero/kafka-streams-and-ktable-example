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
           (org.apache.kafka.streams.kstream KStreamBuilder KStream KTable ForeachAction)
           (org.apache.kafka.streams.state QueryableStoreTypes)
           (org.apache.kafka.streams.processor Processor ProcessorSupplier ProcessorContext)
           (org.apache.kafka.streams.kstream.internals KTableSource KStreamImpl ChangedSerializer ChangedDeserializer Change)
           (org.apache.kafka.streams.state.internals RocksDBKeyValueStoreSupplier)))

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
    (.put StreamsConfig/APPLICATION_ID_CONFIG (str "example-consumer" (System/currentTimeMillis)))
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

(comment

  (def ^KStreamBuilder builder (KStreamBuilder.))

  (.addSource builder "the-original-source" (into-array ["share-holders"]))

  (def ktable-source (KTableSource. "the-original-source-store"))

  (.enableSendingOldValues ktable-source)

  (.addProcessor builder
                 "forward old and new"
                 ktable-source
                 (into-array ["the-original-source"]))

  (def ss (RocksDBKeyValueStoreSupplier. "the-original-source-store"
                                         nil nil
                                         false {}
                                         true))

  (.addStateStore builder ss (into-array ["forward old and new"]))

  (def to-stream-b (.newName builder "KTABLE-TOSTREAM-"))

  (.addProcessor builder
                 to-stream-b
                 (reify ProcessorSupplier
                   (get [this]
                     (let [ctx (volatile! nil)]
                       (reify Processor
                         (init [this context]
                           (vreset! ctx context))
                         (process [this key change]
                           (let [new (.newValue change)
                                 old (.oldValue change)
                                 new-key (:ticker new)
                                 old-key (:ticker old)]
                             (doseq [msg (condp = [new old]
                                           [nil nil] nil
                                           [new nil] [{:key new-key :val {:add new}}]
                                           [nil old] [{:key old-key :val {:delete-and-forward-delete old}}] ;; we do not need the whole old value, just enough to build the key
                                           [new old] (if (= new-key old-key) ;; comparing partitions would be more efficient
                                                       [{:key new-key :val {:add new}}]
                                                       [{:key old-key :val {:delete-but-do-not-forward old}} ;; order is important
                                                        {:key new-key :val {:add new}}]))]
                               (.forward ^ProcessorContext @ctx (:key msg) (:val msg)))))
                         (punctuate [this timestamp])
                         (close [this])))))
                 (into-array ["forward old and new"]))

  (.addSink builder "sink"
            (str "share-holders" KStreamImpl/REPARTITION_TOPIC_SUFFIX)
            (into-array [to-stream-b]))
  (.addInternalTopic builder (str "share-holders" KStreamImpl/REPARTITION_TOPIC_SUFFIX))


  ;;;; reading

  (def repartition-source (str "share-holders" KStreamImpl/REPARTITION_TOPIC_SUFFIX))
  (def repartition-source-store (str repartition-source "-store"))
  (.addSource builder repartition-source (into-array [repartition-source]))

  (.addProcessor builder
                 "process new and old"
                 (reify ProcessorSupplier
                   (get [this]
                     (let [ctx (volatile! nil)
                           store (volatile! nil)]
                       (reify Processor
                         (init [this context]
                           (vreset! ctx context)
                           (vreset! store (.getStateStore context repartition-source-store)))
                         ;; tupleForwarder = new TupleForwarder<>(store, context, new ForwardingCacheFlushListener<K, V>(context, sendOldValues), sendOldValues);

                         (process [this key command]
                           (let [v (second (first command))
                                 cmd (ffirst command)
                                 mixed-key (str key "|" (:id v))]
                             (condp = cmd
                               :add (do
                                      (.put @store mixed-key joined)
                                      (.forward ^ProcessorContext @ctx (:id v) joined) ;; cache?
                                      )
                               :delete-and-forward-delete
                               (do
                                 (.put @store mixed-key nil)
                                 (.forward ^ProcessorContext @ctx (:id v) nil)) ;; cache?
                               :delete-but-do-not-forward
                               (do
                                 (.put @store mixed-key nil)
                                 ;(.forward ^ProcessorContext @ctx (:id v) nil)
                                 )                          ;; cache??
                               )))
                         (punctuate [this timestamp])
                         (close [this])))))
                 (into-array [repartition-source]))

  (def ss (RocksDBKeyValueStoreSupplier. "the-original-source-store"
                                         nil nil
                                         false {}
                                         true))

  (.addStateStore builder ss (into-array ["forward old and new"]))


  (def poo-stream (.stream builder
                           (into-array [(str "share-holders" KStreamImpl/REPARTITION_TOPIC_SUFFIX)])))

  (.print poo-stream)

  (def ks (KafkaStreams. builder (kafka-config)))

  (.start ks)

  (.close ks)

  )
)