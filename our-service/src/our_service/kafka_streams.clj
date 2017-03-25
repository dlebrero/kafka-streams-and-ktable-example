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
    (.put StreamsConfig/APPLICATION_ID_CONFIG "example-consumer")
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka1:9092")
    (.put StreamsConfig/ZOOKEEPER_CONNECT_CONFIG "zoo1:2181")
    (.put StreamsConfig/CACHE_MAX_BYTES_BUFFERING_CONFIG 0)
    (.put StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 100000)
    (.put StreamsConfig/KEY_SERDE_CLASS_CONFIG (class (Serdes/String)))
    (.put StreamsConfig/VALUE_SERDE_CLASS_CONFIG EdnSerde)
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")))

(defn forward-to-old-key-if-nil-processor []
  (reify ProcessorSupplier
    (get [_]
      (let [ctx (volatile! nil)]
        (reify Processor
          (init [_ context]
            (vreset! ctx context))
          (process [_ key change]
            (let [new-value (.newValue change)
                  old-value (.oldValue change)
                  new-key (:ticker new-value)
                  old-key (:ticker old-value)]
              ;; Not very efficient
              (let [msg (condp = [new-value old-value]
                          [nil nil] nil
                          [new-value nil] {:key new-key :val new-value}
                          [nil old-value] {:key old-key :val nil}
                          [new-value old-value] {:key new-key :val new-value})]
                ;; Note that we wrap the data to forward
                (.forward ^ProcessorContext @ctx (:key msg)
                          {:previous-key key
                           :val          (:val msg)})))
            (.commit @ctx))
          (punctuate [_ timestamp])
          (close [_]))))))

(defn store-original-share-holder-and-forward [^KStreamBuilder builder]
  ;; Mostly copied from org.apache.kafka.streams.kstream.KStreamBuilder#table
  (.addSource builder "the-original-source" (into-array ["share-holders"]))
  (let [ktable-source (doto
                        (KTableSource. "the-original-source-store")
                        (.enableSendingOldValues))
        original-store (RocksDBKeyValueStoreSupplier. "the-original-source-store" nil nil false {} true)
        forward-to-old-key (.newName builder "KTABLE-TOSTREAM-")
        forward-old-and-new-processor "forward old and new"
        repartition-sink (str "share-holders" KStreamImpl/REPARTITION_TOPIC_SUFFIX)]
    (.addProcessor builder
                   forward-old-and-new-processor
                   ktable-source
                   (into-array ["the-original-source"]))
    (.addStateStore builder original-store (into-array [forward-old-and-new-processor]))
    (.addProcessor builder
                   forward-to-old-key
                   (forward-to-old-key-if-nil-processor)
                   (into-array [forward-old-and-new-processor]))
    (.addSink builder "sink" repartition-sink (into-array [forward-to-old-key]))
    (.addInternalTopic builder repartition-sink)
    repartition-sink))

(defn joiner [ref-data-store-name]
  (reify ProcessorSupplier
    (get [_]
      (let [ctx (volatile! nil)
            ref-data-store (volatile! nil)]
        (reify Processor
          (init [_ context]
            (vreset! ctx context)
            (vreset! ref-data-store (.getStateStore context ref-data-store-name)))
          (process [_ key {:keys [previous-key val] :as v}]
            (log/info "joining" key v)
            (if-not val
              ;; No need to join deletes
              (.forward @ctx previous-key nil)
              (if-not key
                ;; "should not happen"
                (.forward @ctx previous-key val)
                ;; Do join
                (let [ref-data (.get @ref-data-store key)]
                  (.forward @ctx previous-key (assoc val :exchange (:exchange ref-data))))))
            (.commit @ctx))
          (punctuate [_ timestamp])
          (close [_]))))))

(defn join [^KStreamBuilder builder input-topic ref-data-store-name output-topic]

  (let [repartition-source (str input-topic "-source")]
    (.addSource builder repartition-source (into-array [input-topic]))
    (.addProcessor builder
                   "joiner"
                   (joiner ref-data-store-name)
                   (into-array [repartition-source]))
    (.addSink builder "sink-join" output-topic (into-array ["joiner"]))
    (.connectProcessorAndStateStores builder "joiner" (into-array [ref-data-store-name]))))

;;;
;;; Create topology, but do not start it
;;;
(defn create-kafka-stream-topology []
  (let [^KStreamBuilder builder (KStreamBuilder.)
        ref-data (.table builder "shares-ref-data" "shares-ref-data-store")
        repartition-topic (store-original-share-holder-and-forward builder)
        _ (join builder repartition-topic "shares-ref-data-store" "share-holders-with-ref-data")

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