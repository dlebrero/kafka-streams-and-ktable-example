(ns our-service.main
  (:require
    [clojure.tools.nrepl.server :as nrepl]
    [our-service.util :as k]
    [our-service.kafka-streams :as kafka-streams]
    [our-service.fake-producer :as fake-producer]
    [ring.adapter.jetty :as jetty]
    [clojure.tools.logging :as log])
  (:use ring.middleware.params)
  (:gen-class))

(def state (atom {}))

(defn -main [& args]
  (nrepl/start-server :port 3002 :bind "0.0.0.0")
  (log/info "Waiting for kafka to be ready")
  (k/wait-for-kafka "kafka1" 9092)
  (log/info "Waiting for topics to be created")
  (k/wait-for-topic "share-holders-with-ref-data")
  (Thread/sleep 5000)
  (log/info "Starting Kafka Streams")
  (let [[kstream us-share-holders] (kafka-streams/start-kafka-streams)
        web (fake-producer/api us-share-holders)]
    (reset! state {:us-shares us-share-holders
                   :kstream   kstream
                   :web       web
                   :jetty     (jetty/run-jetty
                                (wrap-params (fake-producer/api us-share-holders))
                                {:port  80
                                 :join? false})})))

