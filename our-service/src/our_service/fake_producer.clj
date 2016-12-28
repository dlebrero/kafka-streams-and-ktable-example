(ns our-service.fake-producer
  (:require
    [franzy.serialization.serializers :as serializers]
    [franzy.clients.producer.client :as client]
    [franzy.clients.producer.protocols :as producer]
    [compojure.core :refer [routes ANY GET POST]]
    [clojure.tools.logging :as log])
  (:use ring.middleware.params))

(defn for-ever
  [thunk]
  (loop []
    (if-let [result (try
                      [(thunk)]
                      (catch Exception e
                        (println e)
                        (Thread/sleep 100)))]
      (result 0)
      (recur))))

(def kafka-client (delay
                    (client/make-producer {:bootstrap.servers "kafka1:9092"
                                           :acks              "all"
                                           :retries           1
                                           :client.id         "example-producer"}
                                          (serializers/keyword-serializer)
                                          (serializers/edn-serializer))))

(defn produce-edn [m]
  (log/info "Producing" m)
  (for-ever
    #(producer/send-sync! @kafka-client m)))

(defn update-share-holder [client ticker exchange amount]
  (let [kafka-key (str client ":::" ticker)]
    (if (zero? amount)
      (produce-edn {:topic "share-holders"
                    :key   kafka-key
                    :value nil})
      (produce-edn {:topic "share-holders"
                    :key   kafka-key
                    :value {:client   client
                            :id       kafka-key
                            :ticker   ticker
                            :exchange exchange
                            :amount   amount}}))))

(defn api [us-share-holders]
  (routes
    (POST "/set-shares" [client ticker exchange amount]
      (update-share-holder client ticker exchange (Integer/parseInt amount))
      {:status 200
       :body   (pr-str "done!")})
    (GET "/local-state" []
      {:status 200
       :body   (pr-str (us-share-holders))})))

(comment

  (update-share-holder "daniel" "AAPL" "NASDAQ" 99)
  (update-share-holder "daniel" "BT.A" "LON" 1)
  (update-share-holder "daniel" "AAPL" "NASDAQ" 0)

  )
