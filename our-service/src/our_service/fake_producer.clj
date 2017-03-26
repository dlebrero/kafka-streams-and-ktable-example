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

(defn set-ref-data [ticker name exchange]
  (produce-edn {:topic "shares-ref-data"
                :key   ticker
                :value {:name     name
                        :exchange exchange}}))

(defn set-client-data [client email]
  (produce-edn {:topic "client-data"
                :key   client
                :value {:client client
                        :email  email}}))

(defn update-share-holder [position-id client ticker amount]
  (if (zero? amount)
    (produce-edn {:topic "share-holders"
                  :key   position-id
                  :value nil})
    (produce-edn {:topic "share-holders"
                  :key   position-id
                  :value {:client client
                          :id     position-id
                          :ticker ticker
                          :amount amount}})))

(defn api [us-share-holders]
  (routes
    (POST "/set-shares" [position client ticker amount]
      (update-share-holder position client ticker (Integer/parseInt amount))
      {:status 200
       :body   (pr-str "done!")})
    (POST "/set-ref-data" [ticker name exchange]
      (set-ref-data ticker name exchange)
      {:status 200
       :body   (pr-str "done!")})
    (POST "/set-client-data" [client email]
      (set-client-data client email)
      {:status 200
       :body   (pr-str "done!")})
    (GET "/local-state" []
      {:status 200
       :body   (pr-str (us-share-holders))})))

(comment

  (do
    (set-ref-data "AAPL" "Apple Inc." "NASDAQ")
    (set-ref-data "FB" "Facebook Inc." "NASDAQ")
    (set-ref-data "AMZN" "Amazon Inc." "NASDAQ")
    (set-ref-data "VOD" "Vodafone Group PLC" "LON")
    (set-ref-data "BT.A" "BT Group" "LON"))

  (set-client-data "daniel" "dlebrero@gmail.com")

  (update-share-holder "position1" "daniel" "VOD" 2)
  (update-share-holder "position2" "daniel" "AAPL" 2)
  (update-share-holder "position3" "daniel" "FB" 20)
  (update-share-holder "position4" "daniel" "FB" 10)

  (update-share-holder "position3" "daniel" "FB" 0)
  (update-share-holder "position4" "daniel" "VOD" 20)

  )
