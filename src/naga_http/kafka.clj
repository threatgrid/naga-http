(ns ^{:doc "Provides functionality for consuming and producing Kafka events
            and storing them in the registered graph"
      :author "Paula Gearon"}
  naga-http.kafka
  (:require [naga-http.configuration :as c]
            [naga-http.publish :as p]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [franzy.clients.consumer.client :as consumer]
            [franzy.clients.consumer.protocols :as protocols]
            [franzy.clients.consumer.defaults :as cd]
            [franzy.serialization.deserializers :as deserializers]
            [franzy.clients.producer.protocols :as kafka]
            [franzy.clients.producer.client :as producer]
            [franzy.clients.producer.defaults :as pd]
            [franzy.serialization.serializers :as serializers]
            [clients.core :as client]
            [naga.data :as naga-data]
            [naga.store :as naga-store]))

(def ^:const default-max-errors 20)

(def ^:const default-poll-timeout 100)

(def production-key "updates")

(defn get-servers
  []
  (let [{:keys [host port]} (get-in @c/properties [:naga-http :kafka])]
    (str host ":" port)))

(defn start-service
  [f]
  (.start (Thread. f)))

(defmacro do-at-shutdown
  [& body]
  `(.addShutdownHook (Runtime/getRuntime)
                     (Thread. (fn [] ~@body))))

(defn load-data
  "Loads a string (containing JSON) into a graph and updates the server storage to use this graph."
  [storage-atom text]
  (letfn [(store-update [{s :store :as storage}]
            (let [{:keys [entity] :as json-data} (json/parse-string text true)]
              (if entity
                (let [graph-data (naga-data/json->triples s [entity])
                      new-graph (naga-store/assert-data s graph-data)]
                  (assoc storage :store new-graph))
                (do
                  (log/info "Unexpected data for topic: " text)
                  storage))))]
    (try
      ;; Dereferencing to get the atom in the server
      (swap! storage-atom store-update)
      (catch Exception e
        (log/error "Error processing data from Kafka" e)))))

(defn start-listening-service
  "Builds a listening service and starts it."
  [storage config topic partition shutdown?]
  (let [poll-timeout (get-in @c/properties [:naga-http :kafka :poll] default-poll-timeout)
        max-errors (get-in @c/properties [:naga-http :kafka :max-errors] default-max-errors)
        key-deserializer (deserializers/string-deserializer)
        value-deserializer (deserializers/string-deserializer)
        opts (cd/make-default-consumer-options)
        topic-partitions [{:topic topic :partition partition}]
        c (consumer/make-consumer config key-deserializer value-deserializer opts)]

    ;; create the listening service
    (try
      (protocols/assign-partitions! c topic-partitions)
      (protocols/seek-to-beginning-offset! c topic-partitions)
      (log/debug "Initializing Kafka service")
      (start-service
       (fn []
         (log/debug "Listening to Kafka topic")
         (try
           (loop [err-count 0]
             (let [errs (try
                          (let [cr (protocols/poll! c poll-timeout)]
                            (when-not (realized? shutdown?)
                              (log/debug "Kafka message")
                              (doseq [{v :value} (protocols/records-by-topic cr topic)]
                                (log/info v)
                                (load-data storage v))
                              err-count))
                          ;; TODO: treat local exceptions as retryable/fatal
                          ;;       and protocol exceptions as retryable with long delay
                          (catch Exception e
                            (log/error "Exception in Kafka: " e)
                            (if (< err-count max-errors)
                              (inc err-count)
                              (log/error "Kafka service exceeded maximum errors. Exiting."))))]
               (when errs (recur errs))))
           (finally (.close c)))))
      ;; if something went wrong starting the thread, then clean up
      (catch Exception e
        (log/error "Error while setting up Kafka" e)
        (.close c)))))

(defrecord KafkaPublisher [producer topic partition options]
  p/Publisher
  (publish [_ data]
    (let [data-json (json/encode data)]
      (log/info (str "Publishing JSON Delta to Kafka"))
      (log/debug (str "Published JSON Delta to Kafka: " data-json))
      (client/retry-exp
       "send to Kafka"
       (kafka/send-async! producer topic partition production-key data-json options)))))


(defn init
  "Initialize the Kafka listener"
  [storage]

  (log/debug "Initializing Kafka")

  (let [{:keys [publish topic partition security truststore keystore password]}
        (get-in @c/properties [:naga-http :kafka])

        config (cond-> {:bootstrap.servers (get-servers)
                        :auto.offset.reset :latest}
                 security (assoc :security.protocol (str/upper-case security))
                 truststore (assoc :ssl.truststore.location truststore)
                 keystore (assoc :ssl.keystore.location keystore)
                 password (assoc :ssl.truststore.password password))
        shutdown? (promise)]

    (do-at-shutdown (deliver shutdown? true))
    (start-listening-service storage config topic partition shutdown?)

    ;; return the publisher
    (when publish
      (log/info "Configuring Kafka publishing")
      (let [options (pd/make-default-producer-options)
            key-ser (serializers/string-serializer)
            val-ser (serializers/string-serializer)
            producer (producer/make-producer config key-ser val-ser options)]
        (->KafkaPublisher producer topic partition options)))))
