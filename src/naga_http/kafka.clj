(ns ^{:doc "Provides functionality for consuming Kafka events and storing
            them in the registered graph"
      :author "Paula Gearon"}
  naga-http.kafka
  (:require [naga-http.configuration :as c]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [franzy.clients.consumer.client :as consumer]
            [franzy.clients.consumer.protocols :as protocols]
            [franzy.clients.consumer.defaults :as cd]
            [franzy.serialization.deserializers :as deserializers]
            [naga.data :as naga-data]
            [naga.store :as naga-store]))

(def ^:const default-max-errors 20)

(def ^:const default-poll-timeout 100)

(defn get-servers
  []
  (let [{:keys [host port]} (get-in @c/properties [:naga-http :kafka])]
    (str host ":" port)))

;; A write-once atom referencing an atom
(def storage-atom (atom nil))

(defn register-storage
  "Registers the atom to load data into. This avoids referencing the server atom."
  [s]
  (reset! storage-atom s))

(def shutdown? (promise))

(defn start-service
  [f]
  (.start (Thread. f)))

(defmacro do-at-shutdown
  [& body]
  `(.addShutdownHook (Runtime/getRuntime)
                     (Thread. (fn [] ~@body))))

(defn load-data
  "Loads a string (containing JSON) into a graph and updates the server storage to use this graph."
  [text]
  (letfn [(store-update [{s :store :as storage}]
            (let [{:keys [entity] :as json-data} (json/parse-string text true)]
              (if entity
                (let [ graph-data (naga-data/json->triples s [entity])
                      new-graph (naga-store/assert-data s graph-data)]
                  (assoc storage :store new-graph))
                (do
                  (log/info "Unexpected data for topic: " text)
                  storage))))]
    (try
      ;; Dereferencing to get the atom in the server
      (swap! @storage-atom store-update)
      (catch Exception e
        (log/error "Error processing data from Kafka" e)))))

(defn init
  "Initialize the Kafka listener"
  [topic]
  (log/debug "Initializing Kafka")
  (do-at-shutdown (deliver shutdown? true))
  (let [topic (get-in @c/properties [:naga-http :kafka :topic] topic)
        pc {:bootstrap.servers (get-servers)
            :auto.offset.reset :latest}
        poll-timeout (get-in @c/properties [:naga-http :kafka :poll] default-poll-timeout)
        max-errors (get-in @c/properties [:naga-http :kafka :max-errors] default-max-errors)
        key-deserializer (deserializers/string-deserializer)
        value-deserializer (deserializers/string-deserializer)
        opts (cd/make-default-consumer-options)
        topic-partitions [{:topic topic :partition 0}]
        c (consumer/make-consumer pc key-deserializer value-deserializer opts)]
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
                                (load-data v))
                              err-count))
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

