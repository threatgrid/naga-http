(ns ^{:doc "Provides functionality for consuming Kafka events and storing
            them in the registered graph"
      :author "Paula Gearon"}
  naga-http.kafka
  (:require [franzy.clients.consumer.client :as consumer]
            [franzy.clients.consumer.protocols :as protocols]
            [franzy.clients.consumer.defaults :as cd]
            [franzy.serialization.deserializers :as deserializers]
            [naga.data :as naga-data]
            [naga.store :as naga-store]))

(defn get-servers
  []
  ;; FIXME: read from properties file
  "localhost:9092")

(def default-topic "naga")

(def poll-timeout 100)

(def graphs (atom []))

(def shutdown? (promise))

(defn start-service
  [f]
  (.start (Thread. f)))

(defmacro do-at-shutdown
  [& body]
  `(.addShutdownHook (Runtime/getRuntime)
                     (Thread. (fn [] ~@body))))

(defn load-data
  "Loads a string (containing JSON) into a graph."
  [text]
  (reset! graphs
    (map (fn [g]
           (let [graph-data (naga-data/string->triples g text)]
             (naga-store/assert-data g graph-data)))
         @graphs)))

(defn init
  "Initialize the Kafka listener"
  [topic]
  (do-at-shutdown (deliver shutdown? true))
  (let [topic (or topic default-topic)
        pc {:bootstrap.servers (get-servers)
            :auto.offset.reset :latest}
        key-deserializer (deserializers/string-deserializer)
        value-deserializer (deserializers/string-deserializer)
        opts (cd/make-default-consumer-options)
        topic-partitions [{:topid topic :partition 0}]
        c (consumer/make-consumer pc key-deserializer value-deserializer opts)]
    (try
      (protocols/assign-partitions! c topic-partitions)
      (protocols/seek-to-beginning-offset! c topic-partitions)
      (start-service
        (fn []
          (try
            (loop [cr (protocols/poll! c poll-timeout)]
              (when-not (realized? shutdown?)
                (doseq [{v :value} (protocols/records-by-topic cr topic)]
                  (load-data v))
                (recur (protocols/poll! c poll-timeout))))
            (finally (.close c)))))
      ;; if something went wrong starting the thread, then clean up
      (catch Exception _ (.close c)))))

(defn register-graph
  "Registers this graph to have kafka events loaded into it"
  [graph]
  (swap! graphs conj graph))

