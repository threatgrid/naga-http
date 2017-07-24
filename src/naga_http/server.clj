(ns ^{:doc "Defines a ring handler that exposes the Naga rule engine as a web service."
      :author "Jesse Bouwman"}
    naga-http.server
  (:require [clojure.tools.logging :as log]
            [clojure.string :as s]
            [cheshire.core :as json]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [naga.data :as data]
            [naga.engine :as e]
            [naga.lang.pabu :as pabu]
            [naga.rules :as r]
            [naga.storage.memory.core]
            [naga.storage.datomic.core]
            [naga.store :as store]
            [naga-http.configuration :as c]
            [naga-http.kafka :as kafka]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.format :refer [wrap-restful-format]])
  (:import [java.util Date]
           [java.net BindException]
           [clojure.lang ExceptionInfo]))

(def plain-headers {"Content-Type" "text/plain"})

(def json-headers {"Content-Type" "application/json"})

(def default-max-threads 50)

(def default-http-port 3030)

(def programs (atom {}))

(def default-graph (store/get-storage-handle {:type :memory}))

(def default-store {:type :memory :store default-graph})

(def storage (atom default-store))

(defmacro http-response
  [& body]
  `(try
     (let [response# (or (do ~@body)
                         "OK")]
       (cond
         (map? response#)
         (if (:headers response#)
           response#
           {:headers json-headers
            :body response#})

         (sequential? response#)
         {:headers json-headers
          :body response#}

         :default
         {:headers plain-headers
          :body response#}))
     (catch ExceptionInfo ei#
       (let [{status# :status :as data#} (ex-data ei#)
             response# {:headers plain-headers
                        :body (.getMessage ei#)}]
         (assoc response# :status (or status# 500))))
     (catch Exception e#
       (.printStackTrace e#)
       {:headers plain-headers
        :status 500
        :body (.getMessage e#)})))

(defn setup-storage! [stext]
  (when stext
    (let [st (json/parse-string stext true)
          handle (store/get-storage-handle st)]
      (reset! storage (assoc st :store handle)))))

(defn uuid-str []
  (str (java.util.UUID/randomUUID)))

(defn set-store! [s]
  (http-response
   (let [g (store/get-storage-handle s)]
     (reset! storage (assoc s :store g))
     (:type s))))

(defn reset-store! []
  (http-response
   (reset! storage default-store)
   default-store))

(defn update-store! [s]
  (swap! storage assoc :store s)
  s)

(defn registered-storage
  "Storage wraps a store with extra metadata"
  []
  (or @storage default-store))

(defn registered-store
  "The connection to the implementation of the Graph protocol"
  ([] (registered-store nil))
  ([storage-config]
   (let [storage (or storage-config (registered-storage))]
     (or (:store storage)
         (store/get-storage-handle storage)))))

(defn parse-program
  [text]
  (let [{:keys [rules axioms]} (pabu/read-str text)]
    [(r/create-program rules []) axioms]))

(defn install-program!
  [s]
  (let [uuid (uuid-str)
        text (slurp s)
        [program axioms] (parse-program text)]
    (swap! programs assoc uuid
           {:created (Date.)
            :text text
            :program program
            :axioms axioms})
    uuid))

(defn post-program [s]
  (http-response (install-program! s)))

(defn get-program [uuid]
  (http-response
   (when-let [program (get @programs uuid)]
     {:headers json-headers
      :body (:text program)})))

(defn delete-programs []
  (http-response
   (reset! programs {})))

(defn get-data-type
  [{:strs [content-type]}]
  (case (s/lower-case content-type)
    "application/json" :json
    "application/edn" :edn
    :pairs))

(defn add-schema
  [header schema raw-data]
  (http-response
   (let [dtype (get-data-type header)]
     (-> (registered-store)
         (store/assert-schema-opts (or schema (slurp raw-data)) {:type dtype})
         update-store!)
     "OK")))

(defn add-data [data]
  (http-response
   (let [store (registered-store)
         triples (data/json->triples store data)]
     (-> store
         (store/assert-data triples)
         update-store!)
     "OK")))

(defn read-data
  "Reads data from a store. Takes optional query arguments, or a flag indicating raw results.
   Queries return raw data. If no query is specified, then return everything.
   If returning everything, then setting raw will return the triples directly from the graph
   store. Otherwise a JSON representation is returned."
  [{:strs [select where raw] :as query}]
  (http-response
   (let [store (registered-store)]
     (if select
       (store/query store select where)
       (if raw
         (store/retrieve-contents store)
         (data/store->json store))))))

(defn execute-program [program axioms store data]
  (when program
    (let [initialized-store (if (seq axioms) (store/assert-data store axioms) store)
          triples-data (when data (data/stream->triples initialized-store data))
          loaded-store (if (seq triples-data)
                         (store/assert-data initialized-store triples-data)
                         initialized-store)
          config (assoc @storage :store loaded-store)
          [store stats] (e/run config program)
          output (data/store->str store)]
      [output store])))

(defn exec-registered [uuid s]
  (http-response
   (if-let [{:keys [program axioms]} (get @programs uuid)]
     (let [store (registered-store)
           [output new-store] (execute-program program axioms store s)]
       (update-store! new-store)
       {:headers json-headers
        :body output})
     {:status 404 :body (str "Program " uuid " not found")})))

(defn exec-program [uuid program-text storage-config]
  (http-response
   (let [[program axioms] (if program-text
                           (parse-program program-text)
                           (let [{:keys [program axioms]} (get @programs uuid)]
                             [program axioms]))]
     (let [store (registered-store storage-config)
           [output new-store] (execute-program program axioms store nil)]
       (when-not storage-config
         (update-store! new-store))
       {:headers json-headers
        :body output}))))

(defn test-post
  [data]
  (let [d (slurp data)]
    (print "POSTED: ")
    (clojure.pprint/pprint d)))

(defroutes app-routes
  (POST   "/store" [:as {dbconfig :body-params}] (set-store! dbconfig))
  (DELETE "/store" request (reset-store!))

  (POST   "/store/schema" [:as {headers :headers schema :body-params raw-data :body}]
          (add-schema headers schema raw-data))

  (POST   "/store/data" [:as {data :body-params}] (add-data data))
  (GET    "/store/data" [:as {params :params :as request}]
          (read-data params))

  (POST "/store/test" [:as {raw :body}]
        (test-post raw))

  (POST   "/rules" [:as {body :body}] (post-program body))
  (DELETE "/rules" request (delete-programs))
  (GET    "/rules/:uuid" [uuid] (get-program uuid))
  (POST   "/rules/:uuid/eval" [uuid :as {body :body}] (exec-registered uuid body))
  (POST   "/rules/:uuid/execute" [uuid :as {{:keys [program store]} :body-params}]
          (exec-program uuid program store))
  (route/not-found "Not Found"))

(def app
  (-> app-routes
      (wrap-restful-format :formats [:json-kw :edn])
      (wrap-params)))

(let [initialized? (promise)]
  (defn init
    "Initialize the server for non-HTTP operations."
    []
    (when-not (realized? initialized?)
      (c/init!)
      (let [{{{graph :graph} :naga
              {topic :topic} :kafka} :naga-http :as properties} @c/properties]
        (clojure.pprint/pprint @c/properties)
        (setup-storage! graph)
        (kafka/init topic)
        (kafka/set-storage storage))
      (deliver initialized? true))))

;; This is here so initialization happens with: lein ring server
(init)

(defn start-server
  "Runs the routes on the provided port"
  [port]
  (jetty/run-jetty app {:port port}))

(defn -main
  "Entry point for the program"
  []
  (try
    (init)
    (start-server (get-in @c/properties [:naga-http :port] default-http-port))
    (catch BindException e
      (log/error "\nServer port already in use")
      (println "\nServer port already in use"))
    (catch ExceptionInfo i
      (log/error (.getMessage i))
      (println (.getMessage i)))))
