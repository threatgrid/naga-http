(ns ^{:doc "Defines a ring handler that exposes the Naga rule engine as a web service."
      :author "Jesse Bouwman"}
    naga-http.server
  (:require [cheshire.core :as json]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [naga.data :as data]
            [naga.engine :as e]
            [naga.lang.pabu :as pabu]
            [naga.rules :as r]
            [naga.storage.memory.core]
            [naga.store :as store]
            [naga-http.kafka :as kafka]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]])
  (:import [java.util Date]))

(def plain-headers {"Content-Type" "text/plain"})

(def json-headers {"Content-Type" "application/json"})

(def programs (atom {}))

(def ^:const default-graph (store/get-storage-handle {:type :memory}))

(def ^:const default-store {:type :memory :store default-graph})

(def storage (atom default-store))

(defn uuid-str []
  (str (java.util.UUID/randomUUID)))

(defn register-store! [s]
  (let [g (store/get-storage-handle s)]
    (reset! storage (assoc s :store g))
    (kafka/register-graph g))
  {:headers plain-headers
   :body (:type s)})

(defn reset-store! []
  (reset! storage default-store)
  (kafka/register-graph (:store default-store))
  {:headers plain-headers
   :body "OK"})

(defn update-store! [s]
  (swap! storage assoc :store s)
  (kafka/register-graph s))

(defn registered-storage []
  (or @storage default-store))

(defn parse-program [text]
  (let [{rules :rules} (pabu/read-str text)]
    (r/create-program rules [])))

(defn install-program! [s]
  (let [uuid (uuid-str)
        text (slurp s)]
    (swap! programs assoc uuid
           {:created (Date.)
            :text text
            :program (parse-program text)})
    uuid))

(defn post-program [s]
  {:headers plain-headers
   :body (install-program! s)})

(defn get-program [uuid]
  (when-let [program (get @programs uuid)]
    {:headers json-headers
     :body (:text program)}))

(defn delete-programs []
  (reset! programs {})
  {:headers plain-headers
   :body "OK"})

(defn execute-program [program store data]
  (when program
    (let [triples-data (when data (data/stream->triples store data))
          loaded-store (store/assert-data store triples-data)
          config (assoc storage :store loaded-store)
          [store stats] (e/run config program)
          output (data/store->str store)]
      [output store])))

(defn exec-registered [uuid s]
  (when-let [program (get @programs uuid)]
    (let [storage (registered-storage)
          store (or (:store storage)
                    (store/get-storage-handle storage))
          [output new-store] (execute-program program store s)]
      (update-store! new-store)
      {:headers json-headers
       :body output})))

(defn exec-program [uuid program storage-config]
  (when-let [program (or program (get @programs uuid))]
    (let [storage (or storage-config (registered-storage))
          store (or (:store storage)
                    (store/get-storage-handle storage))
          [output new-store] (execute-program program store nil)]
      (when-not storage-config
        (update-store! new-store))
      {:headers json-headers
       :body output})))

(defroutes app-routes
  (POST   "/store" request (register-store! (:body request)))
  (DELETE "/store" request (reset-store!))
  (POST   "/rules" request (post-program (:body request)))
  (DELETE "/rules" request (delete-programs))
  (GET    "/rules/:uuid" [uuid] (get-program uuid))
  (POST   "/rules/:uuid/eval" [uuid :as request] (exec-registered uuid (:body request)))
  (POST   "/rules/:uuid/execute" [uuid :as {{:keys [program store]} :body}]
          (exec-program uuid program store))
  (route/not-found "Not Found"))

(def app
  (wrap-defaults
   app-routes
   (assoc-in api-defaults [:params :multipart] true)))

(defn init
  "Initialize the server for non-HTTP operations."
  []
  (kafka/init)
  (kafka/register-graph @storage))
