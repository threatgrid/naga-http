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
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]])
  (:import [java.util Date]))

(def plain-headers {"Content-Type" "text/plain"})

(def json-headers {"Content-Type" "application/json"})

(def programs (atom {}))

(defn uuid-str []
  (str (java.util.UUID/randomUUID)))

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

(defn evaluate-program [uuid s]
  (when-let [{program :program} (get @programs uuid)]
    (let [store (store/get-storage-handle {:type :memory})
          json-data (data/stream->triples store s)
          loaded-store (store/assert-data store json-data)
          axioms (store/resolve-pattern loaded-store '[?e ?p ?v])
          config {:type :memory ; TODO: type from flag+URI (store-type storage)
                  :store loaded-store}
          [store stats] (e/run config program)
          output (store/resolve-pattern store '[?e ?p ?v])]
      {:headers json-headers
       :body (json/generate-string (remove (set axioms) output))})))

(defroutes app-routes
  (POST   "/rules" request (post-program (:body request)))
  (DELETE "/rules" request (delete-programs))
  (GET    "/rules/:uuid" [uuid] (get-program uuid))
  (POST   "/rules/:uuid/eval" [uuid :as request] (evaluate-program uuid (:body request)))
  (route/not-found "Not Found"))

(def app
  (wrap-defaults
   app-routes
   (assoc-in api-defaults [:params :multipart] true)))
