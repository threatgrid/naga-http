(ns ^{:doc "Defines a ring handler that exposes the Naga rule engine as a web service."
      :author "Jesse Bouwman"}
    naga-http.server
  (:require [cheshire.core :as json]
            [compojure.core :refer :all]
            [compojure.route :as route]
            naga.cli
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]]))

(defroutes app-routes
  (POST "/eval/pabu" request
        {:headers {"Content-Type" "application/json"}
         :body (-> (:body request)
                   naga.cli/run-all
                   json/generate-string)})
  (route/not-found "Not Found"))

(def app
  (->> (assoc-in api-defaults [:params :multipart] true)
       (wrap-defaults app-routes)))
