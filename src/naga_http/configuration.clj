(ns ^{:doc "Defines the configuration for http-naga"
      :author "Paula Gearon"}
  naga-http.configuration
  (:require [schema.core :as s]
            [schema-tools.core :as st]
            [clj-momo.properties :as mp]
            [clj-momo.lib.schema :as mls]))

(def files
  "Property file names. The contents will be merged, with latter ones overriding."
  ["server-default.properties"
   "server.properties"])

(defonce properties (atom {}))

(s/defschema PropertiesSchema
  (st/required-keys {"naga-http.kafka.topic" s/Str
                     "naga-http.kafka.host" s/Str
                     "naga-http.kafka.port" s/Int
                     "naga-http.kafka.poll" s/Int}))

(def configurable-properties (mls/keys PropertiesSchema))

(def init! (mp/build-init-fn files PropertiesSchema properties))

