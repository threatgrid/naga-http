(defproject naga-http "0.1.0-SNAPSHOT"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojars.quoll/naga "0.2.0-SNAPSHOT"]
                 [org.clojure/tools.logging "0.4.0"]
                 [log4j "1.2.17"]
                 [threatgrid/clj-momo "0.2.9"]
                 [prismatic/schema "1.1.3"]
                 [ymilky/franzy "0.0.1"]
                 [compojure "1.5.2"]
                 [ring/ring-core "1.6.1"]
                 [ring/ring-defaults "0.3.0"]
                 [ring/ring-jetty-adapter "1.6.1"]
                 [ring-middleware-format "0.7.2"]]
  :resource-paths ["resources"]
  :classpath ".:resources"
  :plugins [[lein-ring "0.9.7"]]
  :ring {:nrepl {:start? true}
         :handler naga-http.server/app}
  :main naga-http.server
  :profiles
  {:dev {:dependencies [[javax.servlet/servlet-api "2.5"]
                        [ring/ring-mock "0.3.0"]]}})
