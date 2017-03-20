(defproject jdbc "0.1.0-SNAPSHOT"
  :description "TPC-H automated testing for Postgres"
  :url "https://github.com/SanchayanMaity/clojure-tpch-postgres"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha15"]
                 [org.clojure/java.jdbc "0.7.0-alpha1"]
                 [postgresql "9.3-1102.jdbc41"]]
  :main jdbc.core
  :aot [jdbc.core])

