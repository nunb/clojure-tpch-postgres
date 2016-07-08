(defproject jdbc "0.1.0-SNAPSHOT"
  :description "CS425: JDBC Homework 3. Sanchayan Maity A20340174"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.6.1"]
                 [postgresql "9.3-1102.jdbc41"]
                 [com.mchange/c3p0 "0.9.5.2"]]
  :main jdbc.core
  :aot [jdbc.core])

