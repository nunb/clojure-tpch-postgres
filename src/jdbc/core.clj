(ns jdbc.core
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:gen-class))

(require '[clojure.java.jdbc :as sql])

(def db-spec
  {:classname "org.postgresql.Driver"
   :subprotocol "postgresql"
   :subname "//localhost:5432/postgres"
   :user "sanchayan"})

(defn pool
  [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec))
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 30 60))
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime (* 3 60 60)))]
    {:datasource cpds}))

(def pooled-db (pool db-spec))

(defn -main
  "JDBC CS425 Assignment."
  []
  (let [rows (sql/query pooled-db ["select * from nation"])]
    (doseq [record rows] (println record)))
  (let [rows (sql/query pooled-db ["select * from region"])]
    (doseq [record rows] (println record))))
