(ns jdbc.core
  (:require [clojure.java.jdbc :as sql]
            [clojure.java.io :as io])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource)
  (:import org.postgresql.copy.CopyManager)
  (:import org.postgresql.copy.PGCopyInputStream)
  (:import org.postgresql.util.PSQLException)
  (:gen-class))

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

(def table1
  (sql/create-table-ddl :nation
                        [[:N_NATIONKEY  :INTEGER :NOT :NULL],
                         [:N_NAME       "CHAR(25)" :NOT :NULL],
                         [:N_REGIONKEY  :INTEGER :NOT :NULL],
                         [:N_COMMENT    "VARCHAR(152)"]]))

(def table2
  (sql/create-table-ddl :region
                        [[:R_REGIONKEY  :INTEGER :NOT :NULL],
                         [:R_NAME       "CHAR(25)" :NOT :NULL],
                         [:R_COMMENT    "VARCHAR(152)"]]))

(def table3
  (sql/create-table-ddl :part
                        [[:P_PARTKEY     :INTEGER :NOT :NULL],
                         [:P_NAME        "VARCHAR(55)" :NOT :NULL],
                         [:P_MFGR        "CHAR(25)" :NOT :NULL],
                         [:P_BRAND       "CHAR(10)" :NOT :NULL],
                         [:P_TYPE        "VARCHAR(25)" :NOT :NULL],
                         [:P_SIZE        :INTEGER :NOT :NULL],
                         [:P_CONTAINER   "CHAR(10)" :NOT :NULL],
                         [:P_RETAILPRICE "DECIMAL(15,2)" :NOT :NULL],
                         [:P_COMMENT     "VARCHAR(23)" :NOT :NULL]]))

(def table4
  (sql/create-table-ddl :supplier
                        [[:S_SUPPKEY     :INTEGER :NOT :NULL],
                         [:S_NAME        "CHAR(25)" :NOT :NULL],
                         [:S_ADDRESS     "VARCHAR(40)" :NOT :NULL],
                         [:S_NATIONKEY   :INTEGER :NOT :NULL],
                         [:S_PHONE       "CHAR(15)" :NOT :NULL],
                         [:S_ACCTBAL     "DECIMAL(15,2)" :NOT :NULL],
                         [:S_COMMENT     "VARCHAR(101)" :NOT :NULL]]))

(def table5
  (sql/create-table-ddl :partsupp
                        [[:PS_PARTKEY     :INTEGER :NOT :NULL],
                         [:PS_SUPPKEY     :INTEGER :NOT :NULL],
                         [:PS_AVAILQTY    :INTEGER :NOT :NULL],
                         [:PS_SUPPLYCOST  "DECIMAL(15,2)"  :NOT :NULL],
                         [:PS_COMMENT     "VARCHAR(199)" :NOT :NULL]]))

(def table6
  (sql/create-table-ddl :customer
                        [[:C_CUSTKEY     :INTEGER :NOT :NULL],
                         [:C_NAME        "VARCHAR(25)" :NOT :NULL],
                         [:C_ADDRESS     "VARCHAR(40)" :NOT :NULL],
                         [:C_NATIONKEY   :INTEGER :NOT :NULL],
                         [:C_PHONE       "CHAR(15)" :NOT :NULL],
                         [:C_ACCTBAL     "DECIMAL(15,2)"  :NOT :NULL],
                         [:C_MKTSEGMENT  "CHAR(10)" :NOT :NULL],
                         [:C_COMMENT     "VARCHAR(117)" :NOT :NULL]]))

(def table7
  (sql/create-table-ddl :orders
                        [[:O_ORDERKEY       :INTEGER :NOT :NULL],
                         [:O_CUSTKEY        :INTEGER :NOT :NULL],
                         [:O_ORDERSTATUS    "CHAR(1)" :NOT :NULL],
                         [:O_TOTALPRICE     "DECIMAL(15,2)" :NOT :NULL],
                         [:O_ORDERDATE      :DATE :NOT :NULL],
                         [:O_ORDERPRIORITY  "CHAR(15)" :NOT :NULL],
                         [:O_CLERK          "CHAR(15)" :NOT :NULL],
                         [:O_SHIPPRIORITY   :INTEGER :NOT :NULL],
                         [:O_COMMENT        "VARCHAR(79)" :NOT :NULL]]))

(def table8
  (sql/create-table-ddl :lineitem
                        [[:L_ORDERKEY    :INTEGER :NOT :NULL],
                         [:L_PARTKEY     :INTEGER :NOT :NULL],
                         [:L_SUPPKEY     :INTEGER :NOT :NULL],
                         [:L_LINENUMBER  :INTEGER :NOT :NULL],
                         [:L_QUANTITY    "DECIMAL(15,2)" :NOT :NULL],
                         [:L_EXTENDEDPRICE  "DECIMAL(15,2)" :NOT :NULL],
                         [:L_DISCOUNT    "DECIMAL(15,2)" :NOT :NULL],
                         [:L_TAX         "DECIMAL(15,2)" :NOT :NULL],
                         [:L_RETURNFLAG  "CHAR(1)" :NOT :NULL],
                         [:L_LINESTATUS  "CHAR(1)" :NOT :NULL],
                         [:L_SHIPDATE    :DATE :NOT :NULL],
                         [:L_COMMITDATE  :DATE :NOT :NULL],
                         [:L_RECEIPTDATE :DATE :NOT :NULL],
                         [:L_SHIPINSTRUCT "CHAR(25)" :NOT :NULL],
                         [:L_SHIPMODE     "CHAR(10)" :NOT :NULL],
                         [:L_COMMENT      "VARCHAR(44)" :NOT :NULL]]))

(defn apply-constraints
  "Apply contraints on the tables."
  [pooled-db]
  (sql/db-do-commands pooled-db
             ["ALTER TABLE REGION ADD PRIMARY KEY (R_REGIONKEY)"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE NATION ADD PRIMARY KEY (N_NATIONKEY)"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE NATION ADD CONSTRAINT N_FK_REGION FOREIGN KEY (N_REGIONKEY) REFERENCES REGION (R_REGIONKEY)"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE PART ADD PRIMARY KEY (P_PARTKEY)"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE SUPPLIER ADD PRIMARY KEY (S_SUPPKEY)"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE SUPPLIER ADD CONSTRAINT S_FK_NATION FOREIGN KEY (S_NATIONKEY) REFERENCES NATION"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE PARTSUPP ADD PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE CUSTOMER ADD PRIMARY KEY (C_CUSTKEY)"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE CUSTOMER ADD CONSTRAINT C_FK_NATION FOREIGN KEY (C_NATIONKEY) REFERENCES NATION"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE LINEITEM ADD PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE ORDERS ADD PRIMARY KEY (O_ORDERKEY)"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE PARTSUPP ADD CONSTRAINT PS_FK_SUPPLIER FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE PARTSUPP ADD CONSTRAINT PS_FK_PART FOREIGN KEY (PS_PARTKEY) REFERENCES PART"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE ORDERS ADD CONSTRAINT O_FK_CUSTOMER FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE LINEITEM ADD CONSTRAINT L_FK_ORDER FOREIGN KEY (L_ORDERKEY)  REFERENCES ORDERS"])
  (sql/db-do-commands pooled-db
             ["ALTER TABLE LINEITEM ADD CONSTRAINT L_FK_PARTSUPP FOREIGN KEY  (L_PARTKEY,L_SUPPKEY) REFERENCES PARTSUPP"])
  )

(defn create-tpch-tables
  "Create tables for TPC-H if they do not exist."
  [pooled-db]
  (println "Creating tables...")
  (sql/db-do-commands pooled-db table1)
  (sql/db-do-commands pooled-db table2)
  (sql/db-do-commands pooled-db table3)
  (sql/db-do-commands pooled-db table4)
  (sql/db-do-commands pooled-db table5)
  (sql/db-do-commands pooled-db table6)
  (sql/db-do-commands pooled-db table7)
  (sql/db-do-commands pooled-db table8)
  (apply-constraints pooled-db)
  (println "Tables creation complete"))

;; Java Interop translated from
;; http://stackoverflow.com/questions/6958965/how-to-copy-a-data-from-file-to-postgresql-using-jdbc
;; http://stackoverflow.com/questions/14539804/copy-from-and-c3po-connection-pool-in-postgres
;; https://gist.github.com/dpick/15dcd98167c099a356c6#file-copy
(defn load-tbl-with-name
  "Load table with a given name."
  [tbl-name path]
  (try
    (let [conn (sql/get-connection db-spec)
        fr (java.io.FileReader. (str path tbl-name ".tbl"))
        cm (CopyManager. conn)]
    (.copyIn cm (str "COPY " tbl-name " from stdin WITH DELIMITER AS '|'") fr))
    (catch Exception e (println (str "Caught exception: " (.toString e))))))


(defn load-tables
  "Load tables for Postgres."
  [path]
  (println "Loading region table...")
  (load-tbl-with-name "region" path)
  (println "Loading nation table...")
  (load-tbl-with-name "nation" path)
  (println "Loading customer table...")
  (load-tbl-with-name "customer" path)
  (println "Loading supplier table...")
  (load-tbl-with-name "supplier" path)
  (println "Loading part table...")
  (load-tbl-with-name "part" path)
  (println "Loading partsupp table...")
  (load-tbl-with-name "partsupp" path)
  (println "Loading orders table...")
  (load-tbl-with-name "orders" path)
  (println "Loading lineitem table...")
  (load-tbl-with-name "lineitem" path)
  (println "Loading complete."))

(defn analyse-tables
  "Analyse tables."
  [pooled-db]
  (println "Analyzing region table...")
  (sql/db-do-commands pooled-db "analyse region")
  (println "Analyzing nation table...")
  (sql/db-do-commands pooled-db "analyse nation")
  (println "Analyzing customer table...")
  (sql/db-do-commands pooled-db "analyse customer")
  (println "Analyzing supplier table...")
  (sql/db-do-commands pooled-db "analyse supplier")
  (println "Analyzing part table...")
  (sql/db-do-commands pooled-db "analyse part")
  (println "Analyzing partsupp table...")
  (sql/db-do-commands pooled-db "analyse partsupp")
  (println "Analyzing orders table...")
  (sql/db-do-commands pooled-db "analyse orders")
  (println "Analyzing lineitem table...")
  (sql/db-do-commands pooled-db "analyse lineitem")
  (println "Analysis complete"))

(defn load-and-analyse
  "Load tables with data and generate statistics"
  [pooled-db path]
  (load-tables path)
  (analyse-tables pooled-db))

(defn check-for-tables
  "Check for existence of tables."
  [pooled-db path]
  (try
    (let [count (-> (sql/query pooled-db ["select count(*) from region as count"]) first :count)]
      (if (= count 0)
        (load-and-analyse pooled-db path)
        (analyse-tables pooled-db)))
    (catch Exception e (println (str "Caught Exception: " (.toString e))
           (println "TCP-H tables do not exist")
           (create-tpch-tables pooled-db)
           (load-tables pooled-db)))))

(defn -main
  "JDBC CS425 Assignment."
  [path]
  (check-for-tables pooled-db path))
