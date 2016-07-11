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

(def tpch_query1
  (str "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, "
       "sum(l_extendedprice) as sum_base_price, "
       "sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, "
       "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, "
       "avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, "
       "count(*) as count_order from lineitem where "
       "l_shipdate <= date '1998-12-01' - interval '107' day "
       "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus LIMIT 107"))

(def tpch_query2
  (str "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, "
       "s_phone, s_comment from part, supplier, partsupp, nation, region "
       "where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 4 "
       "and p_type like '%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey "
       "and r_name = 'MIDDLE EAST' and ps_supplycost = ("
       "select min(ps_supplycost) from partsupp, supplier, nation, region "
       "where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey "
       "and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST') order by "
       "s_acctbal desc, n_name, s_name, p_partkey LIMIT 100"))

(def tpch_query3
  (str "select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, "
       "o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey "
       "and l_orderkey = o_orderkey and o_orderdate < date '1995-03-29' and l_shipdate > date '1995-03-29' "
       "group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate LIMIT 10"))

(def tpch_query4
  (str "select o_orderpriority, count(*) as order_count from orders where "
       "o_orderdate >= date '1997-05-01' and o_orderdate < date '1997-05-01' + interval '3' month "
       "and exists (select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate) "
       "group by o_orderpriority order by o_orderpriority LIMIT 1"))

(def tpch_query5
  (str "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue "
       "from customer, orders, lineitem, supplier, nation, region "
       "where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey "
	"and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey "
	"and r_name = 'EUROPE' and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year "
        "group by n_name order by revenue desc LIMIT 1"))

(def tpch_query6
  (str "select sum(l_extendedprice * l_discount) as revenue from lineitem "
       "where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year "
	"and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24 LIMIT 1"))

(def tpch_query7
  (str "select supp_nation, cust_nation, l_year, sum(volume) as revenue from "
	"(select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, "
	"l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, "
	"nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey "
	"and c_nationkey = n2.n_nationkey and ((n1.n_name = 'KENYA' and n2.n_name = 'IRAQ') or (n1.n_name = 'IRAQ' and n2.n_name = 'KENYA')) "
	"and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping group by supp_nation, cust_nation, l_year "
        "order by supp_nation, cust_nation, l_year LIMIT 1"))

(def tpch_query8
  (str "select o_year, sum(case when nation = 'IRAQ' then volume else 0 end) / sum(volume) as mkt_share from "
	"(select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation "
        "from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey "
	"and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey "
	"and r_name = 'MIDDLE EAST' and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL'"
	") as all_nations group by o_year order by o_year LIMIT 1"))

(def tpch_query9
  (str "select nation, o_year, sum(amount) as sum_profit from (select n_name as nation, extract(year from o_orderdate) as o_year, "
	"l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, "
	"nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey "
	"and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%peru%') as profit "
        "group by nation, o_year order by nation, o_year desc LIMIT 1"))

(def tpch_query10
  (str "select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, "
	"c_comment from customer, orders, lineitem, nation where c_custkey = o_custkey and l_orderkey = o_orderkey and "
        "o_orderdate >= date '1993-07-01' "
	"and o_orderdate < date '1993-07-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey, "
	"c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc LIMIT 20"))

(def tpch_query11
  (str "select ps_partkey, sum(ps_supplycost * ps_availqty) as value from partsupp, supplier, nation where "
	"ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'VIETNAM' group by ps_partkey having "
	"sum(ps_supplycost * ps_availqty) > (select sum(ps_supplycost * ps_availqty) * 0.0001 from partsupp, supplier, nation "
	"where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'VIETNAM' ) order by value desc LIMIT 1"))

(def tpch_query12
  (str "select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, "
	"sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count "
        "from orders, lineitem where o_orderkey = l_orderkey and l_shipmode in ('MAIL', 'REG AIR') and l_commitdate < l_receiptdate "
	"and l_shipdate < l_commitdate and l_receiptdate >= date '1995-01-01' and l_receiptdate < date '1995-01-01' + interval '1' year "
        "group by l_shipmode order by l_shipmode LIMIT 1"))

(def tpch_query13
  (str "select c_count, count(*) as custdist from (select c_custkey, count(o_orderkey) from customer left outer join orders on "
	"c_custkey = o_custkey and o_comment not like '%special%accounts%' group by c_custkey ) as c_orders (c_custkey, c_count) "
        "group by c_count order by custdist desc, c_count desc LIMIT 1"))

(def tpch_query14
  (str "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 "
	"end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part "
        "where l_partkey = p_partkey and l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month LIMIT 1"))

(def tpch_query15
  (str "create view revenue0 (supplier_no, total_revenue) as select l_suppkey, sum(l_extendedprice * (1 - l_discount)) "
	"from lineitem where l_shipdate >= date '1993-04-01' and l_shipdate < date '1993-04-01' + interval '3' month group by l_suppkey; "
        "select s_suppkey, s_name, s_address, s_phone, total_revenue from supplier, revenue0 where s_suppkey = supplier_no "
	"and total_revenue = (select max(total_revenue) from revenue0) order by s_suppkey LIMIT 1; drop view revenue0"))

(def tpch_query16
  (str "select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from partsupp, part where "
	"p_partkey = ps_partkey and p_brand <> 'BRAND#34' and p_type not like 'ECONOMY ANODIZED%' "
	"and p_size in (26, 12, 13, 28, 38, 20, 32, 48) and ps_suppkey not in (select s_suppkey from supplier where "
	"s_comment like '%Customer%Complaints%') group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, "
	"p_type, p_size LIMIT 1"))

(def tpch_query17
  (str "select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part, "
	"(SELECT l_partkey AS agg_partkey, 0.2 * avg(l_quantity) AS avg_quantity FROM lineitem GROUP BY l_partkey) part_agg where "
	"p_partkey = l_partkey and agg_partkey = l_partkey and p_brand = 'Brand#44' and p_container = 'MED PACK' "
        "and l_quantity < avg_quantity LIMIT 1"))

(def tpch_query18
  (str "select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem "
       "where o_orderkey in (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 314) "
	"and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice "
        "order by o_totalprice desc, o_orderdate LIMIT 100"))

(def tpch_query19
  (str "select sum(l_extendedprice * (1 - l_discount)) as revenue from lineitem, part where (p_partkey = l_partkey "
       "and p_brand = 'BRAND#32' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 6 and l_quantity <= 6 + 10 "
	"and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') "
	"or (p_partkey = l_partkey and p_brand = 'BRAND#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') "
	"and l_quantity >= 20 and l_quantity <= 20 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') "
        "and l_shipinstruct = 'DELIVER IN PERSON') or "
        "(p_partkey = l_partkey and p_brand = 'BRAND#42' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') "
	"and l_quantity >= 21 and l_quantity <= 21 + 10 and p_size between 1 and 15 "
	"and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON') LIMIT 1"))

(def tpch_query20
  (str "select s_name, s_address from supplier, nation where s_suppkey in (select ps_suppkey from partsupp, "
	"(select l_partkey agg_partkey, l_suppkey agg_suppkey, 0.5 * sum(l_quantity) AS agg_quantity from lineitem "
	"where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year group by l_partkey, l_suppkey) agg_lineitem "
	"where agg_partkey = ps_partkey and agg_suppkey = ps_suppkey and ps_partkey in (select p_partkey from part "
	"where p_name like 'cream%' ) and ps_availqty > agg_quantity) and s_nationkey = n_nationkey and n_name = 'JAPAN' "
        "order by s_name LIMIT 1"))

(def tpch_query21
  (str "select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where "
	"s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate "
	"and exists (select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey) "
	"and not exists (select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey "
	"and l3.l_receiptdate > l3.l_commitdate) and s_nationkey = n_nationkey and n_name = 'ETHIOPIA' "
        "group by s_name order by numwait desc, s_name LIMIT 100"))

(def tpch_query22
  (str "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ("
	"select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from customer where "
	"substring(c_phone from 1 for 2) in ('22', '28', '19', '25', '33', '17', '13') and c_acctbal > ("
        "select avg(c_acctbal) from customer where c_acctbal > 0.00 and substring(c_phone from 1 for 2) in "
        "('22', '28', '19', '25', '33', '17', '13')) and not exists (select * from orders where o_custkey = c_custkey)) as custsale "
        "group by cntrycode order by cntrycode LIMIT 1"))

(def queries-to-run [tpch_query1 tpch_query2 tpch_query3 tpch_query4
                     tpch_query5 tpch_query6 tpch_query7 tpch_query8
                     tpch_query9 tpch_query10 tpch_query11 tpch_query12
                     tpch_query13 tpch_query14 tpch_query15 tpch_query16
                     tpch_query17 tpch_query18 tpch_query19 tpch_query20
                     tpch_query21 tpch_query22])

(def cr_nation_tbl
  (sql/create-table-ddl :nation
                        [[:N_NATIONKEY  :INTEGER :NOT :NULL],
                         [:N_NAME       "CHAR(25)" :NOT :NULL],
                         [:N_REGIONKEY  :INTEGER :NOT :NULL],
                         [:N_COMMENT    "VARCHAR(152)"]]))

(def cr_region_tbl
  (sql/create-table-ddl :region
                        [[:R_REGIONKEY  :INTEGER :NOT :NULL],
                         [:R_NAME       "CHAR(25)" :NOT :NULL],
                         [:R_COMMENT    "VARCHAR(152)"]]))

(def cr_part_tbl
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

(def cr_supplier_tbl
  (sql/create-table-ddl :supplier
                        [[:S_SUPPKEY     :INTEGER :NOT :NULL],
                         [:S_NAME        "CHAR(25)" :NOT :NULL],
                         [:S_ADDRESS     "VARCHAR(40)" :NOT :NULL],
                         [:S_NATIONKEY   :INTEGER :NOT :NULL],
                         [:S_PHONE       "CHAR(15)" :NOT :NULL],
                         [:S_ACCTBAL     "DECIMAL(15,2)" :NOT :NULL],
                         [:S_COMMENT     "VARCHAR(101)" :NOT :NULL]]))

(def cr_partsupp_tbl
  (sql/create-table-ddl :partsupp
                        [[:PS_PARTKEY     :INTEGER :NOT :NULL],
                         [:PS_SUPPKEY     :INTEGER :NOT :NULL],
                         [:PS_AVAILQTY    :INTEGER :NOT :NULL],
                         [:PS_SUPPLYCOST  "DECIMAL(15,2)"  :NOT :NULL],
                         [:PS_COMMENT     "VARCHAR(199)" :NOT :NULL]]))

(def cr_customer_tbl
  (sql/create-table-ddl :customer
                        [[:C_CUSTKEY     :INTEGER :NOT :NULL],
                         [:C_NAME        "VARCHAR(25)" :NOT :NULL],
                         [:C_ADDRESS     "VARCHAR(40)" :NOT :NULL],
                         [:C_NATIONKEY   :INTEGER :NOT :NULL],
                         [:C_PHONE       "CHAR(15)" :NOT :NULL],
                         [:C_ACCTBAL     "DECIMAL(15,2)"  :NOT :NULL],
                         [:C_MKTSEGMENT  "CHAR(10)" :NOT :NULL],
                         [:C_COMMENT     "VARCHAR(117)" :NOT :NULL]]))

(def cr_orders_tbl
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

(def cr_lineitem_tbl
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
  (let
      [tables
       [cr_nation_tbl cr_region_tbl cr_part_tbl cr_supplier_tbl
                      cr_partsupp_tbl cr_customer_tbl cr_orders_tbl cr_lineitem_tbl]]
    (doseq [table tables] (sql/db-do-commands pooled-db table)))
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
        br (java.io.BufferedReader. (java.io.FileReader. (str path tbl-name ".tbl")))
        cm (CopyManager. conn)]
    (.copyIn cm (str "COPY " tbl-name " from stdin WITH DELIMITER AS '|'") br))
    (catch Exception e (println (str "Caught exception: " (.toString e))))))


(defn load-tables
  "Load tables for Postgres."
  [path]
  (let
      [tables
        ["region" "nation" "customer" "supplier" "part" "partsupp" "orders" "lineitem"]]
    (doseq [table tables]
      (println (str "Loading " table " table..."))
      (load-tbl-with-name table path)))
  (println "Loading complete."))

(defn analyse-tables
  "Analyse tables."
  [pooled-db]
  (let
      [tables
       ["region" "nation" "customer" "supplier" "part" "partsupp" "orders" "lineitem"]]
    (doseq [table tables]
      (println (str "Analyzing " table " table..."))
      (sql/db-do-commands pooled-db (str "analyse " table))))
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
           (load-tables path)
           (analyse-tables pooled-db)))))

;;https://groups.google.com/forum/#!topic/clojure/bKBkInBCzf8
(defmacro bench
  "Times the execution of forms, discarding their output and returning
  a long in nanoseconds."
  ([& forms]
    `(let [start# (System/currentTimeMillis)]
       ~@forms
       (- (System/currentTimeMillis) start#))))

(defn run-timed-query
  "Returns time in ms from an average of 3 runs."
  [tpch_query]
  (/ (reduce + (repeatedly 3 #(bench (sql/query pooled-db tpch_query)))) 3.0))

(defn run-a-query
  "Run a query three times and return the average time."
  [query]
  (let [starttime (System/currentTimeMillis)]
    (dotimes [n 3]
      (try
        (let
          [rs (sql/query pooled-db query)]
        (dorun 0 (map #(println (keys %)) rs)) (dorun (map #(println (vals %)) rs)))
        (catch Exception e)))
      (/ (- (System/currentTimeMillis) starttime) 3.0)))

;;https://clojuredocs.org/clojure.core/conj!
(defn run-all-queries
  "Run all TPC-H queries. Return a vector of average time taken for each query."
  []
  (loop [i 0 v (transient [])]
    (if (< i 22)
      (recur (inc i) (conj! v (run-a-query (get queries-to-run i))))
      (persistent! v))))

(defn print-results
  []
  (let [v (run-all-queries)]
    (dotimes [n 22]
      (println (str "TPC-H Query " (+ n 1) " took " (get v n) " ms.")))
    (println (str "Total runtime of 22 queries: " (reduce + v) " ms."))))

(defn -main
  "JDBC CS425 Assignment."
  [path]
  (check-for-tables pooled-db path)
  (print-results))
