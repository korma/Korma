(ns korma.test.connected
  (:require [clojure.string :as string]
            [criterium.core :as cr])
  (:use clojure.test
        korma.config
        korma.core
        korma.db))

(defn underscores->dashes [n]
  (-> n string/lower-case (.replaceAll "_" "-") keyword))

(defn dashes->underscores [n]
  (-> n name (.replaceAll "-" "_")))

(def dash-naming-strategy
  "naming strategy that converts clojure-style names (with dashes) to sql-style names (with udnerscores)"
  {:keys underscores->dashes :fields dashes->underscores})

(defn mem-db []
  (create-db (h2 {:db "mem:korma_test"
                  :naming dash-naming-strategy})))

(defmacro with-delimiters [& body]
  `(let [delimiters# (:delimiters @options)]
     (try
       (set-delimiters "\"" "\"")
       ~@body
       (finally
         (set-delimiters delimiters#)))))

(defmacro with-naming [strategy & body]
  `(let [naming# (:naming @options)]
     (try
       (set-naming ~strategy)
       ~@body
       (finally
         (set-naming naming#)))))

(declare user address state)

(defentity user
  (table :users)
  (has-many address {:fk :user-id})
  (entity-fields
   :name :age)

  (transform
   (partial
    map
    #(update-in % [:address] (partial sort-by :id)))))

(defentity address
  (belongs-to user {:fk :user-id})
  (belongs-to state)
  
  (entity-fields
   :number :street :city :zip))

(defentity state
  (entity-fields
   :name))

(def initial-data
  {:state
   [{:id "CA" :name "California"}
    {:id "PA" :name "Pennsylvania"}]
   :user []
   :address []})

(def schema
  ["drop table if exists \"state\";"
   "create table \"state\" (\"id\" varchar(20), \"name\" varchar(100));"
   "drop table if exists \"users\";"
   "create table \"users\" (\"id\" integer auto_increment primary key, \"name\" varchar(100), \"age\" integer);"
   "drop table if exists \"address\";"
   "create table \"address\" (\"id\" integer auto_increment primary key, \"user_id\" integer , \"state_id\" varchar(20), \"number\" varchar(20), \"street\" varchar(200), \"city\" varchar(200), \"zip\" varchar(10), foreign key (\"user_id\") references \"users\"(\"id\"), foreign key (\"state_id\") references \"state\"(\"id\"));"])

(defn- random-string []
  (str (java.util.UUID/randomUUID)))

(defn populate [num-users]
  (dorun
   (map exec-raw schema))
  (with-naming dash-naming-strategy
    (with-delimiters
      (let [data (atom initial-data)]
        (insert state
                (values (:state @data)))
        (doseq [n (range (max 1 num-users))]
          (let [u {:id n
                   :name (random-string)
                   :age (rand-int 100)}]
            (insert user
                    (values u))
            (swap! data
                   update-in [:user] conj
                   (assoc u :address []))))
        (doseq [ ;; make sure one of the users has
                ;; no addresses
                u (drop 1 (:user @data))
                n (range (rand-int 10))]
          (let [a {:user-id (:id u)
                   :street (random-string)
                   :number (subs (random-string) 0 10)
                   :city (random-string)
                   :zip (str (rand-int 10000))
                   :state-id (-> @data :state rand-nth :id)}
                inserted (insert address (values a))
                inserted-id (first (vals inserted))
                a (assoc a :id inserted-id)]
            (swap! data update-in [:address] conj a)
            (swap! data update-in [:user (:id u) :address] conj a)))
        @data))))

(def ^:dynamic *data*)

(use-fixtures :each
  (fn [t]
    (with-db (mem-db)
      (transaction
       (binding [*data* (populate 100)]
         (with-delimiters
           (with-naming dash-naming-strategy
             (t))))
       (rollback)))))

(deftest test-one-to-many
  (doseq [u (:user *data*)]
    (is
     (= [u]
        (select user
                (where {:id (:id u)})
                (with address))))))

(deftest test-one-to-many-batch
  (let [user-ids (map :id (:user *data*))]
    (is
     (=  (select user
                 (where {:id [in user-ids]})
                 (with address
                       (with state)))
         (select user
                 (where {:id [in user-ids]})
                 (with-batch address
                   (with-batch state))))
     "`with-data` should return the same data as `with`")))

(defn- getenv [s]
  (or (System/getenv s)
      (System/getProperty s)))

(when (= (getenv "BENCH") "true")
  (deftest bench-one-to-many-batch
    (let [user-ids (map :id (:user *data*))]
      (println "benchmarking with plain `with`")
      (cr/quick-bench
       (->>
        (select user
                (where {:id [in user-ids]})
                (with address
                      (with state)))
        (map
         #(update-in % [:address] doall))
        doall))
      (println "benchmarking with `with-data`")
      (cr/quick-bench
       (select user
               (where {:id [in user-ids]})
               (with-batch address
                 (with-batch state)))))))

(deftest test-one-to-many-batch-limitations
  (doseq [banned [#(order % :street)
                  #(group % :street)
                  #(limit % 1)
                  #(offset % 1)]]
    (is (thrown? Exception
                 (select user
                         (with-batch address
                           banned))))))

