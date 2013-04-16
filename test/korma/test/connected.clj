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
  (create-db (h2 {:db "mem:korma_test"})))

(defmacro with-delimiters [& body]
  `(let [delimiters# (:delimiters @options)]
     (try
       (set-delimiters "\"" "\"")
       ~@body
       (finally
         (apply set-delimiters delimiters#)))))

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
   #(update-in % [:address] (partial sort-by :id))))

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

(defn- reset-schema []
  (dorun
   (map exec-raw schema)))

(defn- populate-states [data]
  (insert state
          (values (:state data))))

(defn- populate-users
  "add num-users to the database and to the `data` map"
  [data num-users]
  (reduce
   (fn [data user-id]
     (let [u {:id user-id
              :name (random-string)
              :age (rand-int 100)}]
       (insert user
               (values u))
       (update-in data
                  [:user]
                  conj (assoc u :address []))))
   data
   (range num-users)))

(defn- populate-addresses
  "add up to max-addresses-per-user addresses to each user. ensure that at least one user has no addresses at all"
  [data max-addresses-per-user]
  (assoc data
    :user (vec
           (cons
            (first (:user data))
            (map
             (fn [user]
               (let [addrs (doall
                            (for [n (range (rand-int max-addresses-per-user))]
                              (let [a {:user-id (:id user)
                                       :street (random-string)
                                       :number (subs (random-string) 0 10)
                                       :city (random-string)
                                       :zip (str (rand-int 10000))
                                       :state-id (-> data :state rand-nth :id)}
                                    inserted (insert address (values a))
                                    ;; insert returns a map with a single key
                                    ;; the key depends on the underlying database, but the
                                    ;; value is the generated value of the key column
                                    inserted-id (first (vals inserted))
                                    a (assoc a :id inserted-id)]
                                a)))]
                 (assoc user :address (vec addrs))))
             (rest (:user data)))))))

(defn populate
  "populate the test database with random data and return a data structure that mirrors the data inserted into the database."
  [num-users]
  (reset-schema)
  (with-naming dash-naming-strategy
    (with-delimiters
      (-> initial-data
          populate-states
          (populate-users num-users)
          (populate-addresses 10)))))

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
  (is (= (sort-by :id (:user *data*))
         (select user
                 (with address)
                 (order :id))))
  (doseq [u (:user *data*)]
    (is
     (= [u]
        (select user
                (where {:id (:id u)})
                (with address)))))
  (doseq [u (:user *data*)]
    (is
     (= [(update-in u [:address]
                    (fn [addrs]
                      (map #(select-keys % [:street :city]) addrs)))]
        (select user
                (where {:id (:id u)})
                (with address
                      (fields :street :city)))))))

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
     "`with-batch` should return the same data as `with`")
    (is
     (=  (select user
                 (where {:id [in user-ids]})
                 (with address
                       ;; with-batch will add the foreign key 
                       (fields :user-id :street :city)
                       (with state)))
         (select user
                 (where {:id [in user-ids]})
                 (with-batch address
                   (fields :street :city)
                   (with-batch state))))
     "`with-batch` should return the same data as `with` when using explicit projection")))

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
      (println "benchmarking with `with-batch`")
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

