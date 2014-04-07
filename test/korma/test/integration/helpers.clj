(ns korma.test.integration.helpers
  (:require [clojure.string :as string]
            [clojure.java.jdbc.deprecated :as jdbc]
            [criterium.core :as cr]
            [korma.config :as kconfig]
            [korma.core :as kcore]
            [korma.db :as kdb])
  (:use clojure.test))

(defn underscores->dashes [n]
  (-> n string/lower-case (.replaceAll "_" "-") keyword))

(defn dashes->underscores [n]
  (-> n name (.replaceAll "-" "_")))

(def dash-naming-strategy
  "naming strategy that converts clojure-style names (with dashes) to sql-style names (with udnerscores)"
  {:keys underscores->dashes :fields dashes->underscores})

(defn mem-db []
  (kdb/create-db (kdb/h2 {:db "mem:korma_test"})))

(defmacro with-delimiters [& body]
  `(let [delimiters# (:delimiters @kconfig/options)]
     (try
       (kconfig/set-delimiters "\"" "\"")
       ~@body
       (finally
         (apply kconfig/set-delimiters delimiters#)))))

(defmacro with-naming [strategy & body]
  `(let [naming# (:naming @kconfig/options)]
     (try
       (kconfig/set-naming ~strategy)
       ~@body
       (finally
         (kconfig/set-naming naming#)))))

(declare user address state)

(kcore/defentity user
  (kcore/table :users)
  (kcore/has-many address {:fk :user-id})
  (kcore/transform
   #(update-in % [:address] (partial sort-by :id))))

(kcore/defentity address
  (kcore/belongs-to user {:fk :user-id})
  (kcore/belongs-to state))

(kcore/defentity state)

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
   (map kcore/exec-raw schema)))

(defn- populate-states [data]
  (kcore/insert state
          (kcore/values (:state data))))

(defn- populate-users
  "add num-users to the database and to the `data` map"
  [data num-users]
  (reduce
   (fn [data user-id]
     (let [u {:id user-id
              :name (random-string)
              :age (rand-int 100)}]
       (kcore/insert user
               (kcore/values u))
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
                                    inserted (kcore/insert address (kcore/values a))
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
