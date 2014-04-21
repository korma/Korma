(ns korma.test.integration.one-to-one
  (:require clojure.string)
  (:use clojure.test
        [korma.db :only [defdb h2 default-connection]]
        [korma.core :only [defentity pk belongs-to has-one transform
                           exec-raw insert values select with]]))

(defdb mem-db (h2 {:db "mem:one_to_one_test"}))

(defentity state
  (pk :state_id)
  (transform #(update-in % [:name] clojure.string/capitalize)))

(defentity address
  (pk :address_id)
  (belongs-to state {:fk :id_of_state})
  (transform #(update-in % [:street] clojure.string/capitalize)))

(defentity user
  (has-one address {:fk :id_of_user}))

(def schema
  ["drop table if exists \"state\";"
   "drop table if exists \"user\";"
   "drop table if exists \"address\";"
   "create table \"state\" (\"state_id\" varchar(20),
                         \"name\" varchar(100));"
   "create table \"user\" (\"id\" integer primary key,
                           \"name\" varchar(100));"
   "create table \"address\" (\"address_id\" integer primary key,
                              \"id_of_user\" integer,
                              \"id_of_state\" varchar(20),
                              \"street\" varchar(200),
                              foreign key (\"id_of_user\") references \"user\"(\"id\"),
                              foreign key (\"id_of_state\") references \"state\"(\"state_id\"));"])

(defn- setup-db []
  (default-connection mem-db)
  (dorun (map exec-raw schema))
  (insert :state (values [{:state_id "CA" :name "california"}
                          {:state_id "PA" :name "pennsylvania"}]))
  (insert :user (values [{:id 1 :name "Chris"}
                         {:id 2 :name "Alex"}]))
  (insert :address (values [{:address_id 101 :street "main street" :id_of_state "CA" :id_of_user 1}
                            {:address_id 102 :street "park street" :id_of_state "PA" :id_of_user 2}])))

(use-fixtures :once (fn [f]
                      (setup-db)
                      (f)))

(deftest belongs-to-with-transformer-users-correct-join-keys
  (is (= [{:id 1 :name "Chris" :address_id 101 :street "Main street" :id_of_state "CA" :id_of_user 1}
          {:id 2 :name "Alex"  :address_id 102 :street "Park street" :id_of_state "PA" :id_of_user 2}]
         (select user (with address)))))

(deftest has-one-with-transformer-users-correct-join-keys
  (is (= [{:address_id 101 :street "Main street" :id_of_state "CA" :id_of_user 1 :state_id "CA" :name "California"}
          {:address_id 102 :street "Park street" :id_of_state "PA" :id_of_user 2 :state_id "PA" :name "Pennsylvania"}]
         (select address (with state)))))
