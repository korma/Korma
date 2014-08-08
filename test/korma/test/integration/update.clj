(ns korma.test.integration.update
  (:require [clojure.string])
  (:use clojure.test
        [korma.db :only [defdb h2 default-connection]]
        [korma.core :only [defentity table transform exec-raw insert values update update* set-fields where]]))

(defdb mem-db (h2 {:db "mem:update"}))

(defentity user)

(defentity user-with-transform
  (table :user)
  (transform #(update-in % [:name] clojure.string/capitalize)))

(def schema
  ["drop table if exists \"user\";"
   "create table \"user\" (\"id\" integer primary key,
                           \"name\" varchar(100),
                           \"active\" boolean default false);"])

(defn- setup-db []
  (dorun (map exec-raw schema))
  (insert user (values [{:id 1 :name "James"}
                        {:id 2 :name "John"}
                        {:id 3 :name "Robert"}])))

(use-fixtures :once (fn [f]
                      (default-connection mem-db)
                      (setup-db)
                      (f)))

(deftest update-returns-count-of-updated-rows
  (testing "Updating one row"
    (is (= 1 (update user (set-fields {:active true}) (where {:id 1})))))
  (testing "Updating multiple rows"
    (is (= 3 (update user (set-fields {:active true})))))
  (testing "No rows are updated"
    (is (= 0 (update user (set-fields {:active true}) (where {:id [> 10]})))))
  (testing "Entity with transform fn"
    (is (= 1 (update user-with-transform (set-fields {:active true}) (where {:id 1}))))))
