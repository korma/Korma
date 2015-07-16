(ns korma.test.integration.mysql
  (:refer-clojure :exclude [update])
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        korma.core
        korma.db))

(defdb live-db-mysql (mysql {:db "korma" :user "root"}))
(defentity users-live-mysql (database live-db-mysql))

(def mysql-uri
  {:connection-uri "jdbc:mysql://localhost/?user=root"})

(defn- setup-korma-db []
  (jdbc/db-do-commands mysql-uri "CREATE DATABASE IF NOT EXISTS korma;"
                       "USE korma;"
                       "CREATE TABLE IF NOT EXISTS `users-live-mysql` (name varchar(200));"))

(defn- clean-korma-db []
  (jdbc/db-do-commands mysql-uri "DROP DATABASE korma;"))

(use-fixtures :each (fn [f]
                      (default-connection live-db-mysql)
                      (setup-korma-db)
                      (f)
                      (clean-korma-db)))

(deftest test-nested-transactions-work
  (transaction
   (insert users-live-mysql (values {:name "thiago"}))
   (transaction
    (update users-live-mysql (set-fields {:name "THIAGO"}) (where {:name "thiago"})))))

(deftest mysql-count
  (insert users-live-mysql (values {:name "thiago"}))
  (is (= [{:cnt 1}]
         (select users-live-mysql (aggregate (count :*) :cnt)))))
