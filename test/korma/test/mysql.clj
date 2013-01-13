(ns korma.test.mysql
  (:require [korma.mysql :as mysql])
  (:use clojure.test
        korma.config
        korma.core
        korma.db))


(defdb test-db-mysql (mysql {:db "korma" :user "korma" :password "kormapass"}))
(defentity users-mysql (database test-db-mysql))

(deftest test-mysql-count
  (sql-only
    (are [result query] (= result query)
      "SELECT COUNT(*) `cnt` FROM `users-mysql` GROUP BY `users-mysql`.`id`"
      (select users-mysql (aggregate (mysql/count :*) :cnt :id))

      "SELECT COUNT(*) `cnt` FROM `users-mysql` GROUP BY `users-mysql`.`id` HAVING (`users-mysql`.`id` = ?)"
      (select users-mysql
             (aggregate (mysql/count :*) :cnt :id)
             (having {:id 5}))

      "SELECT COUNT(`users-mysql`.`id`) `cnt` FROM `users-mysql`"
      (select users-mysql (aggregate (mysql/count :id) :cnt)))))