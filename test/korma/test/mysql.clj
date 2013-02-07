(ns korma.test.mysql
  (:require [korma.mysql :as mysql])
  (:use clojure.test
        korma.config
        korma.core
        korma.db))


(defdb test-db-mysql (mysql {:db "korma" :user "korma" :password "kormapass"}))
(defdb test-db-non-mysql (oracle {:db "korma" :user "korma" :password "kormapass" :delimiters "`"}))
(defentity users-mysql (database test-db-mysql))
(defentity users-non-mysql (database test-db-non-mysql))

(deftest test-mysql-count
  (sql-only
    (are [result query] (= result query)
      "SELECT COUNT(*) AS `cnt` FROM `users-mysql` GROUP BY `users-mysql`.`id`"
      (select users-mysql (aggregate (mysql/count :*) :cnt :id))

      "SELECT COUNT(`users-non-mysql`.*) AS `cnt` FROM `users-non-mysql` GROUP BY `users-non-mysql`.`id`"
      (select users-non-mysql (aggregate (count :*) :cnt :id))

      "SELECT COUNT(*) AS `cnt` FROM `users-mysql` GROUP BY `users-mysql`.`id` HAVING (`users-mysql`.`id` = ?)"
      (select users-mysql
             (aggregate (mysql/count :*) :cnt :id)
             (having {:id 5}))

      "SELECT COUNT(`users-mysql`.`id`) AS `cnt` FROM `users-mysql`"
      (select users-mysql (aggregate (mysql/count :id) :cnt)))))


(defentity users-no-db-specified (database (create-db {:delimiters "`"})))

(deftest test-uses-global-mysql-count-if-present
  (defdb test-db-mysql (mysql {:db "korma" :user "korma" :password "kormapass"}))

  (sql-only
   (is (= "SELECT COUNT(*) AS `cnt` FROM `users-no-db-specified` GROUP BY `users-no-db-specified`.`id`"
          (select users-no-db-specified (aggregate (count :*) :cnt :id))))))
