(ns korma.test.integration.per-query-database
  (:use clojure.test
        [korma.db :only [h2 with-db create-db default-connection]]
        [korma.core :only [defentity has-many exec-raw insert values select with database]]))

(def mem-db (create-db (h2 {:db "mem:query_database"})))

(defentity email)

(defentity user
  (has-many email))

(def schema
  ["drop table if exists \"user\";"
   "drop table if exists \"email\";"
   "create table \"user\" (\"id\" integer primary key,
                           \"name\" varchar(100));"
   "create table \"email\" (\"id\" integer primary key,
                            \"user_id\" integer,
                            \"email_address\" varchar(100),
                            foreign key (\"user_id\") references \"user\"(\"id\"));"])

(defn- setup-db []
  (with-db mem-db
    (dorun (map exec-raw schema))
    (insert user (values {:id 1 :name "Chris"}))
    (insert email (values {:id 1 :user_id 1 :email_address "chris@email.com"}))))

(use-fixtures :once (fn [f]
                      (default-connection nil)
                      (setup-db)
                      (f)))

(deftest use-database-from-parent-when-fetching-children
  (is (= [{:id 1 :name "Chris" :email [{:id 1 :user_id 1 :email_address "chris@email.com"}]}]
         (select user
                 (database mem-db)
                 (with email)))))
