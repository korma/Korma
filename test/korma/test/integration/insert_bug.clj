(ns korma.test.integration.insert-bug
  (:use clojure.test
        [korma.db :only [defdb h2 with-db create-db default-connection]]
        [korma.core :only [query-only sql-only defentity has-many exec-raw insert values select with database]]))

(defdb mem-db (h2 {:db "mem:insert_bug"}))

(defentity email)

(defentity user
           (has-many email))

(def schema
  ["drop table if exists \"user\";"
   "drop table if exists \"email\";"
   "create table \"user\" (\"id\" integer auto_increment primary key,
                           \"name\" varchar(100));"
   "create table \"email\" (\"id\" integer auto_increment primary key,
                            \"user_id\" integer NOT NULL,
                            \"email_address\" varchar(100),
                            foreign key (\"user_id\") references \"user\"(\"id\"));"])

(defn- setup-db []
  (dorun (map exec-raw schema))
  (insert user (values {:name "Chris"}))
  (let [user (first (select user))]
    (insert email (values {:user_id (:id user) :email_address "default@default.com"}))))

(use-fixtures :once (fn [f]
                      (setup-db)
                      (f)))

(deftest insert-based-on-select-with-relation
  (let [data (for [user-with-email (select user (with email))
                   {:keys [user_id email_address]} (:email user-with-email)]
               {:user_id user_id :email_address email_address})]
    ;; This makes the test pass
    ;; (prn data)
    (is (not (nil? (insert email (values data)))))))
