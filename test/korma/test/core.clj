(ns korma.test.core
  (:require [clojure.string :as string])
  (:use [korma.core]
        [korma.db]
        [korma.config])
  (:use [clojure.test]))

(defdb test-db-opts (postgres {:db "korma" :user "korma" :password "kormapass" :delimiters "" :naming {:fields string/upper-case}}))
(defdb test-db (postgres {:db "korma" :user "korma" :password "kormapass"}))

(defentity delims
  (database test-db-opts))

(defentity users)
(defentity state)
(defentity address
  (belongs-to state))
(defentity email)

(defentity user2
  (table :users)
  (has-one address)
  (has-many email))

(defentity users-alias
  (table :users :u))

(defentity blah (pk :cool) (has-many users {:fk :cool_id}))

(deftest select-function
  (is (= (-> (select* "users")
           (fields :id :username)
           (where {:username "chris"})
           (order :created)
           (limit 5)
           (offset 3)
           (as-sql))
         "SELECT \"users\".\"id\", \"users\".\"username\" FROM \"users\" WHERE (\"users\".\"username\" = ?) ORDER BY \"users\".\"created\" ASC LIMIT 5 OFFSET 3")))


(deftest simple-selects
  (sql-only
    (are [query result] (= query result)
         (select users)
         "SELECT \"users\".* FROM \"users\""
         (select users-alias)
         "SELECT \"u\".* FROM \"users\" \"u\""
         (select users
                 (fields :id :username))
         "SELECT \"users\".\"id\", \"users\".\"username\" FROM \"users\""
         (select users
                 (where {:username "chris"
                         :email "hey@hey.com"}))
         "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"username\" = ? AND \"users\".\"email\" = ?)"
         (select users
                 (where {:username "chris"})
                 (order :created))
         "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"username\" = ?) ORDER BY \"users\".\"created\" ASC"
         (select users
                 (where {:active true})
                 (order :created)
                 (limit 5)
                 (offset 3))
         "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"active\" = TRUE) ORDER BY \"users\".\"created\" ASC LIMIT 5 OFFSET 3")))

(deftest update-function
  (is (= (-> (update* "users")
           (set-fields {:first "chris"
                        :last "granger"})
           (where {:id 3})
           (as-sql))
         "UPDATE \"users\" SET \"first\" = ?, \"last\" = ? WHERE (\"users\".\"id\" = ?)")))

(deftest update-queries
  (sql-only
    (are [query result] (= query result)
         (update users
                 (set-fields {:first "chris"}))
         "UPDATE \"users\" SET \"first\" = ?"
         (update users
                 (set-fields {:first "chris"})
                 (where {:id 3}))
         "UPDATE \"users\" SET \"first\" = ? WHERE (\"users\".\"id\" = ?)"
         (update users
                 (set-fields {:first "chris"
                              :last "granger"})
                 (where {:id 3}))
         "UPDATE \"users\" SET \"first\" = ?, \"last\" = ? WHERE (\"users\".\"id\" = ?)")))

(deftest delete-function
  (is (= (-> (delete* "users")
           (where {:id 3})
           (as-sql))
         "DELETE FROM \"users\" WHERE (\"users\".\"id\" = ?)")))

(deftest delete-queries
  (sql-only
    (are [query result] (= query result)
         (delete users)
         "DELETE FROM \"users\""
         (delete users
                 (where {:id 3}))
         "DELETE FROM \"users\" WHERE (\"users\".\"id\" = ?)")))

(deftest insert-function
  (is (= (-> (insert* "users")
           (values {:first "chris" :last "granger"})
           (as-sql))
         "INSERT INTO \"users\" (\"last\", \"first\") VALUES (?, ?)"))

  (testing "WHEN values is empty THEN generates a NOOP SQL statement"
    (is (= (-> (insert* "users")
               (values {})
               (as-sql))
           "DO 0"))))

(deftest insert-queries
  (sql-only
    (are [query result] (= query result)
         (insert users
                 (values {:first "chris" :last "granger"}))
         "INSERT INTO \"users\" (\"last\", \"first\") VALUES (?, ?)"
         (insert users
                 (values [{:first "chris" :last "granger"}
                          {:last "jordan" :first "michael"}]))
         "INSERT INTO \"users\" (\"last\", \"first\") VALUES (?, ?), (?, ?)"
         (insert users (values {}))
         "DO 0"
         (insert users (values []))
         "DO 0"
         (insert users (values [{} {}]))
         "DO 0")))

(deftest complex-where
  (sql-only
    (are [query result] (= query result)
         (select users
                 (where (or (= :name "chris")
                            (= :name "john"))))
         "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"name\" = ? OR \"users\".\"name\" = ?)"
         (select users
                 (where (or {:name "chris"}
                            {:name "john"})))
         "SELECT \"users\".* FROM \"users\" WHERE ((\"users\".\"name\" = ?) OR (\"users\".\"name\" = ?))"
         (select users
                 (where (or {:name "drew"
                             :last "dreward"}
                            {:email "drew@drew.com"}
                            (> :age 10))))
         "SELECT \"users\".* FROM \"users\" WHERE ((\"users\".\"last\" = ? AND \"users\".\"name\" = ?) OR (\"users\".\"email\" = ?) OR \"users\".\"age\" > ?)"
         (select users
                 (where (or (< :x 5)
                            (or (< :y 3)
                                (> :z 4)))))
         "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"x\" < ? OR (\"users\".\"y\" < ? OR \"users\".\"z\" > ?))"
         (select users
                 (where {:name [like "chris"]}))
         "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"name\" LIKE ?)"
         (select users
                 (where (or {:name [like "chris"]}
                            (like :name "john"))))
         "SELECT \"users\".* FROM \"users\" WHERE ((\"users\".\"name\" LIKE ?) OR \"users\".\"name\" LIKE ?)")))

(deftest where-edge-cases
  (sql-only
   (are [query result] (= query result)
        (select users (where {}))
        "SELECT \"users\".* FROM \"users\"")))

(deftest with-many
  (with-out-str
    (dry-run
      (is (= (select user2
                     (with email))
             [{:id 1 :email [{:id 1}]}])))))

(deftest with-one
  (sql-only
    (is (= (select user2
                   (with address)
                   (fields :address.state :name))
           "SELECT \"address\".\"state\", \"users\".\"name\" FROM \"users\" LEFT JOIN \"address\" ON \"users\".\"id\" = \"address\".\"users_id\""))))

(deftest join-order
  (sql-only
    (is (= (select users
                   (join :user2 (= :users.id :user2.users_id))
                   (join :user3 (= :users.id :user3.users_id)))
           "SELECT \"users\".* FROM \"users\" LEFT JOIN \"user2\" ON \"users\".\"id\" = \"user2\".\"users_id\" LEFT JOIN \"user3\" ON \"users\".\"id\" = \"user3\".\"users_id\""))))

(deftest join-with-map
  (sql-only
    (are [query result] (= query result)
         (select :blah (join :cool {:cool.id :blah.id}))
         "SELECT \"blah\".* FROM \"blah\" LEFT JOIN \"cool\" ON (\"cool\".\"id\" = \"blah\".\"id\")")))

(deftest aggregate-group
  (sql-only
    (is (= (select users (group :id :name))
           "SELECT \"users\".* FROM \"users\" GROUP BY \"users\".\"id\", \"users\".\"name\""))
    (is (= (select users (aggregate (count :*) :cnt :id))
           "SELECT COUNT(\"users\".*) \"cnt\" FROM \"users\" GROUP BY \"users\".\"id\""))))

(deftest quoting
  (sql-only
    (is (= (select users (fields :testField :t!))
           "SELECT \"users\".\"testField\", \"users\".\"t!\" FROM \"users\""))))

(deftest sqlfns
  (sql-only
    (is (= (select users
                   (fields [(sqlfn now) :now] (sqlfn max :blah) (sqlfn avg (sqlfn sum 3 4) (sqlfn sum 4 5)))
                   (where {:time [>= (sqlfn now)]}))
           "SELECT NOW() \"now\", MAX(\"users\".\"blah\"), AVG(SUM(?, ?), SUM(?, ?)) FROM \"users\" WHERE (\"users\".\"time\" >= NOW())"))))

(deftest join-ent-directly
  (sql-only
    (is (= (select user2
                   (join address))
           "SELECT \"users\".* FROM \"users\" LEFT JOIN \"address\" ON \"users\".\"id\" = \"address\".\"users_id\""))))

(deftest new-with
  (sql-only
    (are [query result] (= query result)

         (select user2
                 (fields :*)
                 (with address (fields :id)))
         "SELECT \"users\".*, \"address\".\"id\" FROM \"users\" LEFT JOIN \"address\" ON \"users\".\"id\" = \"address\".\"users_id\""

         (select user2
                 (fields :*)
                 (with address
                   (with state (where {:state "nc"}))
                   (where {:id [> 5]})))
         "SELECT \"users\".*, \"address\".*, \"state\".* FROM \"users\" LEFT JOIN \"address\" ON \"users\".\"id\" = \"address\".\"users_id\" LEFT JOIN \"state\" ON \"state\".\"id\" = \"address\".\"state_id\" WHERE (\"state\".\"state\" = ?) AND (\"address\".\"id\" > ?)"

         ;;Ensure that params are still ordered correctly
         (query-only
           (:params
             (select user2
                     (fields :*)
                     (with address
                       (with state (where {:state "nc"}))
                       (where (> :id 5))))))
         ["nc" 5]

         ;;Validate has-many executes the second query
         (dry-run
           (with-out-str
             (select user2
                     (with email
                       (where (like :email "%@gmail.com"))))))
         "dry run :: SELECT \"users\".* FROM \"users\" :: []\ndry run :: SELECT \"email\".* FROM \"email\" WHERE \"email\".\"email\" LIKE ? AND (\"email\".\"users_id\" = ?) :: [%@gmail.com 1]\n")))

(deftest modifiers
  (sql-only
    (are [query result] (= query result)
         (-> (select* "users")
           (fields :name)
           (modifier "DISTINCT")
           (as-sql))
    "SELECT DISTINCT \"users\".\"name\" FROM \"users\""
    (select user2 (modifier "TOP 5"))
    "SELECT TOP 5 \"users\".* FROM \"users\"")))

(deftest delimiters
  (set-delimiters "`")
  (sql-only
    (is (= (select user2)
           "SELECT `users`.* FROM `users`")))
  (set-delimiters "\""))

(deftest naming-delim-options
  (sql-only
    (is (= (select delims)
           "SELECT DELIMS.* FROM DELIMS"))))

(deftest false-set-in-update
  (sql-only
    (are [query result] (= query result)
         (update user2 (set-fields {:blah false}))
         "UPDATE \"users\" SET \"blah\" = FALSE"

         (update user2 (set-fields {:blah nil}))
         "UPDATE \"users\" SET \"blah\" = NULL"

         (update user2 (set-fields {:blah true}))
         "UPDATE \"users\" SET \"blah\" = TRUE")))

(deftest raws
  (sql-only
    (is (= (select user2 (where {(raw "ROWNUM") [>= 5]})))
        "SELECT \"users\".* FROM \"users\" WHERE ROWNUM >= ?")))

(deftest pk-dry-run
  (let [result (with-out-str
                 (dry-run
                   (select blah (with users))))]

    (is (= result
           "dry run :: SELECT \"blah\".* FROM \"blah\" :: []\ndry run :: SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"cool_id\" = ?) :: [1]\n"))))

(deftest subselects
  (are [query result] (= query result)
       (sql-only
         (select users
                 (where {:id [in (subselect users
                                            (where {:age [> 5]}))]})))
       "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"id\" IN (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"age\" > ?)))"

       (sql-only
         (select users
                 (from [(subselect users
                                   (where {:age [> 5]})) :u2])))
       "SELECT \"users\".* FROM \"users\", (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"age\" > ?)) \"u2\""

       (sql-only
         (select users
                 (fields :* [(subselect users
                                        (where {:age [> 5]})) :u2])))
       "SELECT \"users\".*, (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"age\" > ?)) \"u2\" FROM \"users\""

       (query-only
         (:params
           (select users
                   (where {:logins [> 10]})
                   (where {:id [in (subselect users
                                              (where {:age [> 5]}))]})
                   (where {:email [like "%@gmail.com"]}))))
       [10 5 "%@gmail.com"]))

(deftest select-query-object
  (are [query result] (= query result)
       (sql-only (select (-> (select* "blah")
                           (where {:id 4}))))
       "SELECT \"blah\".* FROM \"blah\" WHERE (\"blah\".\"id\" = ?)"))

(deftest multiple-aggregates
  (defentity the_table)
  (is (= (sql-only
           (-> (select* the_table)
             (aggregate (min :date_created) :start_date)
             (aggregate (max :date_created) :end_date)
             (where {:id [in [1 2 3]]})
             (exec)))
         "SELECT MIN(\"the_table\".\"date_created\") \"start_date\", MAX(\"the_table\".\"date_created\") \"end_date\" FROM \"the_table\" WHERE (\"the_table\".\"id\" IN (?, ?, ?))")))

(deftest not-in
  (defentity the_table)
  (is (= (sql-only
           (-> (select* the_table)
             (where {:id [not-in [1 2 3]]})
             (exec)))
         "SELECT \"the_table\".* FROM \"the_table\" WHERE (\"the_table\".\"id\" NOT IN (?, ?, ?))")))

(deftest subselect-table-prefix
  (defentity first_table)
  (is (= (sql-only
           (select first_table
                   (where {:first_table_column
                           (subselect :second_table
                                      (fields :second_table_column)
                                      (where {:second_table_column 1}))})))
         "SELECT \"first_table\".* FROM \"first_table\" WHERE (\"first_table\".\"first_table_column\" = (SELECT \"second_table\".\"second_table_column\" FROM \"second_table\" WHERE (\"second_table\".\"second_table_column\" = ?)))")))

(deftest entity-as-subselect
  (defentity subsel
    (table (subselect "test") :test))

  ;;This kind of entity needs and alias.
  (is (thrown? Exception
               (defentity subsel2
                 (table (subselect "test")))))

  (are [query result] (= query result)
       (sql-only
         (select subsel))
       "SELECT \"test\".* FROM (SELECT \"test\".* FROM \"test\") \"test\""))

(deftest multiple-aliases
  (defentity blahblah
    (table :blah :bb))

  (sql-only
    (are [query result] (= query result)
         (select blahblah (join [blahblah :not-bb] (= :bb.cool :not-bb.cool2)))
         "SELECT \"bb\".* FROM \"blah\" \"bb\" LEFT JOIN \"blah\" \"not-bb\" ON \"bb\".\"cool\" = \"not-bb\".\"cool2\"")))

(deftest empty-in-clause
  (sql-only
    (are [query result] (= query result)
         (select :test (where {:cool [in [1]]}))
         "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"cool\" IN (?))"

         (select :test (where {:cool [in []]}))
         "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"cool\" IN (NULL))"
         )))


(deftest prepare-filter
  (defn reverse-strings [values]
    (apply merge (for [[k v] values :when (string? v)] {k (apply str (reverse v))})))

  (defentity reversed-users
    (table :users)
    (prepare reverse-strings))

  (query-only
    (is (= (-> (insert reversed-users (values {:first "chris" :last "granger"})) :params)
           ["sirhc" "regnarg"]))))

(deftest predicates-used-with-brackets
  (sql-only
   (are [query result] (= query result)
        (select :test (where {:id [= 1]}))
        "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" = ?)"
        (select :test (where {:id [< 10]}))
        "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" < ?)"
        (select :test (where {:id [<= 10]}))
        "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" <= ?)"

        ;; clearly this is not an intended use of 'or'!
        (select :test (where {:id [or [1 2 3]]}))
        "SELECT \"test\".* FROM \"test\" WHERE ((\"test\".\"id\" OR (?, ?, ?)))"

        (select :test (where {:id [not-in [1 2 3]]}))
        "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" NOT IN (?, ?, ?))"
        (select :test (where {:id [not= 1]}))
        "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" != ?)"
        )))
