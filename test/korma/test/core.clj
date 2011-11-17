(ns korma.test.core
  (:use [korma.core])
  (:use [clojure.test]))

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

(defentity blah (pk :cool) (has-many users))

(deftest select-function
         (is (= (-> (select* "users")
                  (fields :id :username)
                  (where {:username "chris"})
                  (order :created)
                  (limit 5)
                  (offset 3)
                  (as-sql))
                "SELECT \"users\".\"id\", \"users\".\"username\" FROM \"users\" WHERE (\"users\".\"username\" = ?) ORDER BY \"users\".\"created\" DESC LIMIT 5 OFFSET 3")))


(deftest simple-selects
         (sql-only
           (are [query result] (= query result)
                (select users)
                "SELECT \"users\".* FROM \"users\""
                (select users-alias)
                "SELECT \"u\".* FROM \"users\" AS \"u\""
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
                "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"username\" = ?) ORDER BY \"users\".\"created\" DESC"
                (select users
                        (where {:active true})
                        (order :created)
                        (limit 5)
                        (offset 3))
                "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"active\" = TRUE) ORDER BY \"users\".\"created\" DESC LIMIT 5 OFFSET 3")))

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
                "INSERT INTO \"users\" (\"last\", \"first\") VALUES (?, ?)")))

(deftest insert-queries
         (sql-only
           (are [query result] (= query result)
                (insert users
                        (values {:first "chris" :last "granger"}))
                "INSERT INTO \"users\" (\"last\", \"first\") VALUES (?, ?)"
                (insert users
                        (values [{:first "chris" :last "granger"}
                                 {:last "jordan" :first "michael"}]))
                "INSERT INTO \"users\" (\"last\", \"first\") VALUES (?, ?), (?, ?)")))

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
                    (join :user2 :users.id :user2.users_id)
                    (join :user3 :users.id :user3.users_id))
                  "SELECT \"users\".* FROM \"users\" LEFT JOIN \"user2\" ON \"users\".\"id\" = \"user2\".\"users_id\" LEFT JOIN \"user3\" ON \"users\".\"id\" = \"user3\".\"users_id\""))))

(deftest aggregate-group
         (sql-only
           (is (= (select users (group :id :name))
                  "SELECT \"users\".* FROM \"users\" GROUP BY \"users\".\"id\", \"users\".\"name\""))
           (is (= (select users (aggregate (count :*) :cnt :id))
                  "SELECT COUNT(\"users\".*) AS \"cnt\" FROM \"users\" GROUP BY \"users\".\"id\""))))

(deftest quoting
         (sql-only
           (is (= (select users (fields :testField :t!))
                  "SELECT \"users\".\"testField\", \"users\".\"t!\" FROM \"users\""))))

(deftest sqlfns
         (sql-only
           (is (= (select users 
                    (fields [(sqlfn now) :now] (sqlfn max :blah) (sqlfn avg (sqlfn sum 3 4) (sqlfn sum 4 5)))
                    (where {:time [>= (sqlfn now)]}))
                  "SELECT NOW() AS \"now\", MAX(\"users\".\"blah\"), AVG(SUM(?, ?), SUM(?, ?)) FROM \"users\" WHERE (\"users\".\"time\" >= NOW())"))))

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
                "SELECT \"users\".* FROM \"users\" LEFT JOIN \"address\" ON \"users\".\"id\" = \"address\".\"users_id\" LEFT JOIN \"state\" ON \"state\".\"id\" = \"address\".\"state_id\" WHERE (\"state\".\"state\" = ?) AND (\"address\".\"id\" > ?)"

                ;;Ensure that params are still ordered correctly
                (:params (-> (select* user2)
                             (fields :*)
                             (with address
                                   (with state (where {:state "nc"}))
                                   (where (> :id 5)))))
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
           (is (= (select user2 (modifier "TOP 5")))
               "SELECT TOP 5 \"users\".* FROM \"users\"")))

(deftest delimiters
         (set-delimiters "`")
         (sql-only
           (is (= (select user2)
                  "SELECT `users`.* FROM `users`")))
         (set-delimiters "\""))

(deftest raws
         (sql-only
           (is (= (select user2 (where {(raw "ROWNUM") [>= 5]})))
               "SELECT \"users\".* FROM \"users\" WHERE ROWNUM >= ?")))

(deftest pk-dry-run
         (let [result (with-out-str
                        (dry-run
                          (select blah (with users))))]

           (is (= result
                  "dry run :: SELECT \"blah\".* FROM \"blah\" :: []\ndry run :: SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"blah_id\" = ?) :: [1]\n"))))
