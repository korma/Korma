(ns korma.test.core
  (:refer-clojure :exclude [update])
  (:require [clojure.string :as string])
  (:use clojure.test
        korma.core
        korma.db))

(defdb test-db-opts (postgres {:db "korma" :user "korma" :password "kormapass" :delimiters "" :naming {:fields string/upper-case}}))
(defdb test-db (postgres {:db "korma" :user "korma" :password "kormapass"}))
(defdb test-db-alias (postgres {:db "korma" :user "korma" :password "kormapass" :alias-delimiter " "}))

(defdb mem-db (h2 {:db "mem:test"}))

(use-fixtures :once
  (fn [f]
    (default-connection mem-db)
    (f)))

(defentity delims
  (database test-db-opts))

(defentity alias-entity
  (table :alias :a)
  (database test-db-alias))

(defentity users)
(defentity state)
(defentity address
  (belongs-to state))
(defentity email)

(defentity user2
  (table :users)
  (has-one address)
  (has-many email))

(create-ns 'f)
(intern 'f 'friend (create-entity "friend"))

(defentity users-with-friend
  (table :users)
  (has-one f/friend))

(defentity users-alias
  (table :users :u))

(defentity users-with-entity-fields
  (table :users)
  (entity-fields :id :username))

(defentity ^{:private true} blah (pk :cool) (has-many users (fk :cool_id)))

(deftest test-defentity-accepts-metadata
  (is (= true (:private (meta #'blah)))))

(deftest select-function
  (is (= "SELECT \"users\".\"id\", \"users\".\"username\" FROM \"users\" WHERE (\"users\".\"username\" = ?) ORDER BY \"users\".\"created\" ASC LIMIT 5 OFFSET 3"
         (-> (select* "users")
             (fields :id :username)
             (where {:username "chris"})
             (order :created)
             (limit 5)
             (offset 3)
             as-sql))))


(deftest simple-selects
  (sql-only
   (are [query result] (= query result)
        (select users)
        "SELECT \"users\".* FROM \"users\""
        (select users-alias)
        "SELECT \"u\".* FROM \"users\" AS \"u\""
        (select users-with-entity-fields)
        "SELECT \"users\".\"id\", \"users\".\"username\" FROM \"users\""
        (select users-with-friend (with f/friend))
        "SELECT \"users\".*, \"friend\".* FROM \"users\" LEFT JOIN \"friend\" ON (\"friend\".\"users_id\" = \"users\".\"id\")"
        (select users
                (fields :id :username))
        "SELECT \"users\".\"id\", \"users\".\"username\" FROM \"users\""
        (select users
                (where {:username "chris"
                        :email "hey@hey.com"}))
        "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"email\" = ? AND \"users\".\"username\" = ?)"
        (select users
                (where {:username "chris"})
                (order :created))
        "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"username\" = ?) ORDER BY \"users\".\"created\" ASC"
        (select users
                (where {:active true})
                (order :created)
                (limit 5)
                (offset 3))
        "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"active\" = ?) ORDER BY \"users\".\"created\" ASC LIMIT 5 OFFSET 3")))

(deftest update-function
  (is (= "UPDATE \"users\" SET \"first\" = ?, \"last\" = ? WHERE (\"users\".\"id\" = ?)"
         (-> (update* "users")
             (set-fields {:first "chris"
                          :last "granger"})
             (where {:id 3})
             as-sql))))

(deftest update-queries
  (sql-only
   (are [result query] (= result query)
        "UPDATE \"users\" SET \"first\" = ?"
        (update users
                (set-fields {:first "chris"}))
        "UPDATE \"users\" SET \"first\" = ? WHERE (\"users\".\"id\" = ?)"
        (update users
                (set-fields {:first "chris"})
                (where {:id 3}))
        "UPDATE \"users\" SET \"first\" = ?, \"last\" = ? WHERE (\"users\".\"id\" = ?)"
        (update users
                (set-fields {:first "chris"
                             :last "granger"})
                (where {:id 3})))))

(deftest delete-function
  (is (= "DELETE FROM \"users\" WHERE (\"users\".\"id\" = ?)"
         (-> (delete* "users")
             (where {:id 3})
             as-sql))))

(deftest delete-queries
  (sql-only
   (are [result query] (= result query)
        "DELETE FROM \"users\""
        (delete users)
        "DELETE FROM \"users\" WHERE (\"users\".\"id\" = ?)"
        (delete users
                (where {:id 3})))))

(deftest insert-function
  (is (= "INSERT INTO \"users\" (\"first\", \"last\") VALUES (?, ?)"
         (-> (insert* "users")
             (values {:first "chris" :last "granger"})
             as-sql)))

  (testing "WHEN values is empty THEN generates a NOOP SQL statement"
    (is (= "DO 0"
           (-> (insert* "users")
               (values {})
               as-sql)))))

(deftest insert-queries
  (sql-only
   (are [result query] (= result query)
        "INSERT INTO \"users\" (\"first\", \"last\") VALUES (?, ?)"
        (insert users
                (values {:first "chris" :last "granger"}))
        "INSERT INTO \"users\" (\"first\", \"last\") VALUES (?, ?), (?, ?)"
        (insert users
                (values [{:first "chris" :last "granger"}
                         {:last "jordan" :first "michael"}]))
        "DO 0"
        (insert users (values {}))
        "DO 0"
        (insert users (values []))
        "DO 0"
        (insert users (values [{} {}])))))

(deftest complex-where
  (sql-only
   (are [query result] (= query result)
        "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"name\" = ? OR \"users\".\"name\" = ?)"
        (select users
                (where (or (= :name "chris")
                           (= :name "john"))))
        "SELECT \"users\".* FROM \"users\" WHERE ((\"users\".\"name\" = ?) OR (\"users\".\"name\" = ?))"
        (select users
                (where (or {:name "chris"}
                           {:name "john"})))
        "SELECT \"users\".* FROM \"users\" WHERE ((\"users\".\"last\" = ? AND \"users\".\"name\" = ?) OR (\"users\".\"email\" = ?) OR \"users\".\"age\" > ?)"
        (select users
                (where (or {:name "drew"
                            :last "dreward"}
                           {:email "drew@drew.com"}
                           (> :age 10))))
        "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"x\" < ? OR (\"users\".\"y\" < ? OR \"users\".\"z\" > ?))"
        (select users
                (where (or (< :x 5)
                           (or (< :y 3)
                               (> :z 4)))))
        "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"name\" LIKE ?)"
        (select users
                (where {:name [like "chris"]}))
        "SELECT \"users\".* FROM \"users\" WHERE ((\"users\".\"name\" LIKE ?) OR \"users\".\"name\" LIKE ?)"
        (select users
                (where (or {:name [like "chris"]}
                           (like :name "john")))))))

(deftest where-edge-cases
  (sql-only
   (are [result query] (= result query)
        "SELECT \"users\".* FROM \"users\""
        (select users (where {})))))

(deftest with-many
  (with-out-str
    (dry-run
     (is (= [{:id 1 :email [{:id 1}]}]
            (select user2
                    (with email)))))))

(deftest with-has-many-composite-key
  (defentity email-ck)
  (defentity user-ck
    (pk :id :id2)
    (has-many email-ck (fk :uid :uid2)))
  (is (= "dry run :: SELECT \"user-ck\".* FROM \"user-ck\" :: []\ndry run :: SELECT \"email-ck\".* FROM \"email-ck\" WHERE (\"email-ck\".\"uid\" = ? AND \"email-ck\".\"uid2\" = ?) :: [1 1]\n"
      (with-out-str
        (dry-run
          (select user-ck
                  (with email-ck)))))))

(deftest with-many-batch
  (is (= "dry run :: SELECT \"users\".* FROM \"users\" :: []\ndry run :: SELECT \"email\".* FROM \"email\" WHERE (\"email\".\"users_id\" IN (?)) :: [1]\n"
         (with-out-str
           (dry-run
            (select user2
                    (with-batch email)))))))

(deftest with-many-batch-composite-key
  (defentity email-batch-ck
    (table :email))
  (defentity user-batch-ck
    (table :users)
    (pk :user_id1 :user_id2)
    (has-many email-batch-ck
              (fk :email_user_id1 :email_user_id2)))
  (is (= "dry run :: SELECT \"users\".* FROM \"users\" :: []\ndry run :: SELECT \"email\".* FROM \"email\" WHERE ((\"email\".\"email_user_id1\" = ? AND \"email\".\"email_user_id2\" = ?)) :: [1 1]\n"
         (with-out-str
           (dry-run
             (select user-batch-ck
                     (with-batch email-batch-ck)))))))

(deftest with-one
  (sql-only
   (is (= "SELECT \"address\".\"state\", \"users\".\"name\" FROM \"users\" LEFT JOIN \"address\" ON (\"address\".\"users_id\" = \"users\".\"id\")"
          (select user2
                  (with address)
                  (fields :address.state :name))))))

(deftest with-has-one-composite-key
  (defentity address-ck)
  (defentity user-ck
    (pk :id :id2)
    (has-one address-ck (fk :fkid :fkid2)))
  (sql-only
   (is (= (str "SELECT \"user-ck\".*, \"address-ck\".* FROM \"user-ck\" "
               "LEFT JOIN \"address-ck\" ON (\"address-ck\".\"fkid\" = \"user-ck\".\"id\" "
               "AND \"address-ck\".\"fkid2\" = \"user-ck\".\"id2\")")
          (select user-ck
                  (with address-ck))))))

(deftest join-has-one-composite-key
  (defentity address-ck)
  (defentity user-ck
    (pk :id :id2)
    (has-one address-ck (fk :fkid :fkid2)))
  (sql-only
   (is (= (str "SELECT \"user-ck\".* FROM \"user-ck\" "
               "LEFT JOIN \"address-ck\" ON (\"user-ck\".\"id\" = \"address-ck\".\"fkid\" "
               "AND \"user-ck\".\"id2\" = \"address-ck\".\"fkid2\")")
          (select user-ck
                  (join address-ck))))))

(deftest with-belongs-to-composite-key
  (defentity user-ck
    (pk :id :id2))
  (defentity address-ck
    (belongs-to user-ck (fk :fkid :fkid2)))
  (sql-only
   (is (= (str "SELECT \"address-ck\".*, \"user-ck\".* FROM \"address-ck\" "
               "LEFT JOIN \"user-ck\" ON (\"user-ck\".\"id\" = \"address-ck\".\"fkid\" "
               "AND \"user-ck\".\"id2\" = \"address-ck\".\"fkid2\")")
          (select address-ck
                  (with user-ck))))))

(deftest join-belongs-to-composite-key
  (defentity user-ck
    (pk :id :id2))
  (defentity address-ck
    (belongs-to user-ck (fk :fkid :fkid2)))
  (sql-only
   (is (= (str "SELECT \"address-ck\".* FROM \"address-ck\" "
               "LEFT JOIN \"user-ck\" ON (\"user-ck\".\"id\" = \"address-ck\".\"fkid\" "
               "AND \"user-ck\".\"id2\" = \"address-ck\".\"fkid2\")")
          (select address-ck
                  (join user-ck))))))

(deftest join-order
  (sql-only
   (is (= "SELECT \"users\".* FROM (\"users\" LEFT JOIN \"user2\" ON \"users\".\"id\" = \"user2\".\"users_id\") LEFT JOIN \"user3\" ON \"users\".\"id\" = \"user3\".\"users_id\""
          (select users
                  (join :user2 (= :users.id :user2.users_id))
                  (join :user3 (= :users.id :user3.users_id)))))))

(deftest join-with-map
  (sql-only
   (are [result query] (= result query)
        "SELECT \"blah\".* FROM \"blah\" LEFT JOIN \"cool\" ON (\"cool\".\"id\" = \"blah\".\"id\")"
        (select :blah (join :cool {:cool.id :blah.id})))))

(deftest aggregate-group
  (sql-only
   (is (= "SELECT \"users\".* FROM \"users\" GROUP BY \"users\".\"id\", \"users\".\"name\""
          (select users (group :id :name))))
   (is (= "SELECT COUNT(*) AS \"cnt\" FROM \"users\" GROUP BY \"users\".\"id\""
          (select users (aggregate (count :*) :cnt :id))))
   (is (= "SELECT COUNT(*) AS \"cnt\" FROM \"users\" GROUP BY \"users\".\"id\" HAVING (\"users\".\"id\" = ?)"
          (select users
                  (aggregate (count :*) :cnt :id)
                  (having {:id 5}))))))

(deftest aggregate-stdev
  (sql-only
   (is (= "SELECT STDEV(\"users\".\"age\") AS \"DevAge\" FROM \"users\""
          (select users (aggregate (stdev :age) :DevAge))))))

(deftest aggregate-count
  (sql-only
    (testing "asterisk is not prefixed with table when used as count column"
      (is (= "SELECT COUNT(*) AS \"cnt\" FROM \"users\""
             (select users (aggregate (count :*) :cnt)))))
    (testing "column names as prefixed when used as count column"
      (is (= "SELECT COUNT(\"users\".\"age\") AS \"cnt\" FROM \"users\""
             (select users (aggregate (count :age) :cnt)))))))

(deftest quoting
  (sql-only
   (is (= "SELECT \"users\".\"testField\", \"users\".\"t!\" FROM \"users\""
          (select users (fields :testField :t!))))))

(deftest sqlfns
  (sql-only
   (is (= "SELECT NOW() AS \"now\", MAX(\"users\".\"blah\"), AVG(SUM(?, ?), SUM(?, ?)) FROM \"users\" WHERE (\"users\".\"time\" >= NOW())"
          (select users
                  (fields [(sqlfn now) :now] (sqlfn max :blah) (sqlfn avg (sqlfn sum 3 4) (sqlfn sum 4 5)))
                  (where {:time [>= (sqlfn now)]}))))))

(deftest join-ent-directly
  (sql-only
   (is (= "SELECT \"users\".* FROM \"users\" LEFT JOIN \"address\" ON (\"users\".\"id\" = \"address\".\"users_id\")"
          (select user2
                  (join address))))))

(deftest left-join-ent-directly
  (sql-only
   (is (= "SELECT \"users\".* FROM \"users\" LEFT JOIN \"address\" ON (\"users\".\"id\" = \"address\".\"users_id\")"
          (select user2
                  (join :left address))))))

(deftest inner-join-ent-directly
  (sql-only
   (is (= "SELECT \"users\".* FROM \"users\" INNER JOIN \"address\" ON (\"users\".\"id\" = \"address\".\"users_id\")"
          (select user2
                  (join :inner address))))))

(deftest new-with
  (sql-only
   (are [result query] (= result query)

        "SELECT \"users\".*, \"address\".\"id\" FROM \"users\" LEFT JOIN \"address\" ON (\"address\".\"users_id\" = \"users\".\"id\")"
        (select user2
                (fields :*)
                (with address (fields :id)))

        "SELECT \"users\".*, \"address\".*, \"state\".* FROM (\"users\" LEFT JOIN \"address\" ON (\"address\".\"users_id\" = \"users\".\"id\")) LEFT JOIN \"state\" ON (\"state\".\"id\" = \"address\".\"state_id\") WHERE (\"state\".\"state\" = ?) AND (\"address\".\"id\" > ?)"
        (select user2
                (fields :*)
                (with address
                      (with state (where {:state "nc"}))
                      (where {:id [> 5]})))

        ;;Ensure that params are still ordered correctly
        ["nc" 5]
        (query-only
         (:params
          (select user2
                  (fields :*)
                  (with address
                        (with state (where {:state "nc"}))
                        (where (> :id 5))))))

        ;;Validate has-many executes the second query
        "dry run :: SELECT \"users\".* FROM \"users\" :: []\ndry run :: SELECT \"email\".* FROM \"email\" WHERE \"email\".\"email\" LIKE ? AND (\"email\".\"users_id\" = ?) :: [%@gmail.com 1]\n"
        (dry-run
         (with-out-str
           (select user2
                   (with email
                         (where (like :email "%@gmail.com"))))))

        "dry run :: SELECT \"users\".* FROM \"users\" :: []\ndry run :: SELECT \"email\".* FROM \"email\" WHERE \"email\".\"email\" LIKE ? AND (\"email\".\"users_id\" IN (?)) :: [%@gmail.com 1]\n"

        (dry-run
         (with-out-str
           (select user2
                   (with-batch email
                     (where (like :email "%@gmail.com")))))))))

(deftest modifiers
  (sql-only
   (are [result query] (= result query)
        "SELECT DISTINCT \"users\".\"name\" FROM \"users\""
        (-> (select* "users")
            (fields :name)
            (modifier "DISTINCT")
            as-sql)
        "SELECT TOP 5 \"users\".* FROM \"users\""
        (select user2 (modifier "TOP 5")))))

(deftest delimiters
  (defdb delimdb
    {:delimiters "`"})
  (defentity delimuser
    (table :users)
    (database delimdb)
    (has-one address)
    (has-many email))
  (sql-only
    (is (= "SELECT `users`.* FROM `users`"
           (select delimuser))))
  (default-connection mem-db))

(deftest naming-delim-options
  (sql-only
   (is (= "SELECT DELIMS.* FROM DELIMS"
          (select delims)))))

(deftest alias-delimiter-options
  (sql-only
   (is (= "SELECT \"a\".* FROM \"alias\" \"a\""
          (select alias-entity)))))

(deftest alias-delimiter-options-with-fields
  (sql-only
   (is (= "SELECT \"a\".\"foo\" \"a\", \"a\".\"bar\" \"b\" FROM \"alias\" \"a\""
          (select
           alias-entity
           (fields [:foo :a] [:bar :b]))))))

(deftest false-set-in-update
  (query-only
   (are [result query] (= result (select-keys query [:sql-str :params]))
        {:sql-str "UPDATE \"users\" SET \"blah\" = ?" :params [false]}
        (update user2 (set-fields {:blah false}))

        {:sql-str "UPDATE \"users\" SET \"blah\" = NULL" :params []}
        (update user2 (set-fields {:blah nil}))

        {:sql-str "UPDATE \"users\" SET \"blah\" = ?" :params [true]}
        (update user2 (set-fields {:blah true})))))

(deftest raws
  (sql-only
   (are [result query] (= result query)
        "SELECT \"users\".* FROM \"users\" WHERE (ROWNUM >= ?)"
        (select user2 (where {(raw "ROWNUM") [>= 5]}))

        "SELECT \"users\".* FROM \"users\" WHERE (MONTH(create_date) = ? AND YEAR(create_date) = ?)"
        (select user2 (where {(raw "YEAR(create_date)")  2013
                              (raw "MONTH(create_date)") 12})))))

(deftest pk-dry-run
  (let [result (with-out-str
                 (dry-run
                  (select blah (with users))))]

    (is (= "dry run :: SELECT \"blah\".* FROM \"blah\" :: []\ndry run :: SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"cool_id\" = ?) :: [1]\n"
           result))))

(deftest subselects
  (are [result query] (= result query)
       "SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"id\" IN (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"age\" > ?)))"
       (sql-only
        (select users
                (where {:id [in (subselect users
                                           (where {:age [> 5]}))]})))

       "SELECT \"state\".* FROM \"state\" WHERE EXISTS((SELECT \"address\".* FROM \"address\" WHERE ((\"address\".\"id\" > ?) AND \"address\".\"state_id\" = \"state\".\"id\")))"
       (sql-only
        (select state
                (where (exists (subselect address
                                          (where (and {:id [> 5]} (= :state_id :state.id))))))))

       "SELECT \"state\".* FROM \"state\" WHERE (EXISTS((SELECT \"address\".* FROM \"address\" WHERE ((\"address\".\"id\" > ?) AND \"address\".\"state_id\" = \"state\".\"id\"))) AND NOT(EXISTS((SELECT \"address\".* FROM \"address\" WHERE ((\"address\".\"id\" < ?) OR \"address\".\"state_id\" <> \"state\".\"id\")))))"
       (sql-only
        (select state
                (where (and (exists (subselect address
                                               (where (and {:id [> 5]} (= :state_id :state.id)))))
                            (not (exists (subselect address
                                                    (where (or  {:id [< 10]} (not= :state_id :state.id))))))))))

       "SELECT \"state\".* FROM \"state\" WHERE EXISTS((SELECT \"address\".* FROM \"address\" WHERE ((\"address\".\"id\" > ?) AND NOT(EXISTS((SELECT \"users\".* FROM \"users\" WHERE \"address\".\"user_id\" = \"users\".\"id\"))))))"
       (sql-only
        (select state
                (where (exists (subselect address
                                          (where (and {:id [> 5]}
                                                      (not (exists (subselect user2 (where (= :address.user_id :id))))))))))))

       "SELECT \"users\".* FROM \"users\", (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"age\" > ?)) AS \"u2\""
       (sql-only
        (select users
                (from [(subselect users
                                  (where {:age [> 5]})) :u2])))

       "SELECT \"users\".*, (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"age\" > ?)) AS \"u2\" FROM \"users\""
       (sql-only
        (select users
                (fields :* [(subselect users
                                       (where {:age [> 5]})) :u2])))

       [10 5 "%@gmail.com"]
       (query-only
        (:params
         (select users
                 (where {:logins [> 10]})
                 (where {:id [in (subselect users
                                            (where {:age [> 5]}))]})
                 (where {:email [like "%@gmail.com"]}))))))

(deftest select-query-object
  (are [query result] (= query result)
       "SELECT \"blah\".* FROM \"blah\" WHERE (\"blah\".\"id\" = ?)"
       (sql-only (select (-> (select* "blah")
                             (where {:id 4}))))))

(deftest multiple-aggregates
  (defentity the_table)
  (is (= "SELECT MIN(\"the_table\".\"date_created\") AS \"start_date\", MAX(\"the_table\".\"date_created\") AS \"end_date\" FROM \"the_table\" WHERE (\"the_table\".\"id\" IN (?, ?, ?))"
         (sql-only
          (-> (select* the_table)
              (aggregate (min :date_created) :start_date)
              (aggregate (max :date_created) :end_date)
              (where {:id [in [1 2 3]]})
              (exec))))))

(deftest not-in
  (defentity the_table)
  (is (= "SELECT \"the_table\".* FROM \"the_table\" WHERE (\"the_table\".\"id\" NOT IN (?, ?, ?))"
         (sql-only
          (-> (select* the_table)
              (where {:id [not-in [1 2 3]]})
              (exec))))))

(deftest subselect-table-prefix
  (defentity first_table)
  (is (= "SELECT \"first_table\".* FROM \"first_table\" WHERE (\"first_table\".\"first_table_column\" = (SELECT \"second_table\".\"second_table_column\" FROM \"second_table\" WHERE (\"second_table\".\"second_table_column\" = ?)))"
         (sql-only
          (select first_table
                  (where {:first_table_column
                          (subselect :second_table
                                     (fields :second_table_column)
                                     (where {:second_table_column 1}))}))))))

(deftest entity-as-subselect
  (defentity subsel
    (table (subselect "test") :test))

  ;;This kind of entity needs and alias.
  (is (thrown? Exception
               (defentity subsel2
                 (table (subselect "test")))))

  (are [result query] (= result query)
       "SELECT \"test\".* FROM (SELECT \"test\".* FROM \"test\") AS \"test\""
       (sql-only
        (select subsel))))

(deftest entity-with-belongs-to-subselect
  (defentity subsel
    (pk :subsel_id)
    (table  (subselect "test") :subseltest))
  (defentity ent
    (table :ent)
    (pk :ent_id)
    (belongs-to subsel  (fk :subsel_ent_id)))
  (are  [result query]  (= result query)
        "SELECT \"ent\".*, \"subseltest\".* FROM \"ent\" LEFT JOIN (SELECT \"test\".* FROM \"test\") AS \"subseltest\" ON (\"subseltest\".\"subsel_id\" = \"ent\".\"subsel_ent_id\")"
        (sql-only
         (select ent
                 (with subsel)))))

(deftest multiple-aliases
  (defentity blahblah
    (table :blah :bb))

  (sql-only
   (are [result query] (= result query)
        "SELECT \"bb\".* FROM \"blah\" AS \"bb\" LEFT JOIN \"blah\" AS \"not-bb\" ON \"bb\".\"cool\" = \"not-bb\".\"cool2\""
        (select blahblah (join [blahblah :not-bb] (= :bb.cool :not-bb.cool2))))))

(deftest empty-in-clause
  (sql-only
   (are [result query] (= result query)
        "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"cool\" IN (?))"
        (select :test (where {:cool [in [1]]}))

        "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"cool\" IN (NULL))"
        (select :test (where {:cool [in []]})))))


(deftest prepare-filter
  (defn reverse-strings [values]
    (apply merge (for [[k v] values :when (string? v)] {k (apply str (reverse v))})))

  (defentity reversed-users
    (table :users)
    (prepare reverse-strings))

  (query-only
   (is (= ["sirhc" "regnarg"]
          (-> (insert reversed-users (values {:first "chris" :last "granger"})) :params)))))

(deftest predicates-used-with-brackets
  (sql-only
   (are [result query] (= result query)
        "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" = ?)"
        (select :test (where {:id [= 1]}))
        "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" < ?)"
        (select :test (where {:id [< 10]}))
        "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" <= ?)"
        (select :test (where {:id [<= 10]}))
        "SELECT \"test\".* FROM \"test\" WHERE ((\"test\".\"id\" BETWEEN ? AND ?))"
        (select :test (where {:id [between [1 10]]}))

        ;; clearly this is not an intended use of 'or'!
        "SELECT \"test\".* FROM \"test\" WHERE ((\"test\".\"id\" OR (?, ?, ?)))"
        (select :test (where {:id [or [1 2 3]]}))

        "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" NOT IN (?, ?, ?))"
        (select :test (where {:id [not-in [1 2 3]]}))
        "SELECT \"test\".* FROM \"test\" WHERE (\"test\".\"id\" <> ?)"
        (select :test (where {:id [not= 1]})))))


;;*****************************************************
;; Supporting Postgres' schema and queries covering multiple databases
;;*****************************************************

(defentity book-with-db (table :korma.book))
(defentity author-with-db (table :other.author) (belongs-to book-with-db))

(defentity book-with-schema (table :korma.myschema.book))
(defentity author-with-schema (table :korma.otherschema.author) (belongs-to book-with-schema))

(deftest dbname-on-tablename
  (are [query result] (= query result)
       (sql-only
        (select author-with-db (with book-with-db)))
       "SELECT \"other\".\"author\".*, \"korma\".\"book\".* FROM \"other\".\"author\" LEFT JOIN \"korma\".\"book\" ON (\"korma\".\"book\".\"id\" = \"other\".\"author\".\"book_id\")"))

(deftest schemaname-on-tablename
  (are [query result] (= query result)
       (sql-only
        (select author-with-schema (with book-with-schema)))
       "SELECT \"korma\".\"otherschema\".\"author\".*, \"korma\".\"myschema\".\"book\".* FROM \"korma\".\"otherschema\".\"author\" LEFT JOIN \"korma\".\"myschema\".\"book\" ON (\"korma\".\"myschema\".\"book\".\"id\" = \"korma\".\"otherschema\".\"author\".\"book_id\")"))


;;*****************************************************
;; Many-to-Many relationships
;;*****************************************************

(declare mtm1 mtm2 mtm3 mtm4)

(defentity mtm1
  (many-to-many mtm2 :mtm1_mtm2 (lfk :mtm1_id)
                                (rfk :mtm2_id)))

(defentity mtm2
  (many-to-many mtm1 :mtm1_mtm2 (lfk :mtm2_id)
                                (rfk :mtm1_id)))

(defentity mtm3
  (pk :id31 :id32)
  (many-to-many mtm4 :mtm3_mtm4 (lfk :mtm3_id31 :mtm3_id32)
                                (rfk :mtm4_id41 :mtm4_id42)))

(defentity mtm4
  (pk :id41 :id42)
  (many-to-many mtm3 :mtm3_mtm4 (lfk :mtm4_id41 :mtm4_id42)
                                (rfk :mtm3_id31 :mtm3_id32)))

(deftest test-many-to-many
  (is (= (str "dry run :: SELECT \"mtm2\".* FROM \"mtm2\" :: []\n"
              "dry run :: SELECT \"mtm1\".* FROM \"mtm1\" "
              "INNER JOIN \"mtm1_mtm2\" ON (\"mtm1_mtm2\".\"mtm1_id\" "
              "= \"mtm1\".\"id\") "
              "WHERE (\"mtm1_mtm2\".\"mtm2_id\" = ?) :: [1]\n")
         (with-out-str (dry-run (select mtm2 (with mtm1)))))))

(deftest test-many-to-many-composite-key
  (is (= (str "dry run :: SELECT \"mtm4\".* FROM \"mtm4\" :: []\n"
              "dry run :: SELECT \"mtm3\".* FROM \"mtm3\" "
              "INNER JOIN \"mtm3_mtm4\" ON ("
              "\"mtm3_mtm4\".\"mtm3_id31\" = \"mtm3\".\"id31\" AND "
              "\"mtm3_mtm4\".\"mtm3_id32\" = \"mtm3\".\"id32\") "
              "WHERE (\"mtm3_mtm4\".\"mtm4_id41\" = ? AND \"mtm3_mtm4\".\"mtm4_id42\" = ?) :: [1 1]\n")
         (with-out-str (dry-run (select mtm4 (with mtm3)))))))

(deftest test-many-to-many-reverse
  (is (= (str "dry run :: SELECT \"mtm1\".* FROM \"mtm1\" :: []\n"
              "dry run :: SELECT \"mtm2\".* FROM \"mtm2\" "
              "INNER JOIN \"mtm1_mtm2\" ON \"mtm1_mtm2\".\"mtm2_id\" "
              "= \"mtm2\".\"id\" "
              "WHERE (\"mtm1_mtm2\".\"mtm1_id\" = ?) :: [1]\n"))
      (with-out-str (dry-run (select mtm1 (with mtm2))))))

(deftest test-many-to-many-reverse-composite-key
  (is (= (str "dry run :: SELECT \"mtm3\".* FROM \"mtm3\" :: []\n"
              "dry run :: SELECT \"mtm4\".* FROM \"mtm4\" "
              "INNER JOIN \"mtm3_mtm4\" ON ("
              "\"mtm3_mtm4\".\"mtm4_id41\" = \"mtm4\".\"id41\" AND "
              "\"mtm3_mtm4\".\"mtm4_id42\" = \"mtm4\".\"id42\") "
              "WHERE (\"mtm3_mtm4\".\"mtm3_id31\" = ? AND \"mtm3_mtm4\".\"mtm3_id32\" = ?) :: [1 1]\n")
         (with-out-str (dry-run (select mtm3 (with mtm4)))))))

(deftest test-many-to-many-join
  (is (= (str "dry run :: SELECT \"mtm2\".* FROM (\"mtm2\" "
              "LEFT JOIN \"mtm1_mtm2\" "
              "ON (\"mtm2\".\"id\" = \"mtm1_mtm2\".\"mtm2_id\")) "
              "LEFT JOIN \"mtm1\" "
              "ON (\"mtm1_mtm2\".\"mtm1_id\" = \"mtm1\".\"id\") :: []\n")
         (with-out-str (dry-run (select mtm2 (join mtm1)))))))

(deftest test-many-to-many-join-composite-key
  (is (= (str "dry run :: SELECT \"mtm4\".* FROM (\"mtm4\" "
              "LEFT JOIN \"mtm3_mtm4\" "
              "ON (\"mtm4\".\"id41\" = \"mtm3_mtm4\".\"mtm4_id41\" "
              "AND \"mtm4\".\"id42\" = \"mtm3_mtm4\".\"mtm4_id42\")) "
              "LEFT JOIN \"mtm3\" "
              "ON (\"mtm3_mtm4\".\"mtm3_id31\" = \"mtm3\".\"id31\" "
              "AND \"mtm3_mtm4\".\"mtm3_id32\" = \"mtm3\".\"id32\") :: []\n")
         (with-out-str (dry-run (select mtm4 (join mtm3)))))))

(deftest test-many-to-many-join-reverse
  (is (= (str "dry run :: SELECT \"mtm1\".* FROM (\"mtm1\" "
              "LEFT JOIN \"mtm1_mtm2\" "
              "ON (\"mtm1\".\"id\" = \"mtm1_mtm2\".\"mtm1_id\")) "
              "LEFT JOIN \"mtm2\" "
              "ON (\"mtm1_mtm2\".\"mtm2_id\" = \"mtm2\".\"id\") :: []\n")
         (with-out-str (dry-run (select mtm1 (join mtm2)))))))

(deftest test-many-to-many-join-reverse-composite-key
  (is (= (str "dry run :: SELECT \"mtm3\".* FROM (\"mtm3\" "
              "LEFT JOIN \"mtm3_mtm4\" "
              "ON (\"mtm3\".\"id31\" = \"mtm3_mtm4\".\"mtm3_id31\" "
              "AND \"mtm3\".\"id32\" = \"mtm3_mtm4\".\"mtm3_id32\")) "
              "LEFT JOIN \"mtm4\" "
              "ON (\"mtm3_mtm4\".\"mtm4_id41\" = \"mtm4\".\"id41\" "
              "AND \"mtm3_mtm4\".\"mtm4_id42\" = \"mtm4\".\"id42\") :: []\n")
         (with-out-str (dry-run (select mtm3 (join mtm4)))))))

;; Entities with many-to-many relationships using default keys.

(declare mtmdk1 mtmdk2)

(defentity mtmdk1
  (many-to-many mtmdk2 :mtmdk1_mtmdk2))

(defentity mtmdk2
  (many-to-many mtmdk1 :mtmdk1_mtmdk2))

(deftest many-to-many-default-keys
  (is (= (str "dry run :: SELECT \"mtmdk2\".* FROM \"mtmdk2\" :: []\n"
              "dry run :: SELECT \"mtmdk1\".* FROM \"mtmdk1\" "
              "INNER JOIN \"mtmdk1_mtmdk2\" "
              "ON (\"mtmdk1_mtmdk2\".\"mtmdk1_id\" = \"mtmdk1\".\"id\") "
              "WHERE (\"mtmdk1_mtmdk2\".\"mtmdk2_id\" = ?) :: [1]\n")
         (with-out-str (dry-run (select mtmdk2 (with mtmdk1)))))))

(deftest many-to-many-default-keys-reverse
  (is (= (str "dry run :: SELECT \"mtmdk1\".* FROM \"mtmdk1\" :: []\n"
              "dry run :: SELECT \"mtmdk2\".* FROM \"mtmdk2\" "
              "INNER JOIN \"mtmdk1_mtmdk2\" "
              "ON (\"mtmdk1_mtmdk2\".\"mtmdk2_id\" = \"mtmdk2\".\"id\") "
              "WHERE (\"mtmdk1_mtmdk2\".\"mtmdk1_id\" = ?) :: [1]\n")
         (with-out-str (dry-run (select mtmdk1 (with mtmdk2)))))))

(deftest test-many-to-many-default-keys-join
  (is (= (str "dry run :: SELECT \"mtm2\".* FROM (\"mtm2\" "
              "LEFT JOIN \"mtm1_mtm2\" "
              "ON (\"mtm2\".\"id\" = \"mtm1_mtm2\".\"mtm2_id\")) "
              "LEFT JOIN \"mtm1\" "
              "ON (\"mtm1_mtm2\".\"mtm1_id\" = \"mtm1\".\"id\") :: []\n")
         (with-out-str (dry-run (select mtm2 (join mtm1)))))))

(deftest test-many-to-many-default-keys-join-reverse
  (is (= (str "dry run :: SELECT \"mtm1\".* FROM (\"mtm1\" "
              "LEFT JOIN \"mtm1_mtm2\" "
              "ON (\"mtm1\".\"id\" = \"mtm1_mtm2\".\"mtm1_id\")) "
              "LEFT JOIN \"mtm2\" "
              "ON (\"mtm1_mtm2\".\"mtm2_id\" = \"mtm2\".\"id\") :: []\n")
         (with-out-str (dry-run (select mtm1 (join mtm2)))))))


;;*****************************************************
;; Union, Union All, Intersect & Except support
;;*****************************************************

(deftest test-union
  (is (= "dry run :: (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"a\" = ?)) UNION (SELECT \"state\".* FROM \"state\" WHERE (\"state\".\"b\" = ? AND \"state\".\"c\" = ?)) :: [1 2 3]\n"
         (with-out-str (dry-run (union (queries (subselect users
                                                           (where {:a 1}))
                                                (subselect state
                                                           (where {:b 2
                                                                   :c 3})))))))))

(deftest test-union-all
  (is (= "dry run :: (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"a\" = ?)) UNION ALL (SELECT \"state\".* FROM \"state\" WHERE (\"state\".\"b\" = ? AND \"state\".\"c\" = ?)) :: [1 2 3]\n"
         (with-out-str (dry-run (union-all (queries (subselect users
                                                               (where {:a 1}))
                                                    (subselect state
                                                               (where {:b 2
                                                                       :c 3})))))))))

(deftest test-intersect
  (is (= "dry run :: (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"a\" = ?)) INTERSECT (SELECT \"state\".* FROM \"state\" WHERE (\"state\".\"b\" = ? AND \"state\".\"c\" = ?)) :: [1 2 3]\n"
         (with-out-str (dry-run (intersect (queries (subselect users
                                                               (where {:a 1}))
                                                    (subselect state
                                                               (where {:b 2
                                                                       :c 3})))))))))

(deftest test-order-by-in-union
  (is (= "dry run :: (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"a\" = ?)) UNION (SELECT \"state\".* FROM \"state\" WHERE (\"state\".\"b\" = ? AND \"state\".\"c\" = ?)) ORDER BY \"a\" ASC :: [1 2 3]\n"
         (with-out-str (dry-run (union (queries (subselect users
                                                           (where {:a 1}))
                                                (subselect state
                                                           (where {:b 2
                                                                   :c 3})))
                                       (order :a)))))))

(deftest test-order-by-in-union-all
  (is (= "dry run :: (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"a\" = ?)) UNION ALL (SELECT \"state\".* FROM \"state\" WHERE (\"state\".\"b\" = ? AND \"state\".\"c\" = ?)) ORDER BY \"a\" ASC :: [1 2 3]\n"
         (with-out-str (dry-run (union-all (queries (subselect users
                                                               (where {:a 1}))
                                                    (subselect state
                                                               (where {:b 2
                                                                       :c 3})))
                                           (order :a)))))))

(deftest test-order-by-in-intersect
  (is (= "dry run :: (SELECT \"users\".* FROM \"users\" WHERE (\"users\".\"a\" = ?)) INTERSECT (SELECT \"state\".* FROM \"state\" WHERE (\"state\".\"b\" = ? AND \"state\".\"c\" = ?)) ORDER BY \"a\" ASC :: [1 2 3]\n"
         (with-out-str (dry-run (intersect (queries (subselect users
                                                               (where {:a 1}))
                                                    (subselect state
                                                               (where {:b 2
                                                                       :c 3})))
                                           (order :a)))))))

;;*****************************************************
;; Transformers for one-to-one relations
;;****************************************************

(defentity state2 (transform identity))
(defentity address2 (belongs-to state2))

(deftest test-belongs-to-with-transform-fn-for-subentity
  (is (= (str "dry run :: SELECT \"address2\".* FROM \"address2\" :: []\n"
              "dry run :: SELECT \"state2\".* FROM \"state2\" WHERE (\"state2\".\"id\" = ?) :: [1]\n")
         (with-out-str (dry-run (select address2 (with state2)))))))

(defentity state2-ck (transform identity) (pk :id :id2))
(defentity address2-ck (belongs-to state2-ck (fk :sid :sid2)))

(deftest test-belongs-to-with-transform-fn-for-subentity-composite-key
  (is (= (str "dry run :: SELECT \"address2-ck\".* FROM \"address2-ck\" :: []\n"
              "dry run :: SELECT \"state2-ck\".* FROM \"state2-ck\" WHERE (\"state2-ck\".\"id\" = ? AND \"state2-ck\".\"id2\" = ?) :: [1 1]\n")
         (with-out-str (dry-run (select address2-ck (with state2-ck)))))))

(defentity address3 (transform identity))
(defentity user3 (has-one address3))

(deftest test-has-one-with-transform-fn-for-subentity
  (is (= (str "dry run :: SELECT \"user3\".* FROM \"user3\" :: []\n"
              "dry run :: SELECT \"address3\".* FROM \"address3\" WHERE (\"address3\".\"user3_id\" = ?) :: [1]\n")
         (with-out-str (dry-run (select user3 (with address3)))))))

(defentity address3-ck (transform identity))
(defentity user3-ck (pk :id :id2) (has-one address3-ck (fk :uid :uid2)))

(deftest test-has-one-with-transform-fn-for-subentity-composite-key
  (is (= (str "dry run :: SELECT \"user3-ck\".* FROM \"user3-ck\" :: []\n"
              "dry run :: SELECT \"address3-ck\".* FROM \"address3-ck\" WHERE (\"address3-ck\".\"uid\" = ? AND \"address3-ck\".\"uid2\" = ?) :: [1 1]\n")
         (with-out-str (dry-run (select user3-ck (with address3-ck)))))))

;;*****************************************************
;; Delimiters for one-to-one joins
;;****************************************************

(defdb mysql-db (mysql {}))

(defentity state-with-db
  (database mysql-db)
  (table :state))

(defentity address-with-db
  (database mysql-db)
  (table :address)
  (belongs-to state-with-db))

(defentity user-with-db
  (database mysql-db)
  (table :user)
  (has-one address-with-db))

(deftest test-correct-delimiters-for-one-to-one-joins
  (testing "correct delimiters are used in belongs-to joins"
    (is (= "SELECT `address`.*, `state`.* FROM `address` LEFT JOIN `state` ON (`state`.`id` = `address`.`state_id`) WHERE (`state`.`status` = ?) ORDER BY `address`.`id` ASC"
           (sql-only (select address-with-db
                             (with state-with-db
                                   (where {:status 1}))
                             (order :id))))))
  (testing "correct delimiters are used in has-one joins"
    (is (= "SELECT `user`.*, `address`.* FROM `user` LEFT JOIN `address` ON (`address`.`user_id` = `user`.`id`) WHERE (`address`.`status` = ?) ORDER BY `user`.`id` ASC"
           (sql-only (select user-with-db
                             (with address-with-db
                                   (where {:status 1}))
                             (order :id)))))))
