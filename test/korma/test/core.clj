(ns korma.test.core
  (:use [korma.core])
  (:use [clojure.test]))

(defentity users)
(defentity address)
(defentity email)

(defentity user2
  (table :users)
  (has-one address)
  (has-many email))

(defentity users-alias
  (table :users :u))

(deftest select-function
         (is (= (-> (select* "users")
                  (fields :id :username)
                  (where {:username "chris"})
                  (order :created)
                  (limit 5)
                  (offset 3)
                  (as-sql))
                "SELECT users.id, users.username FROM users WHERE (users.username = ?) ORDER BY users.created DESC LIMIT 5 OFFSET 3")))

(deftest simple-selects
         (sql-only
           (let [results ["SELECT * FROM users"
                          "SELECT * FROM users AS u"
                          "SELECT users.id, users.username FROM users"
                          "SELECT * FROM users WHERE (users.username = ? AND users.email = ?)"
                          "SELECT * FROM users WHERE (users.username = ?) ORDER BY users.created DESC"
                          "SELECT * FROM users WHERE (users.active = TRUE) ORDER BY users.created DESC LIMIT 5 OFFSET 3"]
                 queries [(select users)
                          (select users-alias)
                          (select users
                                  (fields :id :username))
                          (select users
                                  (where {:username "chris"
                                          :email "hey@hey.com"}))
                          (select users
                                  (where {:username "chris"})
                                  (order :created))
                          (select users
                                  (where {:active true})
                                  (order :created)
                                  (limit 5)
                                  (offset 3))]]
             (doseq [[r q] (map vector results queries)]
               (is (= q r))))))

(deftest update-function
         (is (= (-> (update* "users")
                  (set-fields {:first "chris"
                           :last "granger"})
                  (where {:id 3})
                  (as-sql))
                "UPDATE users SET first = ?, last = ? WHERE (users.id = ?)")))

(deftest update-queries
         (sql-only
           (let [results ["UPDATE users SET first = ?"
                          "UPDATE users SET first = ? WHERE (users.id = ?)"
                          "UPDATE users SET first = ?, last = ? WHERE (users.id = ?)"]
                 queries [(update users
                                  (set-fields {:first "chris"}))
                          (update users
                                  (set-fields {:first "chris"})
                                  (where {:id 3}))
                          (update users
                                  (set-fields {:first "chris"
                                           :last "granger"})
                                  (where {:id 3}))]]
             (doseq [[r q] (map vector results queries)]
               (is (= q r))))))

(deftest delete-function
         (is (= (-> (delete* "users")
                  (where {:id 3})
                  (as-sql))
                "DELETE FROM users WHERE (users.id = ?)")))

(deftest delete-queries
         (sql-only
           (let [results ["DELETE FROM users"
                          "DELETE FROM users WHERE (users.id = ?)"]
                 queries [(delete users)
                          (delete users
                            (where {:id 3}))]]
             (doseq [[r q] (map vector results queries)]
               (is (= q r))))))

(deftest insert-function
         (is (= (-> (insert* "users")
                  (values {:first "chris" :last "granger"})
                  (as-sql))
                "INSERT INTO users (last, first) VALUES (?, ?)")))

(deftest insert-queries
         (sql-only
           (let [results ["INSERT INTO users (last, first) VALUES (?, ?)"
                          "INSERT INTO users (last, first) VALUES (?, ?), (?, ?)"]
                 queries [(insert users
                            (values {:first "chris" :last "granger"}))
                          (insert users
                            (values [{:first "chris" :last "granger"}
                                     {:last "jordan" :first "michael"}]))]]
             (doseq [[r q] (map vector results queries)]
               (is (= q r))))))

(deftest complex-where
         (sql-only
           (let [results ["SELECT * FROM users WHERE (users.name = ? OR users.name = ?)"
                          "SELECT * FROM users WHERE ((users.name = ?) OR (users.name = ?))"
                          "SELECT * FROM users WHERE ((users.last = ? AND users.name = ?) OR (users.email = ?) OR users.age > ?)"
                          "SELECT * FROM users WHERE (users.x < ? OR (users.y < ? OR users.z > ?))"
                          "SELECT * FROM users WHERE (users.name LIKE ?)"
                          "SELECT * FROM users WHERE ((users.name LIKE ?) OR users.name LIKE ?)"]
                 queries [(select users
                                  (where (or (= :name "chris")
                                             (= :name "john"))))
                          (select users
                                  (where (or {:name "chris"}
                                             {:name "john"})))
                          (select users
                                  (where (or {:name "drew"
                                              :last "dreward"}
                                             {:email "drew@drew.com"}
                                             (> :age 10))))
                          (select users
                                  (where (or (< :x 5)
                                             (or (< :y 3)
                                                 (> :z 4)))))
                          (select users
                                  (where {:name [like "chris"]}))
                          (select users
                                  (where (or {:name [like "chris"]}
                                             (like :name "john"))))]]
             (doseq [[r q] (map vector results queries)]
               (is (= q r))))))

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
                  "SELECT address.state, users.name FROM users LEFT JOIN address ON users.id = address.users_id"))))

(deftest join-order
         (sql-only
           (is (= (select users 
                    (join :user2 :users.id :user2.users_id)
                    (join :user3 :users.id :user3.users_id))
                  "SELECT * FROM users LEFT JOIN user2 ON users.id = user2.users_id LEFT JOIN user3 ON users.id = user3.users_id"))))

(deftest aggregate-group
         (sql-only
           (is (= (select users (group :id :name))
                  "SELECT * FROM users GROUP BY users.id, users.name"))
           (is (= (select users (aggregate (count :*) :cnt :id))
                  "SELECT COUNT(*) AS cnt FROM users GROUP BY users.id"))))

(deftest quoting
         (sql-only
           (is (= (select users (fields :testField :t!))
                  "SELECT users.\"testField\", users.\"t!\" FROM users"))))

(deftest sqlfns
         (sql-only
           (is (= (select users 
                    (fields [(sqlfn now) :now] (sqlfn max :blah) (sqlfn avg (sqlfn sum 3 4) (sqlfn sum 4 5)))
                    (where {:time [>= (sqlfn now)]}))
                  "SELECT NOW() AS now, MAX(users.blah), AVG(SUM(?, ?), SUM(?, ?)) FROM users WHERE (users.time >= NOW())"))))
