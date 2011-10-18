(ns korma.test.core
  (:use [korma.core])
  (:use [clojure.test]))

(defentity user)
(defentity address)
(defentity email)

(defentity user2
  (table :user)
  (has-one address)
  (has-many email))

(deftest select-function
         (is (= (-> (select* "user")
                  (fields :id :username)
                  (where {:username "chris"})
                  (order :created)
                  (limit 5)
                  (offset 3)
                  (as-sql))
                "SELECT user.id, user.username FROM \"user\" WHERE (user.username = 'chris') ORDER BY user.created DESC LIMIT 5 OFFSET 3")))

(deftest simple-selects
         (sql-only
           (let [results ["SELECT * FROM \"user\""
                          "SELECT user.id, user.username FROM \"user\""
                          "SELECT * FROM \"user\" WHERE (user.username = 'chris' AND user.email = 'hey@hey.com')"
                          "SELECT * FROM \"user\" WHERE (user.username = 'chris') ORDER BY user.created DESC"
                          "SELECT * FROM \"user\" WHERE (user.active = TRUE) ORDER BY user.created DESC LIMIT 5 OFFSET 3"]
                 queries [(select user)
                          (select user
                                  (fields :id :username))
                          (select user
                                  (where {:username "chris"
                                          :email "hey@hey.com"}))
                          (select user 
                                  (where {:username "chris"})
                                  (order :created))
                          (select user 
                                  (where {:active true})
                                  (order :created)
                                  (limit 5)
                                  (offset 3))]]
             (doseq [[r q] (map vector results queries)]
               (is (= q r))))))

(deftest update-function
         (is (= (-> (update* "user")
                  (set-fields {:first "chris"
                           :last "granger"})
                  (where {:id 3})
                  (as-sql))
                "UPDATE \"user\" SET first = 'chris', last = 'granger' WHERE (user.id = 3)")))

(deftest update-queries
         (sql-only
           (let [results ["UPDATE \"user\" SET first = 'chris'"
                          "UPDATE \"user\" SET first = 'chris' WHERE (user.id = 3)"
                          "UPDATE \"user\" SET first = 'chris', last = 'granger' WHERE (user.id = 3)"]
                 queries [(update user
                                  (set-fields {:first "chris"}))
                          (update user
                                  (set-fields {:first "chris"})
                                  (where {:id 3}))
                          (update user
                                  (set-fields {:first "chris"
                                           :last "granger"})
                                  (where {:id 3}))]]
             (doseq [[r q] (map vector results queries)]
               (is (= q r))))))

(deftest delete-function
         (is (= (-> (delete* "user")
                  (where {:id 3})
                  (as-sql))
                "DELETE FROM \"user\" WHERE (user.id = 3)")))

(deftest delete-queries
         (sql-only
           (let [results ["DELETE FROM \"user\""
                          "DELETE FROM \"user\" WHERE (user.id = 3)"]
                 queries [(delete user)
                          (delete user
                            (where {:id 3}))]]
             (doseq [[r q] (map vector results queries)]
               (is (= q r))))))

(deftest insert-function
         (is (= (-> (insert* "user")
                  (values {:first "chris" :last "granger"})
                  (as-sql))
                "INSERT INTO \"user\" (last, first) VALUES ('granger', 'chris')")))

(deftest insert-queries
         (sql-only
           (let [results ["INSERT INTO \"user\" (last, first) VALUES ('granger', 'chris')"
                          "INSERT INTO \"user\" (last, first) VALUES ('granger', 'chris'), ('jordan', 'michael')"]
                 queries [(insert user
                            (values {:first "chris" :last "granger"}))
                          (insert user
                            (values [{:first "chris" :last "granger"}
                                     {:last "jordan" :first "michael"}]))]]
             (doseq [[r q] (map vector results queries)]
               (is (= q r))))))

(deftest complex-where
         (sql-only
           (let [results ["SELECT * FROM \"user\" WHERE (user.name = 'chris' OR user.name = 'john')"
                          "SELECT * FROM \"user\" WHERE ((user.name = 'chris') OR (user.name = 'john'))"
                          "SELECT * FROM \"user\" WHERE ((user.last = 'dreward' AND user.name = 'drew') OR (user.email = 'drew@drew.com') OR user.age > 10)"
                          "SELECT * FROM \"user\" WHERE (user.x < 5 OR (user.y < 3 OR user.z > 4))"
                          "SELECT * FROM \"user\" WHERE (user.name LIKE 'chris')"
                          "SELECT * FROM \"user\" WHERE ((user.name LIKE 'chris') OR user.name LIKE 'john')"]
                 queries [(select user
                                  (where (or (= :name "chris")
                                             (= :name "john"))))
                          (select user
                                  (where (or {:name "chris"}
                                             {:name "john"})))
                          (select user
                                  (where (or {:name "drew"
                                              :last "dreward"}
                                             {:email "drew@drew.com"}
                                             (> :age 10))))
                          (select user
                                  (where (or (< :x 5)
                                             (or (< :y 3)
                                                 (> :z 4)))))
                          (select user
                                  (where {:name [like "chris"]}))
                          (select user
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
                  "SELECT address.state, user.name FROM \"user\" LEFT JOIN address ON user.id = address.user_id"))))
