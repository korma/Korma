(ns korma.test.core
  (:use [korma.core])
  (:use [clojure.test]))

(defentity user)

(deftest select-function
         (is (= (-> (select-query "user")
                  (fields :id :username)
                  (where {:username "chris"})
                  (order :created)
                  (limit 5)
                  (offset 3)
                  (as-sql))
                "SELECT id, username FROM user WHERE username = 'chris' ORDER BY created DESC LIMIT 5 OFFSET 3")))

(deftest simple-selects
         (sql-only
           (let [results ["SELECT * FROM user"
                          "SELECT id, username FROM user"
                          "SELECT * FROM user WHERE email = 'hey@hey.com' AND username = 'chris'"
                          "SELECT * FROM user WHERE username = 'chris' ORDER BY created DESC"
                          "SELECT * FROM user WHERE active = true ORDER BY created DESC LIMIT 5 OFFSET 3"]
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
         (is (= (-> (update-query "user")
                  (fields {:first "chris"
                           :last "granger"})
                  (where {:id 3})
                  (as-sql))
                "UPDATE user SET first = 'chris', last = 'granger' WHERE id = 3")))

(deftest update-queries
         (sql-only
           (let [results ["UPDATE user SET first = 'chris'"
                          "UPDATE user SET first = 'chris' WHERE id = 3"
                          "UPDATE user SET first = 'chris', last = 'granger' WHERE id = 3"]
                 queries [(update user
                                  (fields {:first "chris"}))
                          (update user
                                  (fields {:first "chris"})
                                  (where {:id 3}))
                          (update user
                                  (fields {:first "chris"
                                           :last "granger"})
                                  (where {:id 3}))]]
             (doseq [[r q] (map vector results queries)]
               (is (= q r))))))

(deftest delete-function
         (is (= (-> (delete-query "user")
                  (where {:id 3})
                  (as-sql))
                "DELETE FROM user WHERE id = 3")))

(deftest delete-queries
         (sql-only
           (let [results ["DELETE FROM user"
                          "DELETE FROM user WHERE id = 3"]
                 queries [(delete user)
                          (delete user
                            (where {:id 3}))]]
             (doseq [[r q] (map vector results queries)]
               (is (= q r))))))

(deftest insert-function
         (is (= (-> (insert-query "user")
                  (values {:first "chris" :last "granger"})
                  (as-sql))
                "INSERT INTO user (last, first) VALUES ('granger', 'chris')")))

(deftest insert-queries
         (sql-only
           (let [results ["INSERT INTO user (last, first) VALUES ('granger', 'chris')"
                          "INSERT INTO user (last, first) VALUES ('granger', 'chris'), ('jordan', 'michael')"]
                 queries [(insert user
                            (values {:first "chris" :last "granger"}))
                          (insert user
                            (values [{:first "chris" :last "granger"}
                                     {:first "michael" :last "jordan"}]))]]
             (doseq [[r q] (map vector results queries)]
               (is (= q r))))))

