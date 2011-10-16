# Korma

Delicious SQL for Clojure.

_Korma is just getting started, use at your own peril_

## Usage

```clojure

(use 'korma.db)
(defdb db (postgres {:db "mydb"
                     :user "user"
                     :password "dbpass"}))

(use 'korma.core)
(defentity user)

(select user)
;; executes: SELECT * FROM user

(select user
  (fields :username :id))
;; executes: SELECT user.username, user.id FROM user

(select user
  (where {:username "chris"}))
;; executes: SELECT * FROM user WHERE (user.username = 'chris)'

(select user 
  (where {:active true})
  (order :created)
  (limit 5)
  (offset 3))
;; executes: SELECT * FROM user WHERE (user.active = TRUE) ORDER BY user.created DESC LIMIT 5 OFFSET 3

(select user
  (where (or (= :username "chris")
             (= :email "chris@chris.com"))))
;; executes: SELECT * FROM user WHERE (user.username = 'chris' OR user.email = 'chris@chris.com')

(select user
  (where {:username [like "chris"]
          :status "active"
          :location [not= nil]))
;; executes SELECT * FROM user WHERE (user.username LIKE 'chris' AND user.status = 'active' AND user.location IS NOT NULL)

(select user
  (where (or {:username "chris"
              :first "chris"}
             {:email [like "*@chris.com"]})))
;; executes: SELECT * FROM user WHERE ((user.username = 'chris' AND user.first = 'chris') OR user.email = 'chris@chris.com)'


(defentity address
 (table-fields :street :city :zip))

(defentity user
 (has-one address))

(select user
 (with address))
;; SELECT address.street, address.city, address.zip FROM user LEFT JOIN address ON user.id = address.user_id

```

## License

Copyright (C) 2011 Chris Granger

Distributed under the Eclipse Public License, the same as Clojure.
