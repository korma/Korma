# Korma

Tasty SQL for Clojure.

## Getting started

Simply add Korma as a dependency to your lein/cake project:

```clojure
[korma "0.3.0-beta6"]
```

For docs and real usage, check out http://sqlkorma.com

## Examples of generated queries:

```clojure

(use 'korma.db)
(defdb db (postgres {:db "mydb"
                     :user "user"
                     :password "dbpass"}))

(use 'korma.core)
(defentity users)

(select users)
;; executes: SELECT * FROM users

(select users
  (fields :usersname :id))
;; executes: SELECT users.usersname, users.id FROM users

(select users
  (where {:usersname "chris"}))
;; executes: SELECT * FROM users WHERE (users.usersname = 'chris)'

(select users 
  (where {:active true})
  (order :created)
  (limit 5)
  (offset 3))
;; executes: SELECT * FROM users WHERE (users.active = TRUE) ORDER BY users.created DESC LIMIT 5 OFFSET 3

(select users
  (where (or (= :usersname "chris")
             (= :email "chris@chris.com"))))
;; executes: SELECT * FROM users WHERE (users.usersname = 'chris' OR users.email = 'chris@chris.com')

(select users
  (where {:usersname [like "chris"]
          :status "active"
          :location [not= nil]))
;; executes SELECT * FROM users WHERE (users.usersname LIKE 'chris' AND users.status = 'active' AND users.location IS NOT NULL)

(select users
  (where (or {:usersname "chris"
              :first "chris"}
             {:email [like "%@chris.com"]})))
;; executes: SELECT * FROM users WHERE ((users.usersname = 'chris' AND users.first = 'chris') OR users.email LIKE '%@chris.com)'


(defentity address
 (table-fields :street :city :zip))

(defentity users
 (has-one address))

(select users
 (with address))
;; SELECT address.street, address.city, address.zip FROM users LEFT JOIN address ON users.id = address.users_id

```

## License

Copyright (C) 2011 Chris Granger

Distributed under the Eclipse Public License, the same as Clojure.
