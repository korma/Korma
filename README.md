# Korma

Delicious SQL for Clojure.

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
;; executes: SELECT username, id FROM user

(select user
  (where {:username "chris"}))
;; executes: SELECT * FROM user WHERE username = 'chris'

```

## License

Copyright (C) 2011 Chris Granger

Distributed under the Eclipse Public License, the same as Clojure.
