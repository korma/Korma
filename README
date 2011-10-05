# Korma

Delicious SQL for the Clojure world.

## Usage

```clojure

(use 'korma.db)
(defdb db (postgres {:db "mydb"
                     :user "user"
                     :password "dbpass"}))

(use 'korma.core)
(defentity user)

(select user)

(select user
  (fields :username :id))

(select user
  (where {:username "chris"}))

```

## License

Copyright (C) 2011 Chris Granger

Distributed under the Eclipse Public License, the same as Clojure.
