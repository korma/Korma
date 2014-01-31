# Korma

Tasty SQL for Clojure.

## TravisCI Status

[![Build Status](https://travis-ci.org/korma/Korma.png)](https://travis-ci.org/korma/Korma)

## Getting started

Simply add Korma as a dependency to your lein project:

```clojure
[korma "0.3.0"]
```

Note: korma depends on version 0.2.3 of jdbc, so if you have another dependency that requires a different version (like the migrations library [lobos](https://github.com/budu/lobos)), be sure to specify:

```clojure
[org.clojure/java.jdbc "0.2.3"]
```

## Docs and Real Usage

*   [http://sqlkorma.com](http://sqlkorma.com)
*   [API Docs](http://korma.github.com/Korma/)

To get rid of the ridiculously verbose logging, add the following into src/log4j.xml:

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE log4j:configuration SYSTEM "log4j.dtd">
<log4j:configuration xmlns:log4j="http://jakarta.apache.org/log4j/">
  <logger name="com.mchange">
    <level value="WARN"/>
  </logger>
</log4j:configuration>
```

And include log4j in your project.clj:

```clojure
[log4j "1.2.15" :exclusions [javax.mail/mail
                            javax.jms/jms
                            com.sun.jdmk/jmxtools
                            com.sun.jmx/jmxri]]
```

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
;; executes: SELECT * FROM users WHERE (users.usersname = 'chris')

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
 (entity-fields :street :city :zip))

(defentity users
 (has-one address))

(select users
 (with address))
;; SELECT address.street, address.city, address.zip FROM users LEFT JOIN address ON users.id = address.users_id

```

## License

Copyright (C) 2011 Chris Granger

Distributed under the Eclipse Public License, the same as Clojure.
