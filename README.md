# Korma

Tasty SQL for Clojure.

## TravisCI Status

[![Build Status](https://travis-ci.org/korma/Korma.png)](https://travis-ci.org/korma/Korma)

## Getting started

Simply add Korma as a dependency to your lein project:

```clojure
[korma "0.4.3"]
```

## Stable and Edge releases

The most recent stable release is Korma 0.4.3, as noted above.

The current edge release is Korma 0.5.0-RC1 (released Feb 28, 2018). It is not guaranteed to be bug free, and we'd welcome help from the community in identifying any bugs or regressions. The plan is to either release a second release candidate in May or, if no bugs have surfaced, for Korma 0.5.0 to be released at that time.

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
  (fields :username :id))
;; executes: SELECT users.username, users.id FROM users

(select users
  (where {:username "chris"}))
;; executes: SELECT * FROM users WHERE (users.username = 'chris')

(select users 
  (where {:active true})
  (order :created)
  (limit 5)
  (offset 3))
;; executes: SELECT * FROM users WHERE (users.active = TRUE) ORDER BY users.created DESC LIMIT 5 OFFSET 3

(select users
  (where (or (= :username "chris")
             (= :email "chris@chris.com"))))
;; executes: SELECT * FROM users WHERE (users.username = 'chris' OR users.email = 'chris@chris.com')

(select users
  (where {:username [like "chris"]
          :status "active"
          :location [not= nil]}))
;; executes SELECT * FROM users WHERE (users.username LIKE 'chris' AND users.status = 'active' AND users.location IS NOT NULL)

(select users
  (where (or {:username "chris"
              :first "chris"}
             {:email [like "%@chris.com"]})))
;; executes: SELECT * FROM users WHERE ((users.username = 'chris' AND users.first = 'chris') OR users.email LIKE '%@chris.com)'


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
