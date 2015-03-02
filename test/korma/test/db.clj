(ns korma.test.db
  (:use [clojure.string :only [upper-case lower-case]]
        [clojure.test :only [deftest is testing use-fixtures]]
        [korma.core :only [exec-raw]]
        [korma.db :only [connection-pool defdb get-connection h2 create-db with-db
                         msaccess mssql mysql odbc oracle postgres sqlite3 vertica firebird default-connection transaction]]))

(defdb mem-db (h2 {:db "mem:test"}))

(use-fixtures :once
  (fn [f]
    (default-connection mem-db)
    (f)))

(def db-config-with-defaults
  {:classname "org.h2.Driver"
   :subprotocol "h2"
   :subname "mem:db_connectivity_test_db"
   :user "bob"
   :password "password"})

(def db-config-with-options-set
  {:classname "org.h2.Driver"
   :subprotocol "h2"
   :subname "mem:db_connectivity_test_db"
   :excess-timeout 99
   :idle-timeout 88
   :initial-pool-size 10
   :minimum-pool-size 5
   :maximum-pool-size 20
   :test-connection-on-checkout true
   :test-connection-query "SELECT 1"})

(def lower-upper {:keys lower-case :fields upper-case})
(def upper-upper {:keys upper-case :fields upper-case})

(deftest connection-pooling-default-test
  (let [pool (connection-pool db-config-with-defaults)
        datasource (:datasource pool)]
    (is (= "org.h2.Driver" (.getDriverClass datasource)))
    (is (= "jdbc:h2:mem:db_connectivity_test_db" (.getJdbcUrl datasource)))
    (is (= "bob" (.getUser datasource)))
    (is (= "password" (.getPassword datasource)))

    (is (= 1800 (.getMaxIdleTimeExcessConnections datasource)))
    (is (= 10800 (.getMaxIdleTime datasource)))
    (is (= 3 (.getMinPoolSize datasource)))
    (is (= 15 (.getMaxPoolSize datasource)))
    (is (= false (.isTestConnectionOnCheckout datasource)))))

(deftest connection-pooling-test
  (let [pool (connection-pool db-config-with-options-set)
        datasource (:datasource pool)]
    (is (= 99 (.getMaxIdleTimeExcessConnections datasource)))
    (is (= 88 (.getMaxIdleTime datasource)))
    (is (= 10 (.getInitialPoolSize datasource)))
    (is (= 5 (.getMinPoolSize datasource)))
    (is (= 20 (.getMaxPoolSize datasource)))
    (is (= (:test-connection-query db-config-with-options-set) (.getPreferredTestQuery datasource)))
    (is (= true (.isTestConnectionOnCheckout datasource)))))

(deftest spec-with-missing-keys-returns-itself
  (defdb valid {:datasource :from-app-server})
  (is (= {:datasource :from-app-server} (get-connection valid))))


;;; DB spec creation fns

(deftest test-firebird
  (testing "firebirdsql - defaults"
    (is (= {:classname "org.firebirdsql.jdbc.FBDriver"
            :subprotocol "firebirdsql"
            :subname "localhost/3050:?encoding=UTF8"
            :make-pool? true}
           (firebird {}))))
  (testing "firebirdsql - options selected"
    (is (= {:db "db"
            :port "port"
            :host "host"
            :classname "org.firebirdsql.jdbc.FBDriver"
            :subprotocol "firebirdsql"
            :encoding "NONE"
            :subname "host/port:db?encoding=NONE"
            :make-pool? false}
           (firebird {:host "host"
                      :port "port"
                      :db "db"
                      :encoding "NONE"
                      :make-pool? false})))))

(deftest test-postgres
  (testing "postgres - defaults"
    (is (= {:classname "org.postgresql.Driver"
            :subprotocol "postgresql"
            :subname "//localhost:5432/"
            :make-pool? true}
           (postgres {}))))
  (testing "postgres - options selected"
    (is (= {:db "db"
            :port "port"
            :host "host"
            :classname "org.postgresql.Driver"
            :subprotocol "postgresql"
            :subname "//host:port/db"
            :make-pool? false}
           (postgres {:host "host"
                      :port "port"
                      :db "db"
                      :make-pool? false})))))

(deftest test-oracle
  (testing "oracle - defaults"
    (is (= {:classname "oracle.jdbc.driver.OracleDriver"
            :subprotocol "oracle:thin"
            :subname "@localhost:1521"
            :make-pool? true}
           (oracle {}))))
  (testing "oracle - options selected"
    (is (= {:port "port"
            :host "host"
            :classname "oracle.jdbc.driver.OracleDriver"
            :subprotocol "oracle:thin"
            :subname "@host:port"
            :make-pool? false}
           (oracle {:host "host"
                    :port "port"
                    :make-pool? false})))))

(deftest test-mysql
  (testing "mysql - defaults"
    (is (= {:classname "com.mysql.jdbc.Driver"
            :subprotocol "mysql"
            :subname "//localhost:3306/"
            :delimiters "`"
            :make-pool? true}
           (mysql {}))))
  (testing "mysql - options selected"
    (is (= {:db "db"
            :port "port"
            :host "host"
            :classname "com.mysql.jdbc.Driver"
            :subprotocol "mysql"
            :subname "//host:port/db"
            :delimiters "`"
            :make-pool? false}
           (mysql {:host "host"
                   :port "port"
                   :db "db"
                   :make-pool? false})))))

(deftest test-vertica
  (testing "vertica - defaults"
    (is (= {:classname "com.vertica.jdbc.Driver"
            :subprotocol "vertica"
            :subname "//localhost:5433/"
            :delimiters "\""
            :make-pool? true}
           (vertica {}))))
  (testing "vertica - options selected"
    (is (= {:db "db"
            :port "port"
            :host "host"
            :classname "com.vertica.jdbc.Driver"
            :subprotocol "vertica"
            :subname "//host:port/db"
            :delimiters "\""
            :make-pool? false}
           (vertica {:host "host"
                     :port "port"
                     :db "db"
                     :make-pool? false})))))

(deftest test-mssql
  (testing "mssql - defaults"
    (is (= {:classname "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            :subprotocol "sqlserver"
            :subname "//localhost:1433;database=;user=dbuser;password=dbpassword"
            :make-pool? true}
           (mssql {}))))
  (testing "mssql - options selected"
    (is (= {:db "db"
            :password "password"
            :user "user"
            :port "port"
            :host "host"
            :classname "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            :subprotocol "sqlserver"
            :subname "//host:port;database=db;user=user;password=password"
            :make-pool? false}
           (mssql {:host "host"
                   :port "port"
                   :db "db"
                   :user "user"
                   :password "password"
                   :make-pool? false})))))

(deftest test-msaccess
  (testing "msaccess - defaults"
    (is (= {:classname "sun.jdbc.odbc.JdbcOdbcDriver"
            :subprotocol "odbc"
            :subname "Driver={Microsoft Access Driver (*.mdb)};Dbq="
            :make-pool? false}
           (msaccess {}))))
  (testing "msaccess - .mdb selected"
    (is (= {:classname "sun.jdbc.odbc.JdbcOdbcDriver"
            :subprotocol "odbc"
            :subname "Driver={Microsoft Access Driver (*.mdb)};Dbq=db.mdb"
            :db "db.mdb"
            :make-pool? true}
           (msaccess {:db "db.mdb" :make-pool? true}))))
  (testing "msaccess - .accdb selected"
    (is (= {:classname "sun.jdbc.odbc.JdbcOdbcDriver"
            :subprotocol "odbc"
            :subname (str "Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
                          "Dbq=db.accdb")
            :db "db.accdb"
            :make-pool? true}
           (msaccess {:db "db.accdb" :make-pool? true})))))

(deftest test-odbc
  (testing "odbc - defaults"
    (is (= {:classname "sun.jdbc.odbc.JdbcOdbcDriver"
            :subprotocol "odbc"
            :subname ""
            :make-pool? true}
           (odbc {}))))
  (testing "odbc - options selected"
    (is (= {:classname "sun.jdbc.odbc.JdbcOdbcDriver"
            :subprotocol "odbc"
            :subname "MyDsn"
            :dsn "MyDsn"
            :make-pool? false}
           (odbc {:dsn "MyDsn" :make-pool? false})))))

(deftest test-sqlite3
  (testing "sqlite3 - defaults"
    (is (= {:classname "org.sqlite.JDBC"
            :subprotocol "sqlite"
            :subname "sqlite.db"
            :make-pool? true}
           (sqlite3 {}))))
  (testing "sqlite3 - options selected"
    (is (= {:db "db"
            :classname "org.sqlite.JDBC"
            :subprotocol "sqlite"
            :subname "db"
            :make-pool? false}
           (sqlite3 {:db "db" :make-pool? false})))))

(deftest test-h2
  (testing "h2 - defaults"
    (is (= {:classname "org.h2.Driver"
            :subprotocol "h2"
            :subname "h2.db"
            :make-pool? true}
           (h2 {}))))
  (testing "h2 - options selected"
    (is (= {:db "db"
            :classname "org.h2.Driver"
            :subprotocol "h2"
            :subname "db"
            :make-pool? false}
           (h2 {:db "db" :make-pool? false})))))

(deftest transaction-options
  (testing "if transaction macro respects isolation levels"
    (is (not= (transaction {:isolation :repeatable-read} (exec-raw "CALL LOCK_MODE()" :results))
              (transaction {:isolation :read-committed} (exec-raw "CALL LOCK_MODE()" :results))))
    (is (thrown? Exception (transaction {:isolation :no-such-isolation} (exec-raw "CALL LOCK_MODE()" :results))))))

(deftest db-specific-naming
  (testing "if default database naming is actually used"
    (is (not (nil? (do (-> {:db "mem:test-lower-upper-naming" :naming lower-upper} h2 create-db default-connection)
                       (-> (exec-raw "SELECT COUNT(*) AS COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dummy_table'" :results) first :count)))))
    (is (not (nil? (do (-> {:db "mem:test-upper-upper-naming" :naming upper-upper} h2 create-db default-connection)
                       (-> (exec-raw "SELECT COUNT(*) AS COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dummy_table'" :results) first :COUNT))))))
  (testing "if with-db uses the specific naming"
    (-> {:db "mem:test-lower-upper-naming" :naming lower-upper} h2 create-db default-connection)
    (is (-> (exec-raw "SELECT COUNT(*) AS COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dummy_table'" :results) first :count nil? not))
    (is (with-db (-> {:db "mem:test-db-specific-naming" :naming upper-upper} h2 create-db)
          (-> (exec-raw "SELECT COUNT(*) AS COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dummy_table'" :results) first :COUNT nil? not)))))
