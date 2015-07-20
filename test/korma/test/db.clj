(ns korma.test.db
  (:use [clojure.test :only [deftest is testing use-fixtures]]
        [korma.core :only [exec-raw]]
        [korma.db :only [connection-pool defdb get-connection h2 extract-options create-db
                         msaccess mssql mysql odbc oracle postgres sqlite3 vertica
                         firebird default-connection transaction]]))

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
   :test-connection-query "SELECT 1"
   :useUnicode true
   :connectTimeout 1000})

(deftest can-extract-options-merging-delimiters
    (let [original (extract-options nil)]
          (is (= (assoc original :delimiters ["`" "`"]) 
                 (extract-options {:delimiters ["`" "`"]})))))

(deftest can-extract-options-merging-alias-delimiter
    (let [original (extract-options nil)]
          (is (= (assoc original :alias-delimiter " az ") 
                 (extract-options {:alias-delimiter " az "})))))

(deftest can-extract-options-merging-naming
    (let [custom-fn (fn [& args] args)
                  original (extract-options nil)]
          (is (= (assoc original :naming {:fields custom-fn
                                          :keys custom-fn}) 
                 (extract-options {:naming {:fields custom-fn
                                            :keys custom-fn}})))))

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
    (is (= true (.isTestConnectionOnCheckout datasource)))
    (is (= {"useUnicode" "true"
            "connectTimeout" "1000"}
           (.getProperties datasource)))))

(deftest spec-with-missing-keys-returns-itself
  (defdb valid {:datasource :from-app-server})
  (is (= {:datasource :from-app-server} (get-connection valid))))


;;; DB spec creation fns

(deftest test-firebird
  (testing "firebirdsql - driver class"
    (is (= "org.firebirdsql.jdbc.FBDriver"
           (-> (create-db (firebird {})) :pool deref :datasource .getDriverClass))))
  (testing "firebirdsql - defaults"
    (is (= {:subprotocol "firebirdsql"
            :subname "localhost/3050:"
            :encoding "UTF8"}
           (firebird {}))))
  (testing "firebirdsql - options selected"
    (is (= {:subprotocol "firebirdsql"
            :subname "host/port:db"
            :make-pool? false
            :encoding "NONE"}
           (firebird {:host "host"
                      :port "port"
                      :db "db"
                      :encoding "NONE"
                      :make-pool? false})))))

(deftest test-postgres
  (testing "postgres - driver class"
    (is (= "org.postgresql.Driver"
           (-> (create-db (postgres {})) :pool deref :datasource .getDriverClass))))
  (testing "postgres - defaults"
    (is (= {:subprotocol "postgresql"
            :subname "//localhost:5432/"}
           (postgres {}))))
  (testing "postgres - options selected"
    (is (= {:subprotocol "postgresql"
            :subname "//host:port/db"
            :make-pool? false}
           (postgres {:host "host"
                      :port "port"
                      :db "db"
                      :make-pool? false})))))

(deftest test-oracle
  (testing "oracle - driver class"
    (is (= "oracle.jdbc.driver.OracleDriver"
           (-> (create-db (oracle {})) :pool deref :datasource .getDriverClass))))
  (testing "oracle - defaults"
    (is (= {:subprotocol "oracle:thin"
            :subname "@localhost:1521"}
           (oracle {}))))
  (testing "oracle - options selected"
    (is (= {:subprotocol "oracle:thin"
            :subname "@host:port"
            :make-pool? false}
           (oracle {:host "host"
                    :port "port"
                    :make-pool? false})))))

(deftest test-mysql
  (testing "mysql - driver class"
    (is (= "com.mysql.jdbc.Driver"
           (-> (create-db (mysql {})) :pool deref :datasource .getDriverClass))))
  (testing "mysql - delimeters"
    (is (= [\` \`]
           (-> (create-db (mysql {})) :options :delimiters))))
  (testing "mysql - defaults"
    (is (= {:subprotocol "mysql"
            :subname "//localhost:3306/"}
           (mysql {}))))
  (testing "mysql - options selected"
    (is (= {:subprotocol "mysql"
            :subname "//host:port/db"
            :make-pool? false}
           (mysql {:host "host"
                   :port "port"
                   :db "db"
                   :make-pool? false})))))

(deftest test-vertica
  (testing "vertica - driver class"
    (is (= "com.vertica.jdbc.Driver"
           (-> (create-db (vertica {})) :pool deref :datasource .getDriverClass))))
  (testing "vertica - defaults"
    (is (= {:subprotocol "vertica"
            :subname "//localhost:5433/"}
           (vertica {}))))
  (testing "vertica - options selected"
    (is (= {:subprotocol "vertica"
            :subname "//host:port/db"
            :make-pool? false}
           (vertica {:host "host"
                     :port "port"
                     :db "db"
                     :make-pool? false})))))

(deftest test-mssql
  (testing "mssql - driver class"
    (is (= "com.microsoft.sqlserver.jdbc.SQLServerDriver"
           (-> (create-db (mssql {})) :pool deref :datasource .getDriverClass))))
  (testing "mssql - defaults"
    (is (= {:subprotocol "sqlserver"
            :subname "//localhost:1433;database=;user=dbuser;password=dbpassword"}
           (mssql {}))))
  (testing "mssql - options selected"
    (is (= {:password "password"
            :user "user"
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
  (testing "msaccess - driver class"
    (is (= "sun.jdbc.odbc.JdbcOdbcDriver"
           (-> (create-db (msaccess {:make-pool? true})) :pool deref :datasource .getDriverClass))))
  (testing "msaccess - defaults"
    (is (= {:subprotocol "odbc"
            :subname "Driver={Microsoft Access Driver (*.mdb)};Dbq="
            :make-pool? false}
           (msaccess {}))))
  (testing "msaccess - .mdb selected"
    (is (= {:subprotocol "odbc"
            :subname "Driver={Microsoft Access Driver (*.mdb)};Dbq=db.mdb"
            :make-pool? true}
           (msaccess {:db "db.mdb" :make-pool? true}))))
  (testing "msaccess - .accdb selected"
    (is (= {:subprotocol "odbc"
            :subname (str "Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
                          "Dbq=db.accdb")
            :make-pool? true}
           (msaccess {:db "db.accdb" :make-pool? true})))))

(deftest test-odbc
  (testing "odbc - driver class"
    (is (= "sun.jdbc.odbc.JdbcOdbcDriver"
           (-> (create-db (odbc {})) :pool deref :datasource .getDriverClass))))
  (testing "odbc - defaults"
    (is (= {:subprotocol "odbc"
            :subname ""}
           (odbc {}))))
  (testing "odbc - options selected"
    (is (= {:subprotocol "odbc"
            :subname "MyDsn"
            :make-pool? false}
           (odbc {:dsn "MyDsn" :make-pool? false})))))

(deftest test-sqlite3
  (testing "sqlite3 - driver class"
    (is (= "org.sqlite.JDBC"
           (-> (create-db (sqlite3 {})) :pool deref :datasource .getDriverClass))))
  (testing "sqlite3 - defaults"
    (is (= {:subprotocol "sqlite"
            :subname "sqlite.db"}
           (sqlite3 {}))))
  (testing "sqlite3 - options selected"
    (is (= {:subprotocol "sqlite"
            :subname "db"
            :make-pool? false}
           (sqlite3 {:db "db" :make-pool? false})))))

(deftest test-h2
  (testing "h2 - driver class"
    (is (= "org.h2.Driver"
           (-> (create-db (h2 {})) :pool deref :datasource .getDriverClass))))
  (testing "h2 - defaults"
    (is (= {:subprotocol "h2"
            :subname "h2.db"}
           (h2 {}))))
  (testing "h2 - options selected"
    (is (= {:subprotocol "h2"
            :subname "db"
            :make-pool? false}
           (h2 {:db "db" :make-pool? false})))))

(deftest transaction-options
  (testing "if transaction macro respects isolation levels"
    (is (not= (transaction {:isolation :repeatable-read} (exec-raw "CALL LOCK_MODE()" :results))
              (transaction {:isolation :read-committed} (exec-raw "CALL LOCK_MODE()" :results))))
    (is (thrown? Exception (transaction {:isolation :no-such-isolation} (exec-raw "CALL LOCK_MODE()" :results))))))
