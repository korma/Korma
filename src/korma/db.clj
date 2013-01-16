(ns korma.db
  "Functions for creating and managing database specifications."
  (:require [clojure.java.jdbc :as jdbc]
            [korma.config :as conf])
  (:import (com.mchange.v2.c3p0 ComboPooledDataSource)))

(defonce _default (atom nil))

(defn default-connection
  "Set the database connection that Korma should use by default when no 
  alternative is specified."
  [conn]
  (conf/merge-defaults (:options conn))
  (reset! _default conn))

(defn connection-pool
  "Create a connection pool for the given database spec only if it
  contains the keys :subprotocol, :subname, and :classname. Otherwise,
  spec is returned unaltered."
  [{:keys [subprotocol subname classname user password
           excess-timeout idle-timeout minimum-pool-size maximum-pool-size]
    :or {excess-timeout (* 30 60)
         idle-timeout (* 3 60 60)
         minimum-pool-size 3
         maximum-pool-size 15}
    :as spec}]
  (if (and subprotocol subname classname)
    {:datasource (doto (ComboPooledDataSource.)
                   (.setDriverClass classname)
                   (.setJdbcUrl (str "jdbc:" subprotocol ":" subname))
                   (.setUser user)
                   (.setPassword password)
                   (.setMaxIdleTimeExcessConnections excess-timeout)
                   (.setMaxIdleTime idle-timeout)
                   (.setMinPoolSize minimum-pool-size)
                   (.setMaxPoolSize maximum-pool-size))}
    spec))

(defn delay-pool
  "Return a delay for creating a connection pool for the given spec."
  [spec]
  (delay (connection-pool spec)))

(defn get-connection
  "Get a connection from the potentially delayed connection object."
  [db]
  (let [db (or (:pool db) db)]
    (if-not db
      (throw (Exception. "No valid DB connection selected."))
      (if (delay? db)
        @db
        db))))

(defn create-db
  "Create a db connection object manually instead of using defdb. This is often useful for
  creating connections dynamically, and probably should be followed up with:

  (default-connection my-new-conn)"
  [spec]
  {:pool (delay-pool spec)
   :options (conf/extract-options spec)})

(defmacro defdb
  "Define a database specification. The last evaluated defdb will be used by default
  for all queries where no database is specified by the entity."
  [db-name spec]
  `(let [spec# ~spec]
     (defonce ~db-name (create-db spec#))
     (default-connection ~db-name)))

(defn postgres
  "Create a database specification for a postgres database. Opts should include keys
  for :db, :user, and :password. You can also optionally set host and port."
  [{:keys [host port db]
    :or {host "localhost", port 5432, db ""}
    :as opts}]
  (merge {:classname "org.postgresql.Driver" ; must be in classpath
          :subprotocol "postgresql"
          :subname (str "//" host ":" port "/" db)}
         opts))

(defn oracle
  "Create a database specification for an Oracle database. Opts should include keys
  for :user and :password. You can also optionally set host and port."
  [{:keys [host port]
    :or {host "localhost", port 1521}
    :as opts}]
  (merge {:classname "oracle.jdbc.driver.OracleDriver" ; must be in classpath
          :subprotocol "oracle:thin"
          :subname (str "@" host ":" port)}
         opts))

(defn mysql
  "Create a database specification for a mysql database. Opts should include keys
  for :db, :user, and :password. You can also optionally set host and port.
  Delimiters are automatically set to \"`\"."
  [{:keys [host port db]
    :or {host "localhost", port 3306, db ""}
    :as opts}]
  (merge {:classname "com.mysql.jdbc.Driver" ; must be in classpath
          :subprotocol "mysql"
          :subname (str "//" host ":" port "/" db)
          :delimiters "`"}
         opts))

(defn mssql
  "Create a database specification for a mssql database. Opts should include keys
  for :db, :user, and :password. You can also optionally set host and port."
  [{:keys [user password db host port]
    :or {user "dbuser", password "dbpassword", db "", host "localhost", port 1433}
    :as opts}]
  (merge {:classname "com.microsoft.sqlserver.jdbc.SQLServerDriver" ; must be in classpath
          :subprotocol "sqlserver"
          :subname (str "//" host ":" port ";database=" db ";user=" user ";password=" password)} 
         opts))

(defn sqlite3
  "Create a database specification for a SQLite3 database. Opts should include a key
  for :db which is the path to the database file."
  [{:keys [db]
    :or {db "sqlite.db"}
    :as opts}]
  (merge {:classname "org.sqlite.JDBC" ; must be in classpath
          :subprotocol "sqlite"
          :subname db}
         opts))

(defn h2
  "Create a database specification for a h2 database. Opts should include a key
  for :db which is the path to the database file."
  [{:keys [db]
    :or {db "h2.db"}
    :as opts}]
  (merge {:classname "org.h2.Driver" ; must be in classpath
          :subprotocol "h2"
          :subname db}
         opts))

(defmacro transaction
  "Execute all queries within the body in a single transaction."
  [& body]
  `(jdbc/with-connection (get-connection @_default)
     (jdbc/transaction
       ~@body)))

(defn rollback
  "Tell this current transaction to rollback."
  []
  (jdbc/set-rollback-only))

(defn is-rollback?
  "Returns true if the current transaction will be rolled back"
  []
  (jdbc/is-rollback-only))

(defn- handle-exception [e sql params]
  (if-not (instance? java.sql.SQLException e)
    (.printStackTrace e)
    (do
      (when-let [ex (.getNextException e)]
        (handle-exception ex sql params))
      (println "Failure to execute query with SQL:")
      (println sql " :: " params)
      (jdbc/print-sql-exception e)))
  (throw e))

(defn- exec-sql [{:keys [results sql-str params]}]
  (try
    (case results
      :results (jdbc/with-query-results rs (apply vector sql-str params)
                 (vec rs))
      :keys (jdbc/do-prepared-return-keys sql-str params)
      (jdbc/do-prepared sql-str params))
    (catch Exception e
      (handle-exception e sql-str params))))

(defn- ->naming-strategy [{:keys [keys fields]}]
  {:keyword keys
   :identifier fields})

(defn do-query [{:keys [db options] :or {options @conf/options} :as query}]
  (jdbc/with-naming-strategy (->naming-strategy (:naming options))
    (if (jdbc/find-connection)
      (exec-sql query)
      (jdbc/with-connection (get-connection (or db @_default))
        (exec-sql query)))))
