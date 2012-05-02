(ns korma.db
  "Functions for creating and managing database specifications."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.internal :as ijdbc]
            [korma.config :as conf])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(defonce _default (atom nil))

(defn ->strategy [{:keys [keys fields]}]
  {:keyword keys
   :identifier fields})

(defn default-connection
  "Set the database connection that Korma should use by default when no 
  alternative is specified."
  [conn]
  (conf/merge-defaults (:options conn))
  (reset! _default conn))

(defn connection-pool
  "Create a connection pool for the given database spec."
  [spec]
  (let [excess (or (:excess-timeout spec) (* 30 60))
        idle (or (:idle-timeout spec) (* 3 60 60))
        cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec))
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               (.setMaxIdleTimeExcessConnections excess)
               (.setMaxIdleTime idle))]
    {:datasource cpds}))

(defn delay-pool
  "Return a delay for creating a connection pool for the given spec."
  [spec]
  (delay (connection-pool spec)))

(defn get-connection
  "Get a connection from the potentially delayed connection object."
  [db]
  (let [db (if (map? db)
             (:pool db)
             db)]
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
  [{:keys [host port db] :as opts}]
  (let [host (or host "localhost")
        port (or port 5432)
        db (or db "")]
  (merge {:classname "org.postgresql.Driver" ; must be in classpath
          :subprotocol "postgresql"
          :subname (str "//" host ":" port "/" db)}
         opts)))

(defn oracle
  "Create a database specification for an Oracle database. Opts should include keys
  for :user and :password. You can also optionally set host and port."
  [{:keys [host port] :as opts}]
  (let [host (or (:host opts) "localhost")
        port (or (:port opts) 1521)]
    (merge {:classname "oracle.jdbc.driver.OracleDriver" ; must be in classpath
            :subprotocol "oracle:thin"
            :subname (str "@" host ":" port)}
           opts)))

(defn mysql
  "Create a database specification for a mysql database. Opts should include keys
  for :db, :user, and :password. You can also optionally set host and port.
  Delimiters are automatically set to \"`\"."
  [{:keys [host port db] :as opts}]
  (let [host (or (:host opts) "localhost")
        port (or (:port opts) 3306)
        db (or (:db opts) "")]
  (merge {:classname "com.mysql.jdbc.Driver" ; must be in classpath
          :subprotocol "mysql"
          :subname (str "//" host ":" port "/" db)
          :delimiters "`"}
         opts)))

(defn mssql
  "Create a database specification for a mssql database. Opts should include keys
  for :db, :user, and :password. You can also optionally set host and port."
  [{:keys [user password db host port] :as opts}]
  (let [host (or (:host opts) "localhost")
        port (or (:port opts) 5432)
        user (or user "dbuser")
        password (or password "dbpassword")
        db (or (:db opts) "")]
  (merge {:classname "com.microsoft.sqlserver.jdbc.SQLServerDriver" ; must be in classpath
          :subprotocol "sqlserver"
          :subname (str "//" host ":" port ";database=" db ";user=" user ";password=" password)} 
         opts)))

(defn sqlite3
  "Create a database specification for a SQLite3 database. Opts should include a key
  for :db which is the path to the database file."
  [{:keys [db] :as opts}]
  (let [db (or (:db opts) "sqlite.db")]
    (merge {:classname "org.sqlite.JDBC" ; must be in classpath
            :subprotocol "sqlite"
            :subname db}
           opts)))

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

(defn handle-exception [e sql params]
  (println "Failure to execute query with SQL:")
  (println sql " :: " params)
  (cond
    (instance? java.sql.SQLException e) (jdbc/print-sql-exception (.getNextException e))
    :else (.printStackTrace e))
  (throw (.getNextException e)))

(defn- exec-sql [query]
  (let [results? (:results query)
        sql (:sql-str query)
        params (:params query)]
    (try
      (condp = results?
        :results (jdbc/with-query-results rs (apply vector sql params)
                   (vec rs))
        :keys (ijdbc/do-prepared-return-keys* sql params)
        (jdbc/do-prepared sql params))
      (catch Exception e (handle-exception e sql params)))))

(defn do-query [query]
  (let [conn (when-let[db (:db query)]
               (get-connection db))
        cur (or conn (get-connection @_default))
        prev-conn (jdbc/find-connection)
        opts (or (:options query) @conf/options)]
    (jdbc/with-naming-strategy (->strategy (:naming opts))
      (if-not prev-conn
        (jdbc/with-connection cur
          (exec-sql query))
        (exec-sql query)))))
