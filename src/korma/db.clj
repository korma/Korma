(ns korma.db
  "Functions for creating and managing database specifications."
  (:require [clojure.java.jdbc :as jdbc]))

(defonce _default (atom nil))

(def ^:dynamic *current-db* nil)
(def ^:dynamic *current-conn* nil)

(defn- ->delimiters [delimiters]
  (if delimiters
    (let [[begin end] delimiters
          end (or end begin)]
      [begin end])
    ["\"" "\""]))

(defn- ->naming [strategy]
  (merge {:fields identity
          :keys identity} strategy))

(defn- ->alias-delimiter [alias-delimiter]
  (or alias-delimiter " AS "))

(defn extract-options [{:keys [naming
                               delimiters 
                               alias-delimiter
                               subprotocol]}]
  {:naming (->naming naming)
   :delimiters (->delimiters delimiters)
   :alias-delimiter (->alias-delimiter alias-delimiter)
   :subprotocol subprotocol})

(defn default-connection
  "Set the database connection that Korma should use by default when no
  alternative is specified."
  [conn]
  (reset! _default conn))

(defn- as-properties [m]
  (let [p (java.util.Properties.)]
    (doseq [[k v] m]
      (.setProperty p (name k) (str v)))
    p))

(def c3p0-enabled?
  (try
    (import 'com.mchange.v2.c3p0.ComboPooledDataSource)
    true
    (catch Throwable _ false)))

(defmacro resolve-new [class]
  (when-let [resolved (resolve class)]
    `(new ~resolved)))

(defn connection-pool
  "Create a connection pool for the given database spec."
  [{:keys [subprotocol subname classname
           excess-timeout idle-timeout
           initial-pool-size minimum-pool-size maximum-pool-size
           test-connection-query
           idle-connection-test-period
           test-connection-on-checkin
           test-connection-on-checkout]
    :or {excess-timeout (* 30 60)
         idle-timeout (* 3 60 60)
         initial-pool-size 3
         minimum-pool-size 3
         maximum-pool-size 15
         test-connection-query nil
         idle-connection-test-period 0
         test-connection-on-checkin false
         test-connection-on-checkout false}
    :as spec}]
  (when-not c3p0-enabled?
    (throw (Exception. "com.mchange.v2.c3p0.ComboPooledDataSource not found in class path."))) 
  {:datasource (doto (resolve-new ComboPooledDataSource)
                 (.setDriverClass classname)
                 (.setJdbcUrl (str "jdbc:" subprotocol ":" subname))
                 (.setProperties (as-properties (dissoc spec
                                                        :make-pool? :classname :subprotocol :subname
                                                        :naming :delimiters :alias-delimiter
                                                        :excess-timeout :idle-timeout
                                                        :initial-pool-size :minimum-pool-size :maximum-pool-size
                                                        :test-connection-query
                                                        :idle-connection-test-period
                                                        :test-connection-on-checkin
                                                        :test-connection-on-checkout)))
                 (.setMaxIdleTimeExcessConnections excess-timeout)
                 (.setMaxIdleTime idle-timeout)
                 (.setInitialPoolSize initial-pool-size)
                 (.setMinPoolSize minimum-pool-size)
                 (.setMaxPoolSize maximum-pool-size)
                 (.setIdleConnectionTestPeriod idle-connection-test-period)
                 (.setTestConnectionOnCheckin test-connection-on-checkin)
                 (.setTestConnectionOnCheckout test-connection-on-checkout)
                 (.setPreferredTestQuery test-connection-query))})

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
  "Create a db connection object manually instead of using defdb. This is often
   useful for creating connections dynamically, and probably should be followed
   up with:

   (default-connection my-new-conn)

   If the spec includes `:make-pool? true` makes a connection pool from the spec."
  [spec]
  {:pool (if (:make-pool? spec)
           (delay-pool spec)
           spec)
   :options (extract-options spec)})

(defmacro defdb
  "Define a database specification. The last evaluated defdb will be used by
  default for all queries where no database is specified by the entity."
  [db-name spec]
  `(let [spec# ~spec]
     (defonce ~db-name (create-db spec#))
     (default-connection ~db-name)))

(defn firebird
  "Create a database specification for a FirebirdSQL database. Opts should include
  keys for :db, :user, :password. You can also optionally set host, port and make-pool?"
  [{:keys [host port db make-pool?]
    :or {host "localhost", port 3050, db "", make-pool? true}
    :as opts}]
  (merge {:classname "org.firebirdsql.jdbc.FBDriver" ; must be in classpath
          :subprotocol "firebirdsql"
          :subname (str host "/" port ":" db)
          :make-pool? make-pool?
          :encoding "UTF8"}
         (dissoc opts :host :port :db)))

(defn postgres
  "Create a database specification for a postgres database. Opts should include
  keys for :db, :user, and :password. You can also optionally set host and
  port."
  [{:keys [host port db make-pool?]
    :or {host "localhost", port 5432, db "", make-pool? true}
    :as opts}]
  (merge {:classname "org.postgresql.Driver" ; must be in classpath
          :subprotocol "postgresql"
          :subname (str "//" host ":" port "/" db)
          :make-pool? make-pool?}
         (dissoc opts :host :port :db)))

(defn oracle
  "Create a database specification for an Oracle database. Opts should include keys
  for :user and :password. You can also optionally set host and port."
  [{:keys [host port make-pool?]
    :or {host "localhost", port 1521, make-pool? true}
    :as opts}]
  (merge {:classname "oracle.jdbc.driver.OracleDriver" ; must be in classpath
          :subprotocol "oracle:thin"
          :subname (str "@" host ":" port)
          :make-pool? make-pool?}
         (dissoc opts :host :port)))

(defn mysql
  "Create a database specification for a mysql database. Opts should include keys
  for :db, :user, and :password. You can also optionally set host and port.
  Delimiters are automatically set to \"`\"."
  [{:keys [host port db make-pool?]
    :or {host "localhost", port 3306, db "", make-pool? true}
    :as opts}]
  (merge {:classname "com.mysql.jdbc.Driver" ; must be in classpath
          :subprotocol "mysql"
          :subname (str "//" host ":" port "/" db)
          :delimiters "`"
          :make-pool? make-pool?}
         (dissoc opts :host :port :db)))

(defn vertica
  "Create a database specification for a vertica database. Opts should include keys
  for :db, :user, and :password. You can also optionally set host and port.
  Delimiters are automatically set to \"`\"."
  [{:keys [host port db make-pool?]
    :or {host "localhost", port 5433, db "", make-pool? true}
    :as opts}]
  (merge {:classname "com.vertica.jdbc.Driver" ; must be in classpath
          :subprotocol "vertica"
          :subname (str "//" host ":" port "/" db)
          :delimiters "\""
          :make-pool? make-pool?}
         (dissoc opts :host :port :db)))


(defn mssql
  "Create a database specification for a mssql database. Opts should include keys
  for :db, :user, and :password. You can also optionally set host and port."
  [{:keys [user password db host port make-pool?]
    :or {user "dbuser", password "dbpassword", db "", host "localhost", port 1433, make-pool? true}
    :as opts}]
  (merge {:classname "com.microsoft.sqlserver.jdbc.SQLServerDriver" ; must be in classpath
          :subprotocol "sqlserver"
          :subname (str "//" host ":" port ";database=" db ";user=" user ";password=" password)
          :make-pool? make-pool?}
         (dissoc opts :host :port :db)))

(defn msaccess
  "Create a database specification for a Microsoft Access database. Opts
  should include keys for :db and optionally :make-pool?."
  [{:keys [db make-pool?]
    :or {db "", make-pool? false}
    :as opts}]
  (merge {:classname "sun.jdbc.odbc.JdbcOdbcDriver" ; must be in classpath
          :subprotocol "odbc"
          :subname (str "Driver={Microsoft Access Driver (*.mdb"
                        (when (.endsWith db ".accdb") ", *.accdb")
                        ")};Dbq=" db)
          :make-pool? make-pool?}
         (dissoc opts :db)))

(defn odbc
  "Create a database specification for an ODBC DSN. Opts
  should include keys for :dsn and optionally :make-pool?."
  [{:keys [dsn make-pool?]
    :or {dsn "", make-pool? true}
    :as opts}]
  (merge {:classname "sun.jdbc.odbc.JdbcOdbcDriver" ; must be in classpath
          :subprotocol "odbc"
          :subname dsn
          :make-pool? make-pool?}
         (dissoc opts :dsn)))

(defn sqlite3
  "Create a database specification for a SQLite3 database. Opts should include a
  key for :db which is the path to the database file."
  [{:keys [db make-pool?]
    :or {db "sqlite.db", make-pool? true}
    :as opts}]
  (merge {:classname "org.sqlite.JDBC" ; must be in classpath
          :subprotocol "sqlite"
          :subname db
          :make-pool? make-pool?}
         (dissoc opts :db)))

(defn h2
  "Create a database specification for a h2 database. Opts should include a key
  for :db which is the path to the database file."
  [{:keys [db make-pool?]
    :or {db "h2.db", make-pool? true}
    :as opts}]
  (merge {:classname "org.h2.Driver" ; must be in classpath
          :subprotocol "h2"
          :subname db
          :make-pool? make-pool?}
         (dissoc opts :db)))

(defmacro transaction
  "Execute all queries within the body in a single transaction.
  Optionally takes as a first argument a map to specify the :isolation and :read-only? properties of the transaction."
  {:arglists '([body] [options & body])}
  [& body]
  (let [options (first body)
        check-options (and (-> body rest seq)
                           (map? options))
        {:keys [isolation read-only?]} (when check-options options)
        body (if check-options (rest body) body)]
    `(binding [*current-db* (or *current-db* @_default)]
      (jdbc/with-db-transaction [conn# (or *current-conn* (get-connection *current-db*)) :isolation ~isolation :read-only? ~read-only?]
        (binding [*current-conn* conn#]
          ~@body)))))

(defn rollback
  "Tell this current transaction to rollback."
  []
  (jdbc/db-set-rollback-only! *current-conn*))

(defn is-rollback?
  "Returns true if the current transaction will be rolled back"
  []
  (jdbc/db-is-rollback-only *current-conn*))

(defn- exec-sql [{:keys [results sql-str params]}]
  (let [keys (get-in *current-db* [:options :naming :keys])]
    (case results
      :results (jdbc/query *current-conn*
                           (apply vector sql-str params)
                           :identifiers keys)
      :keys (jdbc/db-do-prepared-return-keys *current-conn* sql-str params)
      (jdbc/db-do-prepared *current-conn* sql-str params))))

(defmacro with-db
  "Execute all queries within the body using the given db spec"
  [db & body]
  `(jdbc/with-db-connection [conn# (korma.db/get-connection ~db)]
     (binding [*current-db* ~db
               *current-conn* conn#]
       ~@body)))

(defn do-query [{:keys [db] :as query}]
  (if *current-conn*
    (exec-sql query)
    (with-db (or db @_default)
      (exec-sql query))))
