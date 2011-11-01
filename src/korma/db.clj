(ns korma.db
  "Functions for creating and managing database specifications."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.java.jdbc.internal :as ijdbc])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(defonce _default (atom nil))

(defn default-spec 
  "Set the database spec that Korma should use by default when no alternative is specified."
  [spec]
  (reset! _default spec))

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
  (if-not db
    (throw (Exception. "No valid DB connection selected."))
    (if (delay? db)
      @db
      db)))

(defmacro defdb 
  "Define a database specification. The last evaluated defdb will be used by default
  for all queries where no database is specified by the entity."
  [db-name spec]
  `(do 
     (defonce ~db-name (delay-pool ~spec))
     (default-spec ~db-name)))

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
  for :db, :user, and :password. You can also optionally set host and port."
  [{:keys [host port db] :as opts}]
  (let [host (or (:host opts) "localhost")
        port (or (:port opts) 3306)
        db (or (:db opts) "")]
  (merge {:classname "com.mysql.jdbc.Driver" ; must be in classpath
          :subprotocol "mysql"
          :subname (str "//" host ":" port "/" db)} 
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
  (merge {:classname "com.microsoft.jdbc.sqlserver.SQLServerDriver" ; must be in classpath
          :subprotocol "sqlserver"
          :subname (str "//" host ":" port ";database=" db ";user=" user ";password=" password)} 
         opts)))

(defn do-query [query]
  (let [conn (when-let[db (:db query)]
               (get-connection db))
        cur (or conn (get-connection @_default))
        results? (:results query)
        sql (:sql-str query)
        params (:params query)]
    (jdbc/with-connection cur
                          (if results?
                            (jdbc/with-query-results rs (apply vector sql params)
                                                     (vec rs))
                            (ijdbc/do-prepared-return-keys* sql params)))))

(comment
(defdb db (postgres {:db "db"
                     :user "chris"
                     :password "dbpass"}))
  )

