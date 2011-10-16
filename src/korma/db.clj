(ns korma.db
  "Functions for creating and managing database specifications."
  (:require [clojure.java.jdbc :as jdbc]))

(def _default (atom nil))

(defn default-spec 
  "Set the database spec that Korma should use by default when no alternative is specified."
  [spec]
  (reset! _default spec))

(defmacro defdb 
  "Define a database specification. The last evaluated defdb will be used by default
  for all queries where no database is specified by the entity."
  [db-name spec]
  `(do 
     (def ~db-name ~spec)
     (default-spec ~spec)))

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

(defn do-query [conn q]
  (let [cur (or conn @_default)]
    (jdbc/with-connection cur
      (jdbc/with-query-results rs [q]
        (vec rs)))))

(comment
(defdb db (postgres {:db "db"
                     :user "chris"
                     :password "dbpass"}))
  )

