(ns korma.db
  (:require [clojure.java.jdbc :as jdbc]))

(def _default (atom nil))

(defn default-spec [spec]
  (reset! _default spec))

(defmacro defdb [db-name spec]
  `(do 
     (def ~db-name ~spec)
     (default-spec ~spec)))

(defn postgres [opts]
  (let [host (or (:host opts) "localhost")
        port (or (:port opts) 5432)
        db (or (:db opts) "")]
  (merge {:classname "org.postgresql.Driver" ; must be in classpath
          :subprotocol "postgresql"
          :subname (str "//" host ":" port "/" db)} 
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

