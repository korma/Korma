(ns korma.core
  (:require [clojure.string :as string]))

(def ^{:dynamic true} *exec-mode* false)

(defn empty-query [ent]
  (let [[ent table] (if (string? ent)
                      [{} ent]
                      [ent (:table ent)])]
    {:ent ent
     :table table}))

(defn select-query [ent]
  (let [q (empty-query ent)]
    (merge q {:type :select
              :fields []
              :where {}
              :order []
              :group []})))

(defn update-query [ent]
  (let [q (empty-query ent)]
    (merge q {:type :update
              :fields {}
              :where {}} 
    )))

(defn delete-query [ent]
  (let [q (empty-query ent)]
    (merge q {:type :delete
              :where []})))

(defn insert-query [ent]
  (let [q (empty-query ent)]
    (merge q {:type :insert
              :set []})))

(defn sql-select [query]
  (let [clauses (if-not (seq (:fields query))
                  ["*"]
                  (map name (:fields query)))
        clauses-str (string/join ", " clauses)
        neue-sql (str "SELECT " clauses-str " FROM " (:table query))]
        [neue-sql query]))

(defn sql-update [query]
  (let [neue-sql (str "UPDATE " (:table query))]
        [neue-sql query]))

(defn kv-clause [[k v]]
  (str (name k) " = '" v "'"))

(defn sql-set [[sql query]]
  (let [clauses (map kv-clause (:fields query))
        clauses-str (string/join ", " clauses)
        neue-sql (str " SET " clauses-str)]
        [(str sql neue-sql) query]))

(defn sql-insert [query]
  (let [clauses (if-not (seq (:fields query))
                  ["*"]
                  (map name (:fields query)))
        clauses-str (string/join ", " clauses)
        neue-sql (str "SELECT " clauses-str " FROM " (:table query))]
        [neue-sql query]))

(defn sql-delete [query]
  (let [clauses (if-not (seq (:fields query))
                  ["*"]
                  (map name (:fields query)))
        clauses-str (string/join ", " clauses)
        neue-sql (str "SELECT " clauses-str " FROM " (:table query))]
        [neue-sql query]))

(defn join-clause [join-type table sub-table]
  (let [join-type (string/upper-case (name join-type))
        join (str " " join-type " JOIN " sub-table " ON ")
        on-clause (str table "." sub-table "_id = " sub-table ".id")]
    (str join on-clause)))

(defn sql-joins [[sql query]]
  (let [sub-ents (-> query :ent :has-one)
        clauses (for [{:keys [table]} sub-ents]
                  (join-clause :left (:table query) table))
        clauses-str (string/join " " clauses)]
  [(str sql clauses-str) query]))

(defn sql-where [[sql query]]
  (if (seq (:where query))
    (let [clauses (map kv-clause (:where query))
          clauses-str (string/join " AND " clauses)
          neue-sql (str " WHERE " clauses-str)]
      [(str sql neue-sql) query])
    [sql query]))

(defn sql-order [[sql query]]
  (if (seq (:order query))
    (let [clauses (for [[k dir] (:order query)]
                    (str (name k) " " (string/upper-case (name dir))))
          clauses-str (string/join ", " clauses)
          neue-sql (str " ORDER BY " clauses-str)]
      [(str sql neue-sql) query])
    [sql query]))

(defn sql-limit-offset [[sql {:keys [limit offset] :as query}]]
  (let [limit-sql (when limit
                    (str " LIMIT " limit))
        offset-sql (when offset
                     (str " OFFSET " offset))]
    [(str sql limit-sql offset-sql) query]))

(defmulti ->sql :type)
(defmethod ->sql :select [query]
  (-> query 
    (sql-select)
    (sql-joins)
    (sql-where)
    (sql-order)
    (sql-limit-offset)))

(defmethod ->sql :update [query]
  (-> query 
    (sql-update)
    (sql-set)
    (sql-where)))

(defmethod ->sql :delete [query]
  (-> query 
    (sql-delete)
    (sql-where)))

(defmethod ->sql :insert [query]
  (-> query 
    (sql-insert)))



(defn exec [[sql query]]
  (cond
    (:sql query) sql
    (= *exec-mode* :sql) sql
    (= *exec-mode* :dry-run) (do
                               (println "dry run SQL ::" sql)
                               [])
    :else (do
            ;;exec query
            [])))

(defmacro defentity [ent & body]
  `(let [e# (-> {:table ~(name ent)}
              ~@body)]
     (def ~ent e#)))

(defmacro select [ent & body]
  `(let [query# (-> (select-query ~ent)
                 ~@body)]
     (exec (->sql query#))))

(defmacro update [ent & body]
  `(let [query# (-> (update-query ~ent)
                  ~@body)]
     (exec (->sql query#))))

(defn fields [query & vs]
  (if (= (:type query) :update)
    (update-in query [:fields] merge (first vs))
    (update-in query [:fields] concat vs)))

(defn where [query vs]
  (update-in query [:where] merge vs))

(defn order [query k & [dir]]
  (update-in query [:order] conj [k (or dir :desc)]))

(defn limit [query v]
  (assoc query :limit v))

(defn offset [query v]
  (assoc query :offset v))

(defn as-sql [query]
  (first (->sql query)))

(defn has-one [ent sub-ent]
  (update-in ent [:has-one] conj sub-ent))

(defmacro sql-only [& body]
  `(binding [*exec-mode* :sql]
     ~@body))

(defmacro dry-run [& body]
  `(binding [*exec-mode* :dry-run]
     ~@body))
