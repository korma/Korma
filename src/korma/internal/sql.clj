(ns korma.internal.sql
  (:require [clojure.string :as string]))

;;*****************************************************
;; Str utils
;;*****************************************************

(defn str-value [v]
  (cond
    (string? v) (str "'" v "'")
    (number? v) v
    (true? v) "TRUE"
    (false? v) "FALSE"
    (coll? v) (pr-str (seq v))
    :else v))

(defn comma [vs]
  (string/join ", " vs))

;;*****************************************************
;; Clauses
;;*****************************************************

(defn kv-clause [[k v]]
  (str (name k) " = " (str-value v)))

(defn join-clause [join-type table sub-table]
  (let [join-type (string/upper-case (name join-type))
        join (str " " join-type " JOIN " sub-table " ON ")
        on-clause (str table "." sub-table "_id = " sub-table ".id")]
    (str join on-clause)))

(defn insert-values-clause [vs]
  (for [v vs]
    (str "(" (comma (map str-value (vals v))) ")")))

;;*****************************************************
;; Query types
;;*****************************************************

(defn sql-select [query]
  (let [clauses (if-not (seq (:fields query))
                  ["*"]
                  (map name (:fields query)))
        clauses-str (comma clauses)
        neue-sql (str "SELECT " clauses-str " FROM " (:table query))]
        [neue-sql query]))

(defn sql-update [query]
  (let [neue-sql (str "UPDATE " (:table query))]
        [neue-sql query]))

(defn sql-delete [query]
  (let [neue-sql (str "DELETE FROM " (:table query))]
        [neue-sql query]))

(defn sql-insert [query]
  (let [ins-keys (map name (keys (first (:values query))))
        keys-clause (comma ins-keys)
        ins-values (insert-values-clause (:values query))
        values-clause (comma ins-values)
        neue-sql (str "INSERT INTO " (:table query) " (" keys-clause ") VALUES " values-clause)]
        [neue-sql query]))


;;*****************************************************
;; Sql parts
;;*****************************************************

(defn sql-set [[sql query]]
  (let [clauses (map kv-clause (:fields query))
        clauses-str (string/join ", " clauses)
        neue-sql (str " SET " clauses-str)]
        [(str sql neue-sql) query]))

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


;;*****************************************************
;; To sql
;;*****************************************************

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
