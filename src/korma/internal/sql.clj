(ns korma.internal.sql
  (:require [clojure.string :as string]
            [clojure.walk :as walk]))

;;*****************************************************
;; dynamic vars
;;*****************************************************

(def ^{:dynamic true} *bound-table* nil)

(defmacro bind-query [query & body]
  `(binding [*bound-table* (:table ~query)]
     ~@body))


;;*****************************************************
;; Str utils
;;*****************************************************

(defn prefix [ent field]
  (let [field-name (name field)]
    ;;check if it's already prefixed
    (if (= -1 (.indexOf field-name "."))
      (let [table (if (string? ent)
                    ent
                    (:table ent))]
        (str table "." field-name))
      field-name)))

(defn comma [vs]
  (string/join ", " vs))

(declare kv-clause)

(defn map->where [m]
  (str "(" (string/join " AND " (map (comp :generated kv-clause) m)) ")"))

(defn str-value [v]
  (cond
    (map? v) (or (:generated v) (map->where v))
    (keyword? v) (if *bound-table*
                   (prefix *bound-table* v)
                   (name v))
    (string? v) (str "'" v "'")
    (number? v) v
    (nil? v) "NULL"
    (true? v) "TRUE"
    (false? v) "FALSE"
    (coll? v) (str "(" (comma (map str-value v)) ")")
    :else v))

;;*****************************************************
;; Predicates
;;*****************************************************

(def predicates {'like 'korma.internal.sql/pred-like
                 'and 'korma.internal.sql/pred-and
                 'or 'korma.internal.sql/pred-or
                 'not 'korma.internal.sql/pred-not
                 'in 'korma.internal.sql/pred-in
                 '> 'korma.internal.sql/pred->
                 '< 'korma.internal.sql/pred-<
                 '>= 'korma.internal.sql/pred->=
                 '<= 'korma.internal.sql/pred-<=
                 'not= 'korma.internal.sql/pred-not=
                 '= 'korma.internal.sql/pred-=})

(defn infix [k op v]
  {:generated (str (str-value k) " " op " " (str-value v))})

(defn group-with [op vs]
  {:generated (str "(" (string/join op (map str-value vs)) ")")})

(defn pred-and [& args] (group-with " AND " args))
(defn pred-or [& args] (group-with " OR " args))
(defn pred-not [v] (str "NOT(" (str-value v) ")"))

(defn pred-in [k v] (infix k "IN" v))
(defn pred-> [k v] (infix k ">" v))
(defn pred-< [k v] (infix k "<" v))
(defn pred->= [k v] (infix k ">=" v))
(defn pref-<= [k v] (infix k "<=" v))
(defn pred-like [k v] (infix k "LIKE" v))

(defn pred-= [k v] (cond 
                     (and k v) (infix k "=" v)
                     k (infix k "IS" v)
                     v (infix v "IS" k)))
(defn pred-not= [k v] (cond
                        (and k v) (infix k "!=" v)
                        k (infix k "IS NOT" v)
                        v (infix v "IS NOT" k)))

;;*****************************************************
;; Clauses
;;*****************************************************

(defn kv-clause [[k v]]
  (if-not (vector? v)
    (pred-= k v)
    ((first v) k (second v))))

(defn join-clause [join-type table pk fk]
  (let [join-type (string/upper-case (name join-type))
        join (str " " join-type " JOIN " table " ON ")
        on-clause (str pk " = " fk)]
    (str join on-clause)))

(defn insert-values-clause [ks vs]
  (for [v vs]
    (str "(" (comma (map str-value (map #(get v %) ks))) ")")))

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
  (let [ins-keys (keys (first (:values query)))
        keys-clause (comma (map name ins-keys))
        ins-values (insert-values-clause ins-keys (:values query))
        values-clause (comma ins-values)
        neue-sql (str "INSERT INTO " (:table query) " (" keys-clause ") VALUES " values-clause)]
        [neue-sql query]))


;;*****************************************************
;; Sql parts
;;*****************************************************

(defn sql-set [[sql query]]
  (let [clauses (map (comp :generated kv-clause) (:fields query))
        clauses-str (string/join ", " clauses)
        neue-sql (str " SET " clauses-str)]
        [(str sql neue-sql) query]))

(defn sql-joins [[sql query]]
  (let [clauses (for [[type table pk fk] (:joins query)]
                  (join-clause :left table pk fk))
        clauses-str (string/join " " clauses)]
  [(str sql clauses-str) query]))

(defn sql-where [[sql query]]
  (if (seq (:where query))
    (let [clauses (map #(if (map? %) (str-value %) %) (:where query))
          clauses-str (string/join " AND " clauses)
          neue-sql (str " WHERE " clauses-str)]
      [(str sql neue-sql) query])
    [sql query]))

(defn sql-order [[sql query]]
  (if (seq (:order query))
    (let [clauses (for [[k dir] (:order query)]
                    (str (str-value k) " " (string/upper-case (name dir))))
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
;; Where
;;*****************************************************

(defn parse-where [form]
  (if (string? form)
    form
    (walk/postwalk-replace predicates form)))


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
