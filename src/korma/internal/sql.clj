(ns korma.internal.sql
  (:require [clojure.string :as string]
            [clojure.walk :as walk]))


;;*****************************************************
;; dynamic vars
;;*****************************************************

(def ^{:dynamic true} *bound-table* nil)
(def ^{:dynamic true} *bound-params* nil)

;;*****************************************************
;; Str utils
;;*****************************************************

(defn table-alias [{:keys [type table alias]}]
  (if (= :select type)
    (or alias table)
    table))

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

(declare kv-clause str-value)

(defn map->where [m]
  (str "(" (string/join " AND " (map (comp :generated kv-clause) m)) ")"))

(defn field-str [v]
  (let [fname (if *bound-table*
                (prefix *bound-table* v)
                (name v))]
    fname))

(defn coll-str [v]
  (str "(" (comma (map str-value v)) ")"))

(defn table-str [v]
  (str v))

(defn parameterize [v]
  (when *bound-params*
    (swap! *bound-params* conj v))
  "?")

(defn str-value [v]
  (cond
    (map? v) (or (:generated v) (map->where v))
    (keyword? v) (field-str v)
    (nil? v) "NULL"
    (true? v) "TRUE"
    (false? v) "FALSE"
    (coll? v) (coll-str v)
    :else (parameterize v)))

;;*****************************************************
;; Binding macros
;;*****************************************************

(defmacro bind-query [query & body]
  `(binding [*bound-table* (table-alias ~query)]
     ~@body))

(defmacro bind-params [& body]
  `(binding [*bound-params* (atom [])]
     (let [query# (do ~@body)
           params# (if (:params query#)
                     (concat (:params query#) @*bound-params*)
                     @*bound-params*)]
       (assoc query# :params params#))))

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
        alias-clause (if (:alias query)
                       (str " AS " (:alias query))
                       "")
        neue-sql (str "SELECT " clauses-str " FROM " (table-str (:table query)) alias-clause)]
    (assoc query :sql-str neue-sql)))

(defn sql-update [query]
  (let [neue-sql (str "UPDATE " (table-str (:table query)))]
    (assoc query :sql-str neue-sql)))

(defn sql-delete [query]
  (let [neue-sql (str "DELETE FROM " (table-str (:table query)))]
    (assoc query :sql-str neue-sql)))

(defn sql-insert [query]
  (let [ins-keys (keys (first (:values query)))
        keys-clause (comma (map name ins-keys))
        ins-values (insert-values-clause ins-keys (:values query))
        values-clause (comma ins-values)
        neue-sql (str "INSERT INTO " (table-str (:table query)) " (" keys-clause ") VALUES " values-clause)]
    (assoc query :sql-str neue-sql)))


;;*****************************************************
;; Sql parts
;;*****************************************************

(defn sql-set [query]
  (bind-query {}
              (let [clauses (map (comp :generated kv-clause) (:set-fields query))
                    clauses-str (string/join ", " clauses)
                    neue-sql (str " SET " clauses-str)]
    (update-in query [:sql-str] str neue-sql))))

(defn sql-joins [query]
  (let [clauses (for [[type table pk fk] (:joins query)]
                  (join-clause :left table pk fk))
        clauses-str (string/join " " clauses)]
    (update-in query [:sql-str] str clauses-str)))

(defn sql-where [query]
  (if (seq (:where query))
    (let [clauses (map #(if (map? %) (str-value %) %) (:where query))
          clauses-str (string/join " AND " clauses)
          neue-sql (str " WHERE " clauses-str)]
      (update-in query [:sql-str] str neue-sql))
    query))

(defn sql-order [query]
  (if (seq (:order query))
    (let [clauses (for [[k dir] (:order query)]
                    (str (str-value k) " " (string/upper-case (name dir))))
          clauses-str (string/join ", " clauses)
          neue-sql (str " ORDER BY " clauses-str)]
      (update-in query [:sql-str] str neue-sql))
    query))

(defn sql-limit-offset [{:keys [limit offset] :as query}]
  (let [limit-sql (when limit
                    (str " LIMIT " limit))
        offset-sql (when offset
                     (str " OFFSET " offset))]
    (update-in query [:sql-str] str limit-sql offset-sql)))

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
  (bind-params
    (-> query 
      (sql-select)
      (sql-joins)
      (sql-where)
      (sql-order)
      (sql-limit-offset))))

(defmethod ->sql :update [query]
  (bind-params
    (-> query 
      (sql-update)
      (sql-set)
      (sql-where))))

(defmethod ->sql :delete [query]
  (bind-params
    (-> query 
      (sql-delete)
      (sql-where))))

(defmethod ->sql :insert [query]
  (bind-params
    (-> query 
      (sql-insert))))
