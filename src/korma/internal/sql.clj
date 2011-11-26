(ns korma.internal.sql
  (:require [clojure.string :as string]
            [korma.config :as conf]
            [clojure.walk :as walk]))


;;*****************************************************
;; dynamic vars
;;*****************************************************

(def ^{:dynamic true} *bound-table* nil)
(def ^{:dynamic true} *bound-aliases* #{})
(def ^{:dynamic true} *bound-params* nil)
(def ^{:dynamic true} *bound-options* nil)

;;*****************************************************
;; delimiters
;;*****************************************************

(defn delimit-str [s]
  (let [{:keys [naming delimiters]} (or *bound-options* @conf/options)
        [begin end] delimiters
        ->field (:fields naming)]
    (str begin (->field s) end)))

;;*****************************************************
;; Str utils
;;*****************************************************

(declare kv-result kv-clause str-value map-val)

(defn generated [s]
  {::generated s})

(defn sub-query [s]
  {::sub s})

(defn is-generated? [m]
  (or (::pred m)
      (::func m)
      (::sub m)
      (::generated m)))

(defn table-alias [{:keys [type table alias]}]
  (or alias table))


(defn field-identifier [field]
  (cond
    (map? field) (map-val field)
    (string? field) field
    (= :* field) (name field)
    :else (let [field-name (name field)
                parts (string/split field-name #"\.")]
            (if-not (next parts)
              (delimit-str field-name)
              (string/join "." (map delimit-str parts))))))

(defn prefix [ent field]
  (let [field-name (field-identifier field)]
    ;;check if it's already prefixed
    (if (and (keyword? field)
             (not (*bound-aliases* field))
             (= -1 (.indexOf field-name ".")))
      (let [table (if (string? ent)
                    ent
                    (table-alias ent))]
        (str (delimit-str table) "." field-name))
      field-name)))

(defn try-prefix [v]
  (if (and (keyword? v)
           *bound-table*)
    (generated (prefix *bound-table* v))
    v))

(defn comma [vs]
  (string/join ", " vs))

(defn map->where [m]
  (str "(" (string/join " AND " (map kv-clause m)) ")"))

(defn map-val [v]
  (let [func (::func v)
        generated (::generated v)
        args (::args v)
        sub (::sub v)
        pred (::pred v)]
    (cond
      generated generated
      pred (apply pred args)
      func (let [vs (comma (map str-value args))]
             (format func vs))
      sub (do
            (swap! *bound-params* #(vec (concat % (:params sub))))
            (str "(" (:sql-str sub) ")"))
      :else (map->where v))))

(defn alias-clause [alias]
  (when alias
    (str " " (delimit-str (name alias)))))

(defn field-str [v]
    (let [[fname alias] (if (vector? v)
                          v
                          [v nil])
          fname (cond
                  (map? fname) (map-val fname)
                  *bound-table* (prefix *bound-table* fname)
                  :else (field-identifier fname))
          alias-str (alias-clause alias)]
      (str fname alias-str)))

(defn coll-str [v]
  (str "(" (comma (map str-value v)) ")"))

(defn table-str [v]
  (let [tstr (cond
               (string? v) v
               (map? v) (:table v)
               :else (name v))]
    (delimit-str tstr)))

(defn parameterize [v]
  (when *bound-params*
    (swap! *bound-params* conj v))
  "?")

(defn str-value [v]
  (cond
    (map? v) (map-val v)
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
  `(binding [*bound-table* (if (= :select (:type ~query))
                             (table-alias ~query)
                             (:table ~query))
             *bound-aliases* (or (:aliases ~query) #{})
             *bound-options* (or (:options ~query) @conf/options)]
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

(declare pred-map)

(defn do-infix [k op v]
  (str (str-value k) " " op " " (str-value v)))

(defn do-group [op vs]
  (str "(" (string/join op (map str-value vs)) ")"))

(defn infix [k op v]
  {::pred do-infix ::args [(try-prefix k) op (try-prefix v)]})

(defn group-with [op vs]
  {::pred do-group ::args [op (doall (map pred-map vs))]})

(defn pred-and [& args] (group-with " AND " args))
(defn pred-or [& args] (group-with " OR " args))
(defn pred-not [v] {::func "NOT(%s)" :args v})

(defn pred-in [k v] (infix k "IN" v))
(defn pred-> [k v] (infix k ">" v))
(defn pred-< [k v] (infix k "<" v))
(defn pred->= [k v] (infix k ">=" v))
(defn pred-<= [k v] (infix k "<=" v))
(defn pred-like [k v] (infix k "LIKE" v))

(defn pred-= [k v] (cond 
                     (and k v) (infix k "=" v)
                     k (infix k "IS" v)
                     v (infix v "IS" k)))
(defn pred-not= [k v] (cond
                        (and k v) (infix k "!=" v)
                        k (infix k "IS NOT" v)
                        v (infix v "IS NOT" k)))

(defn pred-map [m]
  (if (and (map? m)
           (not (is-generated? m)))
    (apply pred-and (doall (map kv-result m)))
   m))

;;*****************************************************
;; Aggregates
;;*****************************************************

(def aggregates {'count 'korma.internal.sql/agg-count
                 'min 'korma.internal.sql/agg-min
                 'max 'korma.internal.sql/agg-max
                 'first 'korma.internal.sql/agg-first
                 'last 'korma.internal.sql/agg-last
                 'avg 'korma.internal.sql/agg-avg
                 'sum 'korma.internal.sql/agg-sum})

(defn sql-func [op & vs]
  {::func (str (string/upper-case op) "(%s)") ::args (map try-prefix vs)})

(defn agg-count [v] (sql-func "COUNT" v))
(defn agg-sum [v] (sql-func "SUM" v))
(defn agg-avg [v] (sql-func "AVG" v))
(defn agg-min [v] (sql-func "MIN" v))
(defn agg-max [v] (sql-func "MAX" v))
(defn agg-first [v] (sql-func "FIRST" v))
(defn agg-last [v] (sql-func "LAST" v))

(defn parse-aggregate [form]
  (if (string? form)
    form
    (walk/postwalk-replace aggregates form)))

;;*****************************************************
;; Clauses
;;*****************************************************

(defn kv-result [[k v]]
  (if-not (vector? v)
    (pred-= k v)
    (let [[func value] v
          pred? (predicates func)
          func (if pred?
                 (resolve pred?) 
                 func)]
      (func k value))))

(defn kv-clause [pair]
    (map-val (kv-result pair)))

(defn from-table [v]
  (cond
    (string? v) (table-str v)
    (vector? v) (let [[table alias] v]
                  (str (from-table table) (alias-clause alias)))
    (map? v) (if (:table v)
               (let [{:keys [table alias]} v]
                 (str (table-str table) (alias-clause alias)))
               (map-val v))
    :else (table-str v)))

(defn join-clause [join-type table pk fk]
  (let [join-type (string/upper-case (name join-type))
        table (from-table table)
        join (str " " join-type " JOIN " table " ON ")
        on-clause (str (field-str pk) " = " (field-str fk))]
    (str join on-clause)))

(defn insert-values-clause [ks vs]
  (for [v vs]
    (str "(" (comma (map str-value (map #(get v %) ks))) ")")))

;;*****************************************************
;; Query types
;;*****************************************************

(defn sql-select [query]
  (let [clauses (if-not (seq (:fields query))
                  [(field-str :*)]
                  (map field-str (:fields query)))
        modifiers-clause (when (seq (:modifiers query))
                           (apply str (cons (:modifiers query) " ")))
        clauses-str (comma clauses)
        tables (comma (map from-table (:from query)))
        neue-sql (str "SELECT " modifiers-clause clauses-str " FROM " tables)]
    (assoc query :sql-str neue-sql)))

(defn sql-update [query]
  (let [neue-sql (str "UPDATE " (table-str query))]
    (assoc query :sql-str neue-sql)))

(defn sql-delete [query]
  (let [neue-sql (str "DELETE FROM " (table-str query))]
    (assoc query :sql-str neue-sql)))

(defn sql-insert [query]
  (let [ins-keys (keys (first (:values query)))
        keys-clause (comma (map field-identifier ins-keys))
        ins-values (insert-values-clause ins-keys (:values query))
        values-clause (comma ins-values)
        neue-sql (str "INSERT INTO " (table-str query) " (" keys-clause ") VALUES " values-clause)]
    (assoc query :sql-str neue-sql)))

;;*****************************************************
;; Sql parts
;;*****************************************************

(defn sql-set [query]
  (bind-query {}
              (let [fields (for [[k v] (:set-fields query)] [(generated (field-identifier k)) (generated (str-value v))])
                    clauses (map kv-clause fields)
                    clauses-str (string/join ", " clauses)
                    neue-sql (str " SET " clauses-str)]
    (update-in query [:sql-str] str neue-sql))))

(defn sql-joins [query]
  (let [clauses (for [[type table pk fk] (:joins query)]
                  (join-clause type table pk fk))
        clauses-str (string/join "" clauses)]
    (update-in query [:sql-str] str clauses-str)))

(defn sql-where [query]
  (if (seq (:where query))
    (let [clauses (map #(if (map? %) (map-val %) %)
                       (:where query))
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

(defn sql-group [query]
  (if (seq (:group query))
    (let [clauses (map field-str (:group query))
          clauses-str (string/join ", " clauses)
          neue-sql (str " GROUP BY " clauses-str)]
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
      (sql-group)
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
