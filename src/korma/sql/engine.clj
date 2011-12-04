(ns korma.sql.engine
  (:require [clojure.string :as string]
            [korma.sql.utils :as utils]
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

(declare pred-map str-value)

(defn str-values [vs]
  (map str-value vs))

(defn comma-values [vs]
  (utils/comma (str-values vs)))

(defn wrap-values [vs]
  (utils/wrap (comma-values vs)))

(defn map-val [v]
  (let [func (utils/func? v)
        generated (utils/generated? v)
        args (utils/args? v)
        sub (utils/sub-query? v)
        pred (utils/pred? v)]
    (cond
      generated generated
      pred (apply pred args)
      func (let [vs (comma-values args)]
             (format func vs))
      sub (do
            (swap! *bound-params* #(vec (concat % (:params sub))))
            (utils/wrap (:sql-str sub)))
      :else (pred-map v))))

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
    (utils/generated (prefix *bound-table* v))
    v))

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
  (wrap-values v))

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
;; Bindings
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

(def predicates {'like 'korma.sql.fns/pred-like
                 'and 'korma.sql.fns/pred-and
                 'or 'korma.sql.fns/pred-or
                 'not 'korma.sql.fns/pred-not
                 'in 'korma.sql.fns/pred-in
                 '> 'korma.sql.fns/pred->
                 '< 'korma.sql.fns/pred-<
                 '>= 'korma.sql.fns/pred->=
                 '<= 'korma.sql.fns/pred-<=
                 'not= 'korma.sql.fns/pred-not=
                 '= 'korma.sql.fns/pred-=})


(defn do-infix [k op v]
  (utils/space [(str-value k) op (str-value v)]))

(defn do-group [op vs]
  (utils/wrap (string/join op (str-values vs))))

(defn do-wrapper [op v]
  (str op (utils/wrap (str-value v))))

(defn infix [k op v]
  (utils/pred do-infix [(try-prefix k) op (try-prefix v)]))

(defn group-with [op vs]
  (utils/pred do-group [op (doall (map pred-map vs))]))

(defn wrapper [op v]
  (utils/pred do-wrapper [op v]))

(defn pred-and [& args] (group-with " AND " args))
(defn pred-= [k v] (cond 
                     (and k v) (infix k "=" v)
                     k (infix k "IS" v)
                     v (infix v "IS" k)))

(defn pred-vec [[k v]]
  (let [[func value] (if (vector? v)
                       v
                       [pred-= v])
        pred? (predicates func)
        func (if pred?
               (resolve pred?) 
               func)]
    (func k value)))

(defn pred-map [m]
  (if (and (map? m)
           (not (utils/special-map? m)))
    (apply pred-and (doall (map pred-vec m)))
   m))

(defn parse-where [form]
  (if (string? form)
    form
    (walk/postwalk-replace predicates form)))

;;*****************************************************
;; Aggregates
;;*****************************************************

(def aggregates {'count 'korma.sql.fns/agg-count
                 'min 'korma.sql.fns/agg-min
                 'max 'korma.sql.fns/agg-max
                 'first 'korma.sql.fns/agg-first
                 'last 'korma.sql.fns/agg-last
                 'avg 'korma.sql.fns/agg-avg
                 'sum 'korma.sql.fns/agg-sum})

(defn sql-func [op & vs]
  (utils/func (str (string/upper-case op) "(%s)") (map try-prefix vs)))

(defn parse-aggregate [form]
  (if (string? form)
    form
    (walk/postwalk-replace aggregates form)))

;;*****************************************************
;; Clauses
;;*****************************************************

(defn kv-clause [pair]
    (map-val (pred-vec pair)))

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
    (wrap-values (map #(get v %) ks))))

(defn column-clause [{:keys [type col attrs] :as column}]
  (let [field (delimit-str (name col))
        field-type (cond 
                     (vector? type) (do-wrapper (name (first type)) (second type))
                     (map? type) (map-val type)
                     :else (name type))
        attr-clause (utils/space (for [v attrs] 
                                   (if (keyword? v) 
                                     (name v)
                                     (str-value v))))
        clause (utils/space [field field-type attr-clause])]
    clause))

;;*****************************************************
;; Query types
;;*****************************************************

(defn sql-select [query]
  (let [clauses (if-not (seq (:fields query))
                  [(field-str :*)]
                  (map field-str (:fields query)))
        modifiers-clause (when (seq (:modifiers query))
                           (apply str (cons (:modifiers query) " ")))
        clauses-str (utils/comma clauses)
        tables (utils/comma (map from-table (:from query)))
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
        keys-clause (utils/comma (map field-identifier ins-keys))
        ins-values (insert-values-clause ins-keys (:values query))
        values-clause (utils/comma ins-values)
        neue-sql (str "INSERT INTO " (table-str query) " " (utils/wrap keys-clause) " VALUES " values-clause)]
    (assoc query :sql-str neue-sql)))

(defn sql-create [query]
  (let [neue-sql (str "CREATE TABLE " (table-str query) " ")]
    (assoc query :sql-str neue-sql)))

(defn sql-drop [query]
  (let [neue-sql (str "DROP TABLE " (table-str query))]
    (assoc query :sql-str neue-sql)))

;;*****************************************************
;; Sql parts
;;*****************************************************

(defn sql-set [query]
  (bind-query {}
              (let [fields (for [[k v] (:set-fields query)] [(utils/generated (field-identifier k)) (utils/generated (str-value v))])
                    clauses (map kv-clause fields)
                    clauses-str (utils/comma clauses)
                    neue-sql (str " SET " clauses-str)]
    (update-in query [:sql-str] str neue-sql))))

(defn sql-joins [query]
  (let [clauses (for [[type table pk fk] (:joins query)]
                  (join-clause type table pk fk))
        clauses-str (apply str clauses)]
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
          clauses-str (utils/comma clauses)
          neue-sql (str " ORDER BY " clauses-str)]
      (update-in query [:sql-str] str neue-sql))
    query))

(defn sql-group [query]
  (if (seq (:group query))
    (let [clauses (map field-str (:group query))
          clauses-str (utils/comma clauses)
          neue-sql (str " GROUP BY " clauses-str)]
      (update-in query [:sql-str] str neue-sql))
    query))

(defn sql-limit-offset [{:keys [limit offset] :as query}]
  (let [limit-sql (when limit
                    (str " LIMIT " limit))
        offset-sql (when offset
                     (str " OFFSET " offset))]
    (update-in query [:sql-str] str limit-sql offset-sql)))

(defn sql-columns [query]
  (let [clauses (map column-clause (:columns query))
        clauses-str (utils/wrap-all clauses)]
    (update-in query [:sql-str] str clauses-str)))

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

(defmethod ->sql :create [query]
  (bind-params
    (-> query
        (sql-create)
        (sql-columns))))

(defmethod ->sql :alter [query]
  (bind-params
    (-> query
        )))

(defmethod ->sql :drop [query]
  (bind-params
    (-> query
        (sql-drop))))


