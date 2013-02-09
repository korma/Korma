(ns korma.sql.engine
  (:require [clojure.string :as string]
            [clojure.walk :as walk]
            [korma.config :as conf]
            [korma.sql.engine :as user]
            [korma.sql.utils :as utils]))

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
  (utils/comma-separated (str-values vs)))

(defn wrap-values [vs]
  (if (seq vs)
    (utils/wrap (comma-values vs))
    "(NULL)"))

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
            (swap! *bound-params* utils/vconcat (:params sub))
            (utils/wrap (:sql-str sub)))
      :else (pred-map v))))

(defn table-alias [{:keys [table alias]}]
  (or alias table))

(defn table-identifier [table-name]
  (let [parts (string/split table-name #"\.")]
    (if (next parts)
      (string/join "." (map delimit-str parts))
      (delimit-str table-name))))

(defn field-identifier [field]
  (cond
    (map? field) (map-val field)
    (string? field) field
    (= "*" (name field)) "*"
    :else (let [field-name (name field)
                parts (string/split field-name #"\.")]
            (if-not (next parts)
              (delimit-str field-name)
              (string/join "." (map delimit-str parts))))))

(defn prefix [ent field]
  (let [field-name (field-identifier field)
        not-already-prefixed? (and (keyword? field)
                                   (not (*bound-aliases* field))
                                   (= -1 (.indexOf field-name ".")))]
    (if not-already-prefixed?
        (let [table (if (string? ent)
                      ent
                      (table-alias ent))]
          (str (table-identifier table) "." field-name))
      field-name)))

(defn try-prefix [v]
  (if (and (keyword? v)
           *bound-table*)
    (utils/generated (prefix *bound-table* v))
    v))

(defn alias-clause [alias]
  (when alias
    (str " AS " (delimit-str (name alias)))))

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
  (if (utils/special-map? v)
    (map-val v)
    (let [tstr (cond
                 (string? v) v
                 (map? v) (:table v)
                 :else (name v))]
      (table-identifier tstr))))

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

(defn not-nil? [& vs]
  (every? #(not (nil? %)) vs))

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

;;*****************************************************
;; Predicates
;;*****************************************************

(def predicates {'like 'korma.sql.fns/pred-like
                 'and 'korma.sql.fns/pred-and
                 'or 'korma.sql.fns/pred-or
                 'not 'korma.sql.fns/pred-not
                 'in 'korma.sql.fns/pred-in
                 'not-in 'korma.sql.fns/pred-not-in
                 'between 'korma.sql.fns/pred-between
                 '> 'korma.sql.fns/pred->
                 '< 'korma.sql.fns/pred-<
                 '>= 'korma.sql.fns/pred->=
                 '<= 'korma.sql.fns/pred-<=
                 'not= 'korma.sql.fns/pred-not=
                 '= 'korma.sql.fns/pred-=})


(defn do-infix [k op v]
  (string/join " " [(str-value k) op (str-value v)]))

(defn do-group [op vs]
  (utils/wrap (string/join op (str-values vs))))

(defn do-wrapper [op v]
  (str op (utils/wrap (str-value v))))

(defn do-trinary [k op v1 sep v2]
  (utils/wrap (string/join " " [(str-value k) op (str-value v1) sep (str-value v2)])))

(defn trinary [k op v1 sep v2]
  (utils/pred do-trinary [(try-prefix k) op (try-prefix v1) sep (try-prefix v2)]))

(defn infix [k op v]
  (utils/pred do-infix [(try-prefix k) op (try-prefix v)]))

(defn group-with [op vs]
  (utils/pred do-group [op (doall (map pred-map vs))]))

(defn wrapper [op v]
  (utils/pred do-wrapper [op v]))

(defn pred-and [& args]
  (group-with " AND " args))

(defn pred-= [k v]
  (cond
   (not-nil? k v) (infix k "=" v)
   (not-nil? k) (infix k "IS" v)
   (not-nil? v) (infix v "IS" k)))

(defn set= [[k v]]
  (map-val (infix k "=" v)))

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
                 'stdev 'korma.sql.fns/agg-stdev
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

(defn from-table [v & [already-aliased?]]
  (cond
    (string? v) (table-str v)
    (vector? v) (let [[table alias] v]
                  (str (from-table table :aliased) (alias-clause alias)))
    (map? v) (if (:table v)
               (let [{:keys [table alias]} v]
                 (str (table-str table) (when-not already-aliased? (alias-clause alias))))
               (map-val v))
    :else (table-str v)))

(defn join-clause [join-type table on-clause]
  (let [join-type (string/upper-case (name join-type))
        table (from-table table)
        join (str " " join-type " JOIN " table " ON ")]
    (str join (str-value on-clause))))

(defn insert-values-clause [ks vs]
  (for [v vs]
    (wrap-values (map #(get v %) ks))))

;;*****************************************************
;; Query types
;;*****************************************************

(defn sql-select [query]
  (let [clauses (map field-str (:fields query))
        modifiers-clause (when (seq (:modifiers query))
                           (str (reduce str (:modifiers query)) " "))
        clauses-str (utils/comma-separated clauses)
        neue-sql (str "SELECT " modifiers-clause clauses-str)]
    (assoc query :sql-str neue-sql)))

(defn sql-update [query]
  (let [neue-sql (str "UPDATE " (table-str query))]
    (assoc query :sql-str neue-sql)))

(defn sql-delete [query]
  (let [neue-sql (str "DELETE FROM " (table-str query))]
    (assoc query :sql-str neue-sql)))

(def noop-query "DO 0")

(defn sql-insert [query]
  (let [ins-keys (keys (first (:values query)))
        keys-clause (utils/comma-separated (map field-identifier ins-keys))
        ins-values (insert-values-clause ins-keys (:values query))
        values-clause (utils/comma-separated ins-values)
        neue-sql (if-not (empty? ins-keys)
                   (str "INSERT INTO " (table-str query) " " (utils/wrap keys-clause) " VALUES " values-clause)
                   noop-query)]
    (assoc query :sql-str neue-sql)))

;;*****************************************************
;; Sql parts
;;*****************************************************

(defn sql-set [query]
  (bind-query {}
              (let [fields (for [[k v] (:set-fields query)]
                             [(utils/generated (field-identifier k)) (utils/generated (str-value v))])
                    clauses (map set= fields)
                    clauses-str (utils/comma-separated clauses)
                    neue-sql (str " SET " clauses-str)]
    (update-in query [:sql-str] str neue-sql))))

(defn sql-joins [query]
  (let [clauses (for [[type table clause] (:joins query)]
                  (join-clause type table clause))
        tables (utils/comma-separated (map from-table (:from query)))
        clauses-str (utils/left-assoc (cons (str tables (first clauses))
                                            (rest clauses)))]
    (update-in query [:sql-str] str " FROM " clauses-str)))

(defn- sql-where-or-having [where-or-having-kw where-or-having-str query]
  (if (empty? (get query where-or-having-kw))
    query
    (let [clauses (map #(if (map? %) (map-val %) %)
      (get query where-or-having-kw))
          clauses-str (string/join " AND " clauses)
          neue-sql (str where-or-having-str clauses-str)]
      (if (= "()" clauses-str)
        query
        (update-in query [:sql-str] str neue-sql)))))

(def sql-where  (partial sql-where-or-having :where  " WHERE "))
(def sql-having (partial sql-where-or-having :having " HAVING "))

(defn sql-order [query]
  (if (seq (:order query))
    (let [clauses (for [[k dir] (:order query)]
                    (str (str-value k) " " (string/upper-case (name dir))))
          clauses-str (utils/comma-separated clauses)
          neue-sql (str " ORDER BY " clauses-str)]
      (update-in query [:sql-str] str neue-sql))
    query))

(defn sql-group [query]
  (if (seq (:group query))
    (let [clauses (map field-str (:group query))
          clauses-str (utils/comma-separated clauses)
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
;; Combination Queries
;;*****************************************************

(defn- sql-combination-query [type query]
  (let [sub-query-sqls (map map-val (:queries query))
        neue-sql (string/join (str " " type " ") sub-query-sqls)]
    (assoc query :sql-str neue-sql)))

(def sql-union     (partial sql-combination-query "UNION"))
(def sql-union-all (partial sql-combination-query "UNION ALL"))
(def sql-intersect (partial sql-combination-query "INTERSECT"))

;;*****************************************************
;; To sql
;;*****************************************************

(defmacro ^{:private true} bind-params [& body]
  `(binding [*bound-params* (atom [])]
     (let [query# (do ~@body)]
       (update-in query# [:params] utils/vconcat @*bound-params*))))

(defn ->sql [query]
  (bind-params
   (case (:type query)
     :union (-> query sql-union sql-order)
     :union-all (-> query sql-union-all sql-order)
     :intersect (-> query sql-intersect sql-order)
     :select (-> query 
                 sql-select
                 sql-joins
                 sql-where
                 sql-group
                 sql-having
                 sql-order
                 sql-limit-offset)
     :update (-> query 
                 sql-update
                 sql-set
                 sql-where)
     :delete (-> query 
                 sql-delete
                 sql-where)
     :insert (-> query 
                 sql-insert))))
