(ns korma.core
  "Core querying and entity functions"
  (:refer-clojure :exclude [update])
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [korma.config :as conf]
            [korma.db :as db]
            [korma.sql.engine :as eng]
            [korma.sql.fns :as sfns]
            [korma.sql.utils :as utils])
  (:use [korma.sql.engine :only [bind-query]]))

(def ^{:dynamic true} *exec-mode* false)
(declare get-rel)

;;*****************************************************
;; Query types
;;*****************************************************

(defn empty-query [ent]
  (let [ent (if (keyword? ent)
              (name ent)
              ent)
        [ent table alias db opts] (if (string? ent)
                                    [{:table ent} ent nil nil nil]
                                    [ent (:table ent) (:alias ent) (:db ent) (get-in ent [:db :options])])]
    {:ent ent
     :table table
     :db db
     :options opts
     :alias alias}))

(defmacro ^{:private true} make-query [ent m]
  `(let [ent# ~ent]
     (if (:type ent#)
       ent#
       (let [~'this-query (empty-query ent#)]
         (merge ~'this-query ~m)))))

(defn select*
  "Create a select query with fields provided in Ent.  If fields are not provided,
  create an empty select query. Ent can either be an entity defined by defentity,
  or a string of the table name"
  [ent]
  (let [default-fields (not-empty (:fields ent))]
    (make-query ent {:type :select
                     :fields (or default-fields [::*])
                     :from [(:ent this-query)]
                     :modifiers []
                     :joins []
                     :where []
                     :order []
                     :aliases #{}
                     :group []
                     :results :results})))

(defn update*
  "Create an empty update query. Ent can either be an entity defined by defentity,
  or a string of the table name."
  [ent]
  (make-query ent {:type :update
                   :fields {}
                   :where []
                   :post-queries [first]}))

(defn delete*
  "Create an empty delete query. Ent can either be an entity defined by defentity,
  or a string of the table name"
  [ent]
  (make-query ent {:type :delete
                   :where []
                   :post-queries [first]}))

(defn insert*
  "Create an empty insert query. Ent can either be an entity defined by defentity,
  or a string of the table name"
  [ent]
  (make-query ent {:type :insert
                   :values []
                   :results :keys}))

(defn union*
  "Create an empty union query."
  []
  {:type :union
   :queries []
   :order []
   :results :results})

(defn union-all*
  "Create an empty union-all query."
  []
  {:type :union-all
   :queries []
   :order []
   :results :results})

(defn intersect*
  "Create an empty intersect query."
  []
  {:type :intersect
   :queries []
   :order []
   :results :results})

;;*****************************************************
;; Query macros
;;*****************************************************

(defn- make-query-then-exec [query-fn-var body & args]
  `(let [query# (-> (~query-fn-var ~@args)
                    ~@body)]
     (exec query#)))

(defmacro select
  "Creates a select query, applies any modifying functions in the body and then
  executes it. `ent` is either a string or an entity created by defentity.

  ex: (select user
        (fields :name :email)
        (where {:id 2}))"
  [ent & body]
  (make-query-then-exec #'select* body ent))

(defmacro update
  "Creates an update query, applies any modifying functions in the body and then
  executes it. `ent` is either a string or an entity created by defentity.
  Returns number of updated rows as provided by the JDBC driver.

  ex: (update user
        (set-fields {:name \"chris\"})
        (where {:id 4}))"
  [ent & body]
  (make-query-then-exec #'update* body ent))

(defmacro delete
  "Creates a delete query, applies any modifying functions in the body and then
  executes it. `ent` is either a string or an entity created by defentity.
  Returns number of deleted rows as provided by the JDBC driver.

  ex: (delete user
        (where {:id 7}))"
  [ent & body]
  (make-query-then-exec #'delete* body ent))

(defmacro insert
  "Creates an insert query, applies any modifying functions in the body
  and then executes it. `ent` is either a string or an entity created by
  defentity. The return value is the last inserted item, but its
  representation is dependent on the database driver used
  (e.g. postgresql returns the full row as a hash while MySQL and MSSQL
  both return a {:generated_key <ID>} hash)

  ex: (insert user
        (values [{:name \"chris\"} {:name \"john\"}]))"
  [ent & body]
  (make-query-then-exec #'insert* body ent))

(defmacro union
  "Creates a union query, applies any modifying functions in the body and then
  executes it.

  ex: (union
        (queries (subselect user
                   (where {:id 7}))
                 (subselect user-backup
                   (where {:id 7})))
        (order :name))"
  [& body]
  (make-query-then-exec #'union* body))

(defmacro union-all
  "Creates a union-all query, applies any modifying functions in the body and then
  executes it.

  ex: (union-all
        (queries (subselect user
                   (where {:id 7}))
                 (subselect user-backup
                   (where {:id 7})))
        (order :name))"
  [& body]
  (make-query-then-exec #'union-all* body))

(defmacro intersect
  "Creates an intersect query, applies any modifying functions in the body and then
  executes it.

  ex: (intersect
        (queries (subselect user
                   (where {:id 7}))
                 (subselect user-backup
                   (where {:id 8})))
        (order :name))"
  [& body]
  (make-query-then-exec #'intersect* body))

;;*****************************************************
;; Query parts
;;*****************************************************

(defn- update-fields [query fs]
  (let [[first-cur] (:fields query)]
    (if (= first-cur ::*)
      (assoc query :fields fs)
      (update-in query [:fields] utils/vconcat fs))))

(defn fields
  "Set the fields to be selected in a query. Fields can either be a keyword
  or a vector of two keywords [field alias]:

  (fields query :name [:firstname :first])"
  [query & vs]
  (let [aliases (set (map second (filter vector? vs)))]
    (-> query
        (update-in [:aliases] set/union aliases)
        (update-fields (vec vs)))))

(defn set-fields
  "Set the fields and values for an update query."
  [query fields-map]
  (update-in query [:set-fields] merge fields-map))

(defn from
  "Add tables to the from clause."
  [query table]
  (update-in query [:from] conj table))

(defn- where-or-having-form [where*-or-having* query form]
  `(let [q# ~query]
     (~where*-or-having* q#
                         (bind-query q#
                                     (eng/pred-map ~(eng/parse-where `~form))))))

(defn where*
  "Add a where clause to the query. Clause can be either a map or a string, and
  will be AND'ed to the other clauses."
  [query clause]
  (update-in query [:where] conj clause))

(defmacro where
  "Add a where clause to the query, expressing the clause in clojure expressions
  with keywords used to reference fields.
  e.g. (where query (or (= :hits 1) (> :hits 5)))

  Available predicates: and, or, =, not=, <, >, <=, >=, in, like, not, between

  Where can also take a map at any point and will create a clause that compares keys
  to values. The value can be a vector with one of the above predicate functions
  describing how the key is related to the value:
    (where query {:name [like \"chris\"]})"
  [query form]
  (where-or-having-form #'where* query form))

(defn having*
  "Add a having clause to the query. Clause can be either a map or a string, and
  will be AND'ed to the other clauses."
  [query clause]
  (update-in query [:having] conj clause))

(defmacro having
  "Add a having clause to the query, expressing the clause in clojure expressions
  with keywords used to reference fields.
  e.g. (having query (or (= :hits 1) (> :hits 5)))

  Available predicates: and, or, =, not=, <, >, <=, >=, in, like, not, between

  Having can also take a map at any point and will create a clause that compares
  keys to values. The value can be a vector with one of the above predicate
  functions describing how the key is related to the value:
    (having query {:name [like \"chris\"})

  Having only works if you have an aggregation, using it without one will cause
  an error."
  [query form]
  (where-or-having-form #'having* query form))

(defn order
  "Add an ORDER BY clause to a select, union, union-all, or intersect query.
  field should be a keyword of the field name, dir is ASC by default.

  (order query :created :asc)"
  ([query field dir]
   (update-in query [:order] conj [field (or dir :ASC)]))
  ([query field]
   (order query field :ASC)))

(defn values
  "Add records to an insert clause. values can either be a vector of maps or a
  single map.

  (values query [{:name \"john\"} {:name \"ed\"}])"
  [query values]
  (update-in query [:values] utils/vconcat (if (map? values)
                                             [values]
                                             values)))

(defn join* [query type table clause]
  (update-in query [:joins] conj [type table clause]))

(defn add-joins
  ([query ent rel]
   (add-joins query ent rel :left))
  ([query ent rel type]
   (if-let [join-table (:join-table rel)]
     (-> query
         (join* type join-table (sfns/pred-= (:lpk rel) @(:lfk rel)))
         (join* type ent (sfns/pred-= @(:rfk rel) (:rpk rel))))
     (join* query type ent (sfns/pred-= (:pk rel) (:fk rel))))))

(defmacro join
  "Add a join clause to a select query, specifying an entity defined by defentity, or the table name to
  join and the predicate to join on. If the entity relationship uses a join
  table then two clauses will be added. Otherwise, only one clause
  will be added.

  (join query addresses)
  (join query :right addresses)
  (join query addresses (= :addres.users_id :users.id))
  (join query :right addresses (= :address.users_id :users.id))"
  {:arglists '([query ent] [query type ent] [query table clause] [query type table clause])}
  ([query ent]
   `(join ~query :left ~ent))
  ([query type-or-table ent-or-clause]
   `(if (entity? ~ent-or-clause)
      (let [q# ~query
            e# ~ent-or-clause
            rel# (get-rel (:ent q#) e#)
            type# ~type-or-table]
        (add-joins q# e# rel# type#))
      (join ~query :left ~type-or-table ~ent-or-clause)))
  ([query type table clause]
   `(join* ~query ~type ~table (eng/pred-map ~(eng/parse-where clause)))))

(defn post-query
  "Add a function representing a query that should be executed for each result
  in a select. This is done lazily over the result set."
  [query post]
  (update-in query [:post-queries] conj post))

(defn limit
  "Add a limit clause to a select query."
  [query v]
  (assoc query :limit v))

(defn offset
  "Add an offset clause to a select query."
  [query v]
  (assoc query :offset v))

(defn group
  "Add a group-by clause to a select query"
  [query & fields]
  (update-in query [:group] utils/vconcat fields))

(defmacro aggregate
  "Use a SQL aggregator function, aliasing the results, and optionally grouping by
  a field:

  (select users
    (aggregate (count :*) :cnt :status))

  Aggregates available: count, sum, avg, min, max, first, last"
  [query agg alias & [group-by]]
  `(let [q# ~query]
     (bind-query q#
                 (let [res# (fields q# [(-> q# ~(eng/parse-aggregate agg)) ~alias])]
                   (if ~group-by
                     (group res# ~group-by)
                     res#)))))

(defn queries
  "Adds a group of queries to a union, union-all or intersect"
  [query & queries]
  (update-in query [:queries] utils/vconcat queries))

;;*****************************************************
;; Other sql
;;*****************************************************

(defn sqlfn*
  "Call an arbitrary SQL function by providing the name of the function
  and its params"
  [fn-name & params]
  (apply eng/sql-func (name fn-name) params))

(defmacro sqlfn
  "Call an arbitrary SQL function by providing func as a symbol or keyword
  and its params"
  [func & params]
  `(sqlfn* (quote ~func) ~@params))

(defmacro subselect
  "Create a subselect clause to be used in queries. This works exactly like
  (select ...) execept it will wrap the query in ( .. ) and make sure it can be
  used in any current query:

  (select users
    (where {:id [in (subselect users2 (fields :id))]}))"
  [& parts]
  `(utils/sub-query (query-only (select ~@parts))))

(defn modifier
  "Add a modifer to the beginning of a query:

  (select orders
    (modifier \"DISTINCT\"))"
  [query & modifiers]
  (update-in query [:modifiers] conj (reduce str modifiers)))

(defn raw
  "Embed a raw string of SQL in a query. This is used when Korma doesn't
  provide some specific functionality you're looking for:

  (select users
    (fields (raw \"PERIOD(NOW(), NOW())\")))"
  [s]
  (utils/generated s))

;;*****************************************************
;; Query exec
;;*****************************************************

(defmacro sql-only
  "Wrap around a set of queries so that instead of executing, each will return a
  string of the SQL that would be used."
  [& body]
  `(binding [*exec-mode* :sql]
     ~@body))

(defmacro dry-run
  "Wrap around a set of queries to print to the console all SQL that would
  be run and return dummy values instead of executing them."
  [& body]
  `(binding [*exec-mode* :dry-run]
     ~@body))

(defmacro query-only
  "Wrap around a set of queries to force them to return their query objects."
  [& body]
  `(binding [*exec-mode* :query]
     ~@body))

(defn as-sql
  "Force a query to return a string of SQL when (exec) is called."
  [query]
  (bind-query query (:sql-str (eng/->sql query))))

(defn- apply-posts
  [query results]
  (if-let [posts (seq (:post-queries query))]
    (let [post-fn (apply comp posts)]
      (post-fn results))
    results))

(defn- apply-transforms
  [query results]
  (if (#{:delete :update} (:type query))
    results
    (if-let [trans (-> query :ent :transforms seq)]
      (let [trans-fn (apply comp trans)]
        (if (sequential? results)
          (map trans-fn results)
          (trans-fn results)))
      results)))

(defn- apply-prepares
  [query]
  (if-let [preps (-> query :ent :prepares seq)]
    (let [prep-fn (apply comp preps)]
      (case (:type query)
        :insert (update-in query [:values] #(map prep-fn %))
        :update (update-in query [:set-fields] prep-fn)
        query))
    query))

(defn exec
  "Execute a query map and return the results."
  [query]
  (let [query (apply-prepares query)
        query (bind-query query (eng/->sql query))
        sql (:sql-str query)
        params (:params query)]
    (cond
      (:sql query) sql
      (= *exec-mode* :sql) sql
      (= *exec-mode* :query) query
      (= *exec-mode* :dry-run) (do
                                 (println "dry run ::" sql "::" (vec params))
                                 (let [result-keys (conj (->> query :ent :rel vals
                                                              (map deref)
                                                              (filter (comp #{:belongs-to} :rel-type))
                                                              (map :fk-key))
                                                         (-> query :ent :pk))
                                       results (apply-posts query [(zipmap result-keys (repeat 1))])]
                                   (first results)
                                   results))
      :else (let [results (db/do-query query)]
              (apply-transforms query (apply-posts query results))))))

(defn exec-raw
  "Execute a raw SQL string, supplying whether results should be returned. `sql`
  can either be a string or a vector of the sql string and its params. You can
  also optionally provide the connection to execute against as the first
  parameter.

  (exec-raw [\"SELECT * FROM users WHERE age > ?\" [5]] :results)"
  [conn? & [sql with-results?]]
  (let [sql-vec (fn [v] (if (vector? v) v [v nil]))
        [conn? [sql-str params] with-results?] (if (or (string? conn?)
                                                       (vector? conn?))
                                                 [nil (sql-vec conn?) sql]
                                                 [conn? (sql-vec sql) with-results?])]
    (db/do-query {:db conn? :results with-results? :sql-str sql-str :params params})))

;;*****************************************************
;; Entities
;;*****************************************************

(defn entity? [x] (= (type x) ::Entity))

(defn create-entity
  "Create an entity representing a table in a database."
  [table]
  ^{:type ::Entity}
  {:table table
   :name table
   :pk :id
   :db nil
   :transforms '()
   :prepares '()
   :fields []
   :rel {}})

(defn- simple-table-name [ent]
  (last (string/split (:table ent) #"\.")))

(defn- default-fk-name [ent]
  (cond
    (map? ent) (keyword (str (simple-table-name ent) "_id"))
    (var? ent) (recur @ent)
    :else      (throw (Exception. (str "Can't determine default fk for " ent)))))

(defn- many-to-many-keys [parent child {:keys [join-table lfk rfk]}]
  {:lpk (raw (eng/prefix parent (:pk parent)))
   :lfk (delay (raw (eng/prefix {:table (name join-table)} @lfk)))
   :rfk (delay (raw (eng/prefix {:table (name join-table)} @rfk)))
   :rpk (raw (eng/prefix child (:pk child)))
   :join-table join-table})

(defn- get-db-keys [parent child {:keys [fk]}]
  (let [fk-key (or fk (default-fk-name parent))]
    {:pk (raw (eng/prefix parent (:pk parent)))
     :fk (raw (eng/prefix child fk-key))
     :fk-key fk-key}))

(defn- db-keys-and-foreign-ent [type ent sub-ent opts]
  (case type
    :many-to-many        [(many-to-many-keys ent sub-ent opts) sub-ent]
    (:has-one :has-many) [(get-db-keys ent sub-ent opts) sub-ent]
    :belongs-to          [(get-db-keys sub-ent ent opts) ent]))

(defn create-relation [ent sub-ent type opts]
  (let [[db-keys foreign-ent] (db-keys-and-foreign-ent type ent sub-ent opts)
        fk-override (when-let [fk-key (:fk opts)]
                      {:fk (raw (eng/prefix foreign-ent fk-key))
                       :fk-key fk-key})]
    (merge {:table (:table sub-ent)
            :alias (:alias sub-ent)
            :rel-type type}
           db-keys
           fk-override)))

(defn rel [ent sub-ent type opts]
  (let [var-name (-> sub-ent meta :name)
        var-ns (-> sub-ent meta :ns)]
    (assoc-in ent [:rel (name var-name)]
              (delay
               (let [resolved (ns-resolve var-ns var-name)
                     sub-ent (when resolved (deref sub-ent))]
                 (when-not (map? sub-ent)
                   (throw (Exception. (format "Entity used in relationship does not exist: %s" (name var-name)))))
                 (create-relation ent sub-ent type opts))))))

(defn get-rel [ent sub-ent]
  (let [sub-name (if (map? sub-ent)
                   (:name sub-ent)
                   sub-ent)]
    (force (get-in ent [:rel sub-name]))))

(defmacro has-one
  "Add a has-one relationship for the given entity. It is assumed that the foreign key
  is on the sub-entity with the format table_id: user.id = address.user_id
  Can optionally pass a map with a :fk key to explicitly set the foreign key.

  (has-one users address {:fk :userId})"
  [ent sub-ent & [opts]]
  `(rel ~ent (var ~sub-ent) :has-one ~opts))

(defmacro belongs-to
  "Add a belongs-to relationship for the given entity. It is assumed that the foreign key
  is on the current entity with the format sub-ent-table_id: email.user_id = user.id.
  Can optionally pass a map with a :fk key to explicitly set the foreign key.

  (belongs-to users email {:fk :emailID})"
  [ent sub-ent & [opts]]
  `(rel ~ent (var ~sub-ent) :belongs-to ~opts))

(defmacro has-many
  "Add a has-many relation for the given entity. It is assumed that the foreign key
  is on the sub-entity with the format table_id: user.id = email.user_id
  Can optionally pass a map with a :fk key to explicitly set the foreign key.

  (has-many users email {:fk :emailID})"
  [ent sub-ent & [opts]]
  `(rel ~ent (var ~sub-ent) :has-many ~opts))

(defn many-to-many-fn [ent sub-ent-var join-table opts]
  (let [opts (assoc opts
                    :join-table join-table
                    :lfk (delay (get opts :lfk (default-fk-name ent)))
                    :rfk (delay (get opts :rfk (default-fk-name sub-ent-var))))]
    (rel ent sub-ent-var :many-to-many opts)))

(defmacro many-to-many
  "Add a many-to-many relation for the given entity.  It is assumed that a join
   table is used to implement the relationship and that the foreign keys are in
   the join table."
  [ent sub-ent join-table & [opts]]
  `(many-to-many-fn ~ent (var ~sub-ent) ~join-table ~opts))

(defn entity-fields
  "Set the fields to be retrieved by default in select queries for the
  entity."
  [ent & fields]
  (update-in ent [:fields] utils/vconcat fields))

(defn table
  "Set the name of the table and an optional alias to be used for the entity.
  By default the table is the name of entity's symbol."
  [ent t & [alias]]
  (let [tname (if (or (keyword? t)
                      (string? t))
                (name t)
                (if alias
                  t
                  (throw (Exception. "Generated tables must have aliases."))))
        ent (assoc ent :table tname)]
    (if alias
      (assoc ent :alias (name alias))
      ent)))

(defn pk
  "Set the primary key used for an entity. :id by default."
  [ent pk]
  (assoc ent :pk (keyword pk)))

(defn database
  "Set the database connection to be used for this entity."
  [ent db]
  (assoc ent :db db))

(defn transform
  "Add a function to be applied to results coming from the database"
  [ent func]
  (update-in ent [:transforms] conj func))

(defn prepare
  "Add a function to be applied to records/values going into the database"
  [ent func]
  (update-in ent [:prepares] conj func))

(defmacro defentity
  "Define an entity representing a table in the database, applying any modifications in
  the body."
  [ent & body]
  `(let [e# (-> (create-entity ~(name ent))
                ~@body)]
     (def ~ent e#)))

;;*****************************************************
;; With
;;*****************************************************

(defn- force-prefix [ent fields]
  (->> fields
       (map (fn [field]
              (if (vector? field)
                [(utils/generated (eng/prefix ent (first field))) (second field)]
                (eng/prefix ent field))))
       vec))

(defn- merge-query [sub-query query]
  (reduce (fn [query' k]
            (update-in query' [k] into (get sub-query k)))
          query
          [:aliases :fields :group :joins :order :params :post-queries :where]))

(defn- make-sub-query [sub-ent body-fn]
  (let [sub-query (select* sub-ent)]
    (bind-query sub-query
                (-> sub-query
                    (body-fn)
                    (update-in [:fields] #(force-prefix sub-ent %))
                    (update-in [:order] #(force-prefix sub-ent %))
                    (update-in [:group] #(force-prefix sub-ent %))))))

(defn assoc-db-to-entity [query ent]
  (if-let [db (or (:db ent) (:db query) db/*current-db*)]
    (database ent db)
    ent))

(defn- with-one-to-many [rel query ent body-fn]
  (let [fk-key (:fk-key rel)
        pk (get-in query [:ent :pk])
        table (keyword (eng/table-alias ent))
        ent (assoc-db-to-entity query ent)]
    (post-query query
                (partial map
                         #(assoc % table
                                 (select ent
                                         (body-fn)
                                         (where {fk-key (get % pk)})))))))

(defn- make-key-unique [->key m k n]
  (let [unique-key (if (= n 1) k (keyword (->key (str (name k) "_" n))))]
    (if (contains? m unique-key)
      (recur ->key m k (inc n))
      unique-key)))

(defn- merge-with-unique-keys [->key m1 m2]
  (reduce (fn [m [k v]] (assoc m (make-key-unique ->key m k 1) v)) m1 m2))

(defn- get-key-naming-strategy [query]
  (get-in (or (:options query) @conf/options) [:naming :keys]))

(defn- get-join-keys [rel ent sub-ent]
  (case (:rel-type rel)
    :has-one    [(:pk ent) (:fk-key rel)]
    :belongs-to [(:fk-key rel) (:pk sub-ent)]))

(defn- with-one-to-one-later [rel query sub-ent body-fn]
  (let [sub-ent (assoc-db-to-entity query sub-ent)
        [ent-key sub-ent-key] (get-join-keys rel (:ent query) sub-ent)]
    (post-query query
                (partial map
                         (fn [ent]
                           (merge-with-unique-keys (get-key-naming-strategy query)
                                                   ent
                                                   (first
                                                    (select sub-ent
                                                            (body-fn)
                                                            (where {sub-ent-key (get ent ent-key)})))))))))

(defn- with-one-to-one-now [rel query sub-ent body-fn]
  (let [table (if (:alias rel) [(:table sub-ent) (:alias sub-ent)] (:table sub-ent))
        [ent-key sub-ent-key] (get-join-keys rel (:ent query) sub-ent)]
    (bind-query query
                (merge-query
                 (make-sub-query sub-ent body-fn)
                 (join query
                       table
                       (= (raw (eng/prefix sub-ent sub-ent-key))
                          (raw (eng/prefix (:ent query) ent-key))))))))

(defn- with-many-to-many [{:keys [lfk rfk rpk join-table]} query ent body-fn]
  (let [pk (get-in query [:ent :pk])
        table (keyword (eng/table-alias ent))
        ent (assoc-db-to-entity query ent)]
    (post-query query (partial map
                               #(assoc % table
                                       (select ent
                                               (join :inner join-table (= @rfk rpk))
                                               (body-fn)
                                               (where {@lfk (get % pk)})))))))

(defn with* [query sub-ent body-fn]
  (let [{:keys [rel-type] :as rel} (get-rel (:ent query) sub-ent)
        transforms (seq (:transforms sub-ent))]
    (cond
      (and (#{:belongs-to :has-one} rel-type)
           (not transforms))     (with-one-to-one-now rel query sub-ent body-fn)
           (#{:belongs-to :has-one} rel-type) (with-one-to-one-later rel query sub-ent body-fn)
           (= :has-many rel-type)     (with-one-to-many rel query sub-ent body-fn)
           (= :many-to-many rel-type) (with-many-to-many rel query sub-ent body-fn)
           :else (throw (Exception. (str "No relationship defined for table: "
                                         (:table sub-ent)))))))

(defmacro with
  "Add a related entity to the given select query. If the entity has a relationship
  type of :belongs-to or :has-one, the requested fields will be returned directly in
  the result map. If the entity is a :has-many, a second query will be executed lazily
  and a key of the entity name will be assoc'd with a vector of the results.

  (defentity email (entity-fields :email))
  (defentity user (has-many email))
  (select user
    (with email) => [{:name \"chris\" :email [{email: \"c@c.com\"}]} ...

  With can also take a body that will further refine the relation:
  (select user
     (with address
        (with state)
        (fields :address.city :state.state)
        (where {:address.zip x})))"
  [query ent & body]
  `(with* ~query ~ent (fn [q#]
                        (-> q#
                            ~@body))))

(defn- ensure-fields
  "ensure that fields in fs are included in the query's result set"
  [query fs]
  (let [[first-cur] (:fields query)]
    (if (= first-cur ::*)
      query
      (update-in query [:fields] utils/vconcat fs))))

(defn- ensure-valid-subquery [q]
  (if (->> (select-keys q [:order
                           :group
                           :limit
                           :offset
                           :having
                           :modifiers])
           vals
           (some not-empty))
    (throw (Exception. (str "`with-batch` supports only `where` and `fields` "
                            "options, no sorting, grouping, limits, offsets, "
                            "modifiers")))
    q))

(defn- with-later-batch [rel query ent body-fn]
  (let [fk-key (:fk-key rel)
        pk (get-in query [:ent :pk])
        table (keyword (eng/table-alias ent))]
    (post-query query
                (fn [rows]
                  (let [fks (map #(get % pk) rows)
                        child-rows (select ent
                                           (body-fn)
                                           (where {fk-key [in fks]})
                                           (ensure-fields [fk-key])
                                           (ensure-valid-subquery))
                        child-rows-by-pk (group-by fk-key child-rows)]
                    (map #(assoc %
                                 table (get child-rows-by-pk (get % pk)))
                         rows))))))

(defn with-batch* [query sub-ent body-fn]
  (let [rel (get-rel (:ent query) sub-ent)]
    (case (:rel-type rel)
      (:has-one :belongs-to :many-to-many) (with* query sub-ent body-fn)
      :has-many (with-later-batch rel query sub-ent body-fn)
      (throw (Exception. (str "No relationship defined for table: "
                              (:table sub-ent)))))))

(defmacro with-batch
  "Add a related entity. This behaves like `with`, except that, for has-many
   relationships, it runs a single query to get relations of all fetched rows.
   This is faster than regular `with` but it doesn't support many of the
   additional options (order, limit, offset, group, having)"
  [query ent & body]
  `(with-batch* ~query ~ent (fn [q#]
                              (-> q#
                                  ~@body))))
