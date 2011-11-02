(ns korma.core
  (:require [korma.internal.sql :as isql]
            [clojure.set :as set]
            [korma.db :as db])
  (:use [korma.internal.sql :only [bind-query bind-params]]))

(def ^{:dynamic true} *exec-mode* false)

;;*****************************************************
;; Query types
;;*****************************************************

(defn- empty-query [ent]
  (let [[ent table alias] (if (string? ent)
                      [{} ent nil]
                      [ent (:table ent) (:alias ent)])]
    {:ent ent
     :table table
     :alias alias}))

(defn select* 
  "Create an empty select query. Ent can either be an entity defined by defentity,
  or a string of the table name"
  [ent]
  (let [q (empty-query ent)]
    (merge q {:type :select
              :fields []
              :where []
              :order []
              :aliases #{}
              :group []
              :results true})))

(defn update* 
  "Create an empty update query. Ent can either be an entity defined by defentity,
  or a string of the table name."
  [ent]
  (let [q (empty-query ent)]
    (merge q {:type :update
              :fields {}
              :where []})))

(defn delete* 
  "Create an empty delete query. Ent can either be an entity defined by defentity,
  or a string of the table name"
  [ent]
  (let [q (empty-query ent)]
    (merge q {:type :delete
              :where []})))

(defn insert* 
  "Create an empty insert query. Ent can either be an entity defined by defentity,
  or a string of the table name"
  [ent]
  (let [q (empty-query ent)]
    (merge q {:type :insert
              :values []})))

;;*****************************************************
;; Query macros
;;*****************************************************

(defmacro select 
  "Creates a select query, applies any modifying functions in the body and then
  executes it. `ent` is either a string or an entity created by defentity.
  
  ex: (select user 
        (fields :name :email)
        (where {:id 2}))"
  [ent & body]
  `(let [query# (-> (select* ~ent)
                 ~@body)]
     (exec query#)))

(defmacro update 
  "Creates an update query, applies any modifying functions in the body and then
  executes it. `ent` is either a string or an entity created by defentity.
  
  ex: (update user 
        (set-fields {:name \"chris\"}) 
        (where {:id 4}))"
  [ent & body]
  `(let [query# (-> (update* ~ent)
                  ~@body)]
     (exec query#)))

(defmacro delete 
  "Creates a delete query, applies any modifying functions in the body and then
  executes it. `ent` is either a string or an entity created by defentity.
  
  ex: (delete user 
        (where {:id 7}))"
  [ent & body]
  `(let [query# (-> (delete* ~ent)
                  ~@body)]
     (exec query#)))

(defmacro insert 
  "Creates an insert query, applies any modifying functions in the body and then
  executes it. `ent` is either a string or an entity created by defentity.
  
  ex: (insert user 
        (values [{:name \"chris\"} {:name \"john\"}]))"
  [ent & body]
  `(let [query# (-> (insert* ~ent)
                  ~@body)]
     (exec query#)))

;;*****************************************************
;; Query parts
;;*****************************************************

(defn fields
  "Set the fields to be selected in a query."
  [query & vs] 
  (let [aliases (set (map second (filter coll? vs)))]
    (-> query
    (update-in [:aliases] set/union aliases)
    (update-in [:fields] concat vs))))

(defn set-fields
  "Set the fields and values for an update query."
  [query fields-map]
  (update-in query [:set-fields] merge fields-map))

(defn where*
  "Add a where clause to the query. Clause can be either a map or a string, and
  will be AND'ed to the other clauses."
  [query clause]
  (update-in query [:where] conj clause))

(defmacro where
  "Add a where clause to the query, expressing the clause in clojure expressions
  with keywords used to reference fields.
  e.g. (where query (or (= :hits 1) (> :hits 5)))

  Available predicates: and, or, =, not=, <, >, <=, >=, in, like, not

  Where can also take a map at any point and will create a clause that compares keys
  to values. The value can be a vector with one of the above predicate functions 
  describing how the key is related to the value: (where query {:name [like \"chris\"})"
  [query form]
  `(let [q# ~query]
     (bind-params
       (where* q# 
               (bind-query q#
                           ~(isql/parse-where `~form))))))

(defn order
  "Add an ORDER BY clause to a select query. field should be a keyword of the field name, dir
  is DESC by default.
  
  (order query :created :asc)"
  [query field & [dir]]
  (update-in query [:order] conj [field (or dir :desc)]))

(defn values
  "Add records to an insert clause. values can either be a vector of maps or a single
  map.
  
  (values query [{:name \"john\"} {:name \"ed\"}])"
  [query values]
  (update-in query [:values] concat (if (map? values)
                                      [values]
                                      values)))

(defn join 
  "Add a join clause to a select query, specifying the table name to join and the two fields
  to predicate the join on.
  
  (join query :right :address :address.users_id :users.id)"
  ([query table field1 field2]
   (join query :left table field1 field2))
  ([query type table field1 field2]
   (update-in query [:joins] conj [type table field1 field2])))

(defn post-query
  "Add a function representing a query that should be executed for each result in a select.
  This is done lazily over the result set."
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
  [query field]
  (update-in query [:group] conj field))

(defmacro aggregate
  "Use a SQL aggregator function, aliasing the results, and optionally grouping by
  a field:
  
  (select users 
    (aggregate (count :*) :cnt :status))"
  [query agg alias & [group-by]]
  `(bind-query ~query
               (let [res# (fields ~query [~(isql/parse-aggregate agg) ~alias])]
                 (if ~group-by
                   (group res# ~group-by)
                   res#))))

;;*****************************************************
;; Query exec
;;*****************************************************

(defmacro sql-only
  "Wrap around a set of queries so that instead of executing, each will return a string of the SQL 
  that would be used."
  [& body]
  `(binding [*exec-mode* :sql]
     ~@body))

(defmacro dry-run
  "Wrap around a set of queries to print to the console all SQL that would 
  be run and return dummy values instead of executing them."
  [& body]
  `(binding [*exec-mode* :dry-run]
     ~@body))

(defn as-sql
  "Force a query to return a string of SQL when (exec) is called."
  [query]
  (bind-query query (:sql-str (isql/->sql query))))

(defn- apply-posts
  [query results]
  (if-let [posts (seq (:post-queries query))]
    (let [post-fn (apply comp posts)]
      (post-fn results))
    results))

(defn- apply-transforms
  [query results]
  (if-let [trans (seq (-> query :ent :transforms))]
    (let [trans-fn (apply comp trans)]
      (map trans-fn results))
    results))

(defn- apply-prepares
  [query]
  (if-let [preps (seq (-> query :ent :prepares))]
    (let [preps (apply comp preps)]
      (condp = (:type query)
        :insert (let [values (:values query)]
                  (assoc query :values (map preps values)))
        :update (let [value (:set-fields query)]
                  (assoc query :set-fields (preps value)))
        query))
    query))

(defn exec
  "Execute a query map and return the results."
  [query]
  (let [query (apply-prepares query)
        query (bind-query query (isql/->sql query))
        sql (:sql-str query)
        params (:params query)]
    (cond
      (:sql query) sql
      (= *exec-mode* :sql) sql
      (= *exec-mode* :dry-run) (do
                                 (println "dry run ::" sql "::" (vec params))
                                 (apply-posts query [{:id 1}]))
      :else (let [results (db/do-query query)]
              (apply-transforms query (apply-posts query results))))))

(defn exec-raw
  "Execute a raw SQL string, supplying whether results should be returned. `sql` can either be
  a string or a vector of the sql string and its params. You can also optionally
  provide the connection to execute against as the first parameter.
  
  (exec-raw [\"SELECT * FROM users WHERE age > ?\" [5]] true)"
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

(defn create-entity
  "Create an entity representing a table in a database."
  [table]
  {:table table
   :pk :id
   :transforms '()
   :prepares '()
   :fields []
   :rel {}}) 

(defn create-relation
  "Create a relation map describing how two entities are related."
  [ent sub-ent type opts]
  (let [[pk fk] (condp = type
                  :has-one [(isql/prefix ent (:pk ent)) 
                            (isql/prefix sub-ent (keyword (str (:table ent) "_id")))]
                  :belongs-to [(isql/prefix sub-ent (:pk sub-ent)) 
                               (isql/prefix ent (keyword (str (:table sub-ent) "_id")))]
                  :has-many [(isql/prefix ent (:pk ent)) 
                             (isql/prefix sub-ent (keyword (str (:table ent) "_id")))])]
    (merge {:table (:table sub-ent)
            :alias (:alias sub-ent)
            :rel-type type
            :pk pk
            :fk fk}
           opts)))

(defn- rel
  [ent sub-ent type opts]
  (assoc-in ent [:rel (:table sub-ent)] (create-relation ent sub-ent type opts)))

(defn has-one
  "Add a has-one relationship for the given entity."
  [ent sub-ent & [opts]]
  (rel ent sub-ent :has-one opts))

(defn belongs-to
  "Add a belongs-to relationship for the given entity."
  [ent sub-ent & [opts]]
  (rel ent sub-ent :belongs-to opts))

(defn has-many
  "Add a has-many relation for the given entity. It is assumed that the foreign key
  is on the sub-entity with the format table_id:
  
  user.id = email.user_id"
  [ent sub-ent & [opts]]
  (rel ent sub-ent :has-many opts))

(defn entity-fields
  "Set the fields to be retrieved by default in select queries for the
  entity."
  [ent & fields]
  (update-in ent [:fields] concat (map #(isql/prefix ent %) fields)))

(defn table
  "Set the name of the table and an optional alias to be used for the entity. 
  By default the table is the name of entity's symbol."
  [ent t & [alias]]
  (let [ent (assoc ent :table (name t))]
    (if alias
      (assoc ent :alias (name alias))
      ent)))

(defn pk
  "Set the primary key used for an entity. :id by default."
  [ent pk]
  (assoc ent :pk (keyword pk)))

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

(defn- with-later [rel query ent]
  (let [fk (:fk rel)
        pk (get-in query [:ent :pk])
        table (keyword (isql/table-alias ent))]
    (post-query query 
                (partial map 
                         #(assoc % table
                                 (select ent
                                         (where (str fk " = " (get % pk)))))))))

(defn- with-now [rel query ent]
  (let [flds (:fields ent)
        query (if flds
                (apply fields query flds)
                query)
        table (if (:alias rel)
                [(:table ent) (:alias ent)]
                (:table ent))]
    (join query table (:pk rel) (:fk rel))))

(defn- single-with [query ent]
  (let [rel (get-in query [:ent :rel (:table ent)])]
    (cond
      (not rel) (throw (Exception. (str "No relationship defined for table: " (:table ent))))
      (#{:has-one :belongs-to} (:rel-type rel)) (with-now rel query ent)
      :else (with-later rel query ent))))

(defn with
  "Add a related entity to the given select query. If the entity has a relationship
  type of :belongs-to or :has-one, the requested fields will be returned directly in
  the result map. If the entity is a :has-many, a second query will be executed lazily
  and a key of the entity name will be assoc'd with a vector of the results.
  
  (defentity email (entity-fields :email))
  (defentity user (has-many email))
  (select user
    (with email) => [{:name \"chris\" :email [{email: \"c@c.com\"}]} ..."
  [query & ents]
  (reduce single-with query ents))
