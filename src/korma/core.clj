(ns korma.core
  (:require [korma.internal.sql :as isql]
            [korma.db :as db])
  (:use [korma.internal.sql :only [bind-query]]))

(def ^{:dynamic true} *exec-mode* false)

;;*****************************************************
;; Query types
;;*****************************************************

(defn empty-query [ent]
  (let [[ent table] (if (string? ent)
                      [{} ent]
                      [ent (:table ent)])]
    {:ent ent
     :table table}))

(defn select* [ent]
  (let [q (empty-query ent)]
    (merge q {:type :select
              :fields []
              :where []
              :order []
              :group []
              :results true})))

(defn update* [ent]
  (let [q (empty-query ent)]
    (merge q {:type :update
              :fields {}
              :where []})))

(defn delete* [ent]
  (let [q (empty-query ent)]
    (merge q {:type :delete
              :where []})))

(defn insert* [ent]
  (let [q (empty-query ent)]
    (merge q {:type :insert
              :values []})))

;;*****************************************************
;; Query macros
;;*****************************************************

(defmacro select [ent & body]
  `(let [query# (-> (select* ~ent)
                 ~@body)]
     (exec query#)))

(defmacro update [ent & body]
  `(let [query# (-> (update* ~ent)
                  ~@body)]
     (exec query#)))

(defmacro delete [ent & body]
  `(let [query# (-> (delete* ~ent)
                  ~@body)]
     (exec query#)))

(defmacro insert [ent & body]
  `(let [query# (-> (insert* ~ent)
                  ~@body)]
     (exec query#)))

;;*****************************************************
;; Query parts
;;*****************************************************

(defn fields [query & vs]
  (if (= (:type query) :update)
    (update-in query [:fields] merge (first vs))
    (update-in query [:fields] concat (map #(isql/prefix (:table query) %) vs))))

(defn where* [query vs]
  (update-in query [:where] conj vs))

(defmacro where [query form]
  `(let [q# ~query]
     (where* q# 
             (bind-query q#
               ~(isql/parse-where `~form)))))

(defn order [query k & [dir]]
  (update-in query [:order] conj [k (or dir :desc)]))

(defn values [query vs]
  (update-in query [:values] concat (if (map? vs)
                                      [vs]
                                      vs)))

(defn join 
  ([query table field1 field2]
   (join query :left table field1 field2))
  ([query type table field1 field2]
   (update-in query [:joins] conj [type table field1 field2])))

(defn post-query [query post]
  (update-in query [:post-queries] conj post))


(defn limit [query v]
  (assoc query :limit v))

(defn offset [query v]
  (assoc query :offset v))

;;*****************************************************
;; Query exec
;;*****************************************************

(defmacro sql-only [& body]
  `(binding [*exec-mode* :sql]
     ~@body))

(defmacro dry-run [& body]
  `(binding [*exec-mode* :dry-run]
     ~@body))

(defn as-sql [query]
  (bind-query query (first (isql/->sql query))))

(defn apply-posts [query results]
  (if-let [posts (:post-queries query)]
    (let [post-fn (apply comp posts)]
      (post-fn results))
    results))

(defn exec [query]
  (let [[sql] (bind-query query (isql/->sql query))]
    (cond
      (:sql query) sql
      (= *exec-mode* :sql) sql
      (= *exec-mode* :dry-run) (do
                                 (println "dry run SQL ::" sql)
                                 (apply-posts query [{:id 1}]))
      :else (let [results (db/do-query query sql)]
              (apply-posts query results)))))

;;*****************************************************
;; Entities
;;*****************************************************

(defn create-entity [table]
  {:table table
   :pk :id
   :fields []
   :rel {}}) 

(defn create-relation [ent sub-ent type opts]
  (let [[pk fk] (condp = type
                  :has-one [(isql/prefix ent (:pk ent)) (isql/prefix sub-ent (str (:table ent) "_id"))]
                  :belongs-to [(isql/prefix ent (:pk ent)) (isql/prefix sub-ent (str (:table ent) "_id"))]
                  :has-many [(isql/prefix ent (:pk ent)) (isql/prefix sub-ent (str (:table ent) "_id"))])]
    (merge {:table (:table sub-ent)
            :rel-type type
            :pk pk
            :fk fk}
           opts)))

(defn rel [ent sub-ent type opts]
  (assoc-in ent [:rel (:table sub-ent)] (create-relation ent sub-ent type opts)))

(defn has-one [ent sub-ent & [opts]]
  (rel ent sub-ent :has-one opts))

(defn belongs-to [ent sub-ent & [opts]]
  (rel ent sub-ent :belongs-to opts))

(defn has-many [ent sub-ent & [opts]]
  (rel ent sub-ent :has-many opts))

(defn entity-fields [ent & fields]
  (update-in ent [:fields] concat (map #(isql/prefix ent %) fields)))

(defn table [ent t]
  (assoc ent :table (name t)))

(defn pk [ent pk]
  (assoc ent :pk (name pk)))

(defmacro defentity [ent & body]
  `(let [e# (-> (create-entity ~(name ent))
              ~@body)]
     (def ~ent e#)))

;;*****************************************************
;; With
;;*****************************************************

(defn- with-later [rel query ent]
  (let [fk (:fk rel)
        pk (get-in query [:ent :pk])
        table (keyword (:table ent))]
    (post-query query 
                (partial map 
                         #(assoc % table
                                 (select ent
                                         (where (str fk " = " (get % pk)))))))))

(defn- with-now [rel query ent]
  (let [flds (:fields ent)
        query (if flds
                (apply fields query flds)
                query)]
    (join query (:table ent) (:pk rel) (:fk rel))))

(defn with [query ent]
  (let [rel (get-in query [:ent :rel (:table ent)])]
    (cond
      (not rel) (throw (Exception. (str "No relationship defined for table: " (:table ent))))
      (#{:has-one :belongs-to} (:rel-type rel)) (with-now rel query ent)
      :else (with-later rel query ent))))

