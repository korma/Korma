(ns korma.core
  (:require [korma.internal.sql :as isql]
            [korma.db :as db]))

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

(defn select-query [ent]
  (let [q (empty-query ent)]
    (merge q {:type :select
              :fields []
              :where []
              :order []
              :group []})))

(defn update-query [ent]
  (let [q (empty-query ent)]
    (merge q {:type :update
              :fields {}
              :where []})))

(defn delete-query [ent]
  (let [q (empty-query ent)]
    (merge q {:type :delete
              :where []})))

(defn insert-query [ent]
  (let [q (empty-query ent)]
    (merge q {:type :insert
              :values []})))

;;*****************************************************
;; Query macros
;;*****************************************************

(defmacro select [ent & body]
  `(let [query# (-> (select-query ~ent)
                 ~@body)]
     (exec query#)))

(defmacro update [ent & body]
  `(let [query# (-> (update-query ~ent)
                  ~@body)]
     (exec query#)))

(defmacro delete [ent & body]
  `(let [query# (-> (delete-query ~ent)
                  ~@body)]
     (exec query#)))

(defmacro insert [ent & body]
  `(let [query# (-> (insert-query ~ent)
                  ~@body)]
     (exec query#)))

;;*****************************************************
;; Query parts
;;*****************************************************

(defn fields [query & vs]
  (if (= (:type query) :update)
    (update-in query [:fields] merge (first vs))
    (update-in query [:fields] concat vs)))

(defn where* [query vs]
  (update-in query [:where] conj vs))

(defmacro where [query form]
  `(where* ~query ~(isql/parse-where `~form)))

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

(defn with [query ent]
  (let [flds (:fields ent)
        query (if flds
                (apply fields query flds)
                query)
        rel (get-in query [:ent :rel (:table ent)])]
    (join query (:table ent) (:pk rel) (:fk rel))))

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
  (first (isql/->sql query)))

(defn exec [query]
  (let [[sql] (isql/->sql query)]
    (cond
      (:sql query) sql
      (= *exec-mode* :sql) sql
      (= *exec-mode* :dry-run) (do
                                 (println "dry run SQL ::" sql)
                                 [])
      :else (do
              (db/do-query (-> query :ent :db) sql)))))

;;*****************************************************
;; Entities
;;*****************************************************

(defn create-entity [table]
  {:table table
   :fields []
   :rel {}}) 

(defn prefix [ent field]
  (let [table (if (string? ent)
                ent
                (:table ent))]
    (str table "." (name field))))

(defn create-relation [ent sub-ent type opts]
  (let [[pk fk] (condp = type
                  :has-one [(prefix ent :id) (prefix sub-ent (str (:table ent) "_id"))]
                  :belongs-to [(prefix ent :id) (prefix sub-ent (str (:table ent) "_id"))]
                  :has-many [(prefix ent :id) (prefix sub-ent (str (:table ent) "_id"))])]
    (merge {:table (:table sub-ent)
            :rel-type type
            :pk pk
            :fk fk}
           opts)))

(defn rel [ent sub-ent type opts]
  (assoc-in ent [:rel (:table sub-ent)] (create-relation ent sub-ent type opts)))

(defn has-one [ent sub-ent & [opts]]
  (rel ent sub-ent :has-one opts))

(defn has-many [ent sub-ent & [opts]]
  (rel ent sub-ent :has-many opts))

(defn table-fields [ent & fields]
  (update-in ent [:fields] concat (map #(prefix ent %) fields)))

(defn table [ent t]
  (assoc ent :table (name t)))

(defmacro defentity [ent & body]
  `(let [e# (-> (create-entity ~(name ent))
              ~@body)]
     (def ~ent e#)))
