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
              :where {}
              :order []
              :group []})))

(defn update-query [ent]
  (let [q (empty-query ent)]
    (merge q {:type :update
              :fields {}
              :where {}})))

(defn delete-query [ent]
  (let [q (empty-query ent)]
    (merge q {:type :delete
              :where {}})))

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

(defn where [query vs]
  (update-in query [:where] merge vs))

(defn order [query k & [dir]]
  (update-in query [:order] conj [k (or dir :desc)]))

(defn values [query vs]
  (update-in query [:values] concat (if (map? vs)
                                      [vs]
                                      vs)))

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
  {:table table})

(defn has-one [ent sub-ent]
  (update-in ent [:has-one] conj sub-ent))

(defmacro defentity [ent & body]
  `(let [e# (-> (create-entity ~(name ent))
              ~@body)]
     (def ~ent e#)))
