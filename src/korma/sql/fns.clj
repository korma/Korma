(ns korma.sql.fns
  (:require [korma.db :as db]
            [korma.mysql :as mysql]
            [korma.sql.engine :as eng])
  (:use [korma.sql.engine :only [infix group-with sql-func trinary wrapper]]))

;;*****************************************************
;; Predicates
;;*****************************************************


(defn pred-and [& args]
  (apply eng/pred-and args))

(defn pred-or [& args] (group-with " OR " args))
(defn pred-not [v] (wrapper "NOT" v))

(defn pred-in [k v]     (infix k "IN" v))
(defn pred-not-in [k v] (infix k "NOT IN" v))
(defn pred-> [k v]      (infix k ">" v))
(defn pred-< [k v]      (infix k "<" v))
(defn pred->= [k v]     (infix k ">=" v))
(defn pred-<= [k v]     (infix k "<=" v))
(defn pred-like [k v]   (infix k "LIKE" v))

(defn pred-between [k [v1 v2]]
  (trinary k "BETWEEN" v1 "AND" v2))

(def pred-= eng/pred-=)
(defn pred-not= [k v] (cond
                        (and k v) (infix k "<>" v)
                        k         (infix k "IS NOT" v)
                        v         (infix v "IS NOT" k)))

;;*****************************************************
;; Aggregates
;;*****************************************************

(defn- subprotocol [query]
  (let [default (get-in @db/_default [:options :subprotocol])]
     (or (get-in query [:db :options :subprotocol]) default)))

(defn agg-count [query v]
  (if (= "mysql" (subprotocol query))
    (mysql/count query v)
    (sql-func "COUNT" v)))

(defn agg-sum [_query_ v]   (sql-func "SUM" v))
(defn agg-avg [_query_ v]   (sql-func "AVG" v))
(defn agg-stdev [_query_ v] (sql-func "STDEV" v))
(defn agg-min [_query_ v]   (sql-func "MIN" v))
(defn agg-max [_query_ v]   (sql-func "MAX" v))
(defn agg-first [_query_ v] (sql-func "FIRST" v))
(defn agg-last [_query_ v]  (sql-func "LAST" v))
