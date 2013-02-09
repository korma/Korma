(ns ^{:no-doc true}
  korma.sql.utils
  (:require [clojure.string :as string]))

;;*****************************************************
;; map-types
;;*****************************************************

(defn generated [s]
  {::generated s})

(defn sub-query [s]
  {::sub s})

(defn pred [p args]
  {::pred p ::args args})

(defn func [f args]
  {::func f ::args args})

(defn func? [m]
  (::func m))

(defn pred? [m]
  (::pred m))

(defn args? [m]
  (::args m))

(defn sub-query? [m]
  (::sub m))

(defn generated? [m]
  (::generated m))

(defn special-map? [m]
  (boolean (some #(% m) [func? pred? sub-query? generated?])))

;;*****************************************************
;; str-utils
;;*****************************************************

(defn comma-separated [vs]
  (string/join ", " vs))

(defn wrap [v]
  (str "(" v ")"))

(defn wrap-all [vs]
  (wrap (comma-separated vs)))

(defn left-assoc [vs]
  (loop [ret "" [v & vs] vs]
    (cond
      (nil? v) ret
      (nil? vs) (str ret v)
      :else (recur (wrap (str ret v)) vs))))

;;*****************************************************
;; collection-utils
;;*****************************************************

(defn vconcat [v1 v2]
  (vec (concat v1 v2)))
