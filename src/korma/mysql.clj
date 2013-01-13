(ns korma.mysql
  (:require [korma.sql.utils :as utils])
  (:use [korma.sql.engine :only [sql-func]])
  (:refer-clojure :exclude [count]))


(defn count
  "On MySQL, when an argument for COUNT() is a '*',
   it must be a simple '*', instead of 'fieldname.*'."
  [_query_ v]
  (if (= "*" (name v))
    (sql-func "COUNT" (utils/generated "*"))
    (sql-func "COUNT" v)))