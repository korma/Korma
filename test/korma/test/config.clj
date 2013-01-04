(ns korma.test.config
  (:require [clojure.test :refer [deftest is]]
            [korma.config :refer [merge-defaults options]]))

(deftest can-merge-options-into-the-defaults
  (let [original @options]
    (merge-defaults {:a 1})
    (is (= (assoc original :a 1) @options))))
