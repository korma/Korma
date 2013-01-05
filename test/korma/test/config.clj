(ns korma.test.config
  (:use [clojure.test :only [deftest is]]
        [korma.config :only [merge-defaults options]]))

(deftest can-merge-options-into-the-defaults
  (let [original @options]
    (merge-defaults {:a 1})
    (is (= (assoc original :a 1) @options))))
