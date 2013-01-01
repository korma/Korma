(ns korma.test.config
  (:use clojure.test
        korma.config))

(deftest can-merge-options-into-the-defaults
  (let [original @options]
    (merge-defaults {:a 1})
    (is (= (assoc original :a 1) @options))))
