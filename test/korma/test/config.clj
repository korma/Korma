(ns korma.test.config
  (:use [korma.config]
        [clojure.test]))

(deftest merge-nil-with-default-options
  (let [original options]
    (merge-defaults nil)
    (is (= original options))))
