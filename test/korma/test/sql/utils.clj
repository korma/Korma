(ns korma.test.sql.utils
  (:use [clojure.test :only [deftest is testing]]
        [korma.sql.utils :only [left-assoc]]))

(deftest test-left-assoc
  (testing "left-assoc with empty list"
    (is (= "" (left-assoc []))))
  (testing "left-assoc with one item"
    (is (= "1" (left-assoc (list 1)))))
  (testing "left-assoc with multiple items"
    (is (= "(((1)2)3)4" (left-assoc (list 1 2 3 4))))))

