(ns korma.test.integration.delete
  (:use clojure.test
        korma.core
        korma.db
        [korma.test.integration.helpers :only [populate address]]))

(defdb mem-db (h2 {:db "mem:delete"}))

(use-fixtures :once (fn [f]
                      (default-connection mem-db)
                      (populate 10)
                      (f)))

(deftest delete-returns-count-of-deleted-rows
  (testing "Deleting one row"
    (is (= 1 (delete address (where {:id 1})))))
  (testing "Deleting multiple rows"
    (is (= 3 (delete address (where {:id [< 5]})))))
  (testing "No rows are deleted"
    (is (= 0 (delete address (where {:id -1}))))))
