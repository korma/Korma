(ns korma.test.integration.with-db
  (:refer-clojure :exclude [update])
  (:use clojure.test
        korma.db
        korma.core
        korma.test.integration.helpers)
  (:import [java.util.concurrent CountDownLatch]))

(defn mem-db []
  (create-db (h2 {:db "mem:with_db_test"})))

(defn mem-db-2 []
  (create-db (h2 {:db "mem:with_db_test_2"})))

(deftest with-db-success
  (testing "with-db with setting *current-db* in with-db binding"
    (with-db (mem-db) (populate 2))
    (is (> (count (:address (first (with-db (mem-db)
                                     (select user (with address))))))) 1)))

(defn delete-some-rows-from-db []
  (delete address))

(deftest thread-safe-with-db
  (testing "with-db using binding"
    (let [db1 (mem-db)
          db2 (mem-db-2)]
      ;; let's create some records in the first db
      (with-db db1 (populate 2))

      ;; let's create some records in the second db
      (with-db db2 (populate 2))

      (default-connection db1)

      ;; we will create 2 threads and execute them at the same time
      ;; using a CountDownLatch to synchronize their execution
      (let [latch (CountDownLatch. 2)
            t1 (future (with-db db2
                      (.countDown latch)
                      (.await latch)
                      (delete-some-rows-from-db)))
            t2 (future (with-db db1
                      (.countDown latch)
                      (.await latch)
                      (delete-some-rows-from-db)))]
        @t1
        @t2
        (.await latch))

      (default-connection nil)

      (let [addresses-mem-db-1 (with-db db1
                                 (select address))
            addresses-mem-db-2 (with-db db2
                                 (select address))]
        (is (= (count addresses-mem-db-1) 0))
        (is (= (count addresses-mem-db-2) 0))))))
