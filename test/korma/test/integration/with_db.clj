(ns korma.test.integration.with-db
  (:require [clojure.string :as string]
            [korma.core :as kcore]
            [korma.db :as kdb])
  (:import [java.util.concurrent Executors ThreadPoolExecutor CountDownLatch])
  (:use clojure.test
        korma.test.integration.helpers))

(defn mem-db []
  (kdb/create-db (kdb/h2 {:db "mem:with_db_test"})))

(defn mem-db-2 []
  (kdb/create-db (kdb/h2 {:db "mem:with_db_test_2"})))

(deftest with-db-success
  (testing "with-db with setting *current-db* in with-db binding"
    (kdb/with-db (mem-db) (populate 2))
    (is (> (count (:address (first (kdb/with-db (mem-db)
                                     (kcore/select user (kcore/with address))))))) 1)))

(defn delete-some-rows-from-db []
  (kcore/delete address))

(deftest thread-safe-with-db
  (testing "kdb/with-db using binding"
    (let [db1 (mem-db)
          db2 (mem-db-2)]
      ;; let's create some records in the first db
      (kdb/with-db db1 (populate 2))

      ;; let's create some records in the second db
      (kdb/with-db db2 (populate 2))

      (kdb/default-connection db1)

      ;; we will create 2 threads and execute them at the same time
      ;; using a CountDownLatch to synchronize their execution
      (let [latch (CountDownLatch. 2)
            t1 (future (kdb/with-db db2
                      (.countDown latch)
                      (.await latch)
                      (delete-some-rows-from-db)))
            t2 (future (kdb/with-db db1
                      (.countDown latch)
                      (.await latch)
                      (delete-some-rows-from-db)))]
        @t1
        @t2
        (.await latch))

      (kdb/default-connection nil)

      (let [addresses-mem-db-1 (kdb/with-db db1
                                 (kcore/select address))
            addresses-mem-db-2 (kdb/with-db db2
                                 (kcore/select address))]
        (is (= (count addresses-mem-db-1) 0))
        (is (= (count addresses-mem-db-2) 0))))))
