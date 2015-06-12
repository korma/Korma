(ns korma.test.integration.one-to-many
  (:refer-clojure :exclude [update])
  (:require [clojure.string :as string]
            [criterium.core :as cr])
  (:use clojure.test
        korma.core
        korma.db
        korma.test.integration.helpers))

(defn mem-db []
  (create-db (h2 {:db "mem:one_to_many_test"})))

(def ^:dynamic *data*)

(use-fixtures :each
  (fn [t]
    (with-db (mem-db)
      (transaction
       (binding [*data* (populate 100)]
         (t))
       (rollback)))))

(deftest test-one-to-many
  (is (= (sort-by :id (:user *data*))
         (select user
                 (with address)
                 (order :id))))
  (doseq [u (:user *data*)]
    (is
     (= [u]
        (select user
                (where {:id (:id u)})
                (with address)))))
  (doseq [u (:user *data*)]
    (is
     (= [(update-in u [:address]
                    (fn [addrs]
                      (map #(select-keys % [:street :city]) addrs)))]
        (select user
                (where {:id (:id u)})
                (with address
                      (fields :street :city)))))))

(deftest test-one-to-many-batch
  (let [user-ids (map :id (:user *data*))]
    (is
     (=  (select user
                 (where {:id [in user-ids]})
                 (with address
                       (with state)))
         (select user
                 (where {:id [in user-ids]})
                 (with-batch address
                   (with-batch state))))
     "`with-batch` should return the same data as `with`")
    (is
     (=  (select user
                 (where {:id [in user-ids]})
                 (with address
                       ;; with-batch will add the foreign key
                       (fields :user_id :street :city)
                       (with state)))
         (select user
                 (where {:id [in user-ids]})
                 (with-batch address
                   (fields :street :city)
                   (with-batch state))))
     "`with-batch` should return the same data as `with` when using explicit projection")))

(defn- getenv [s]
  (or (System/getenv s)
      (System/getProperty s)))

(when (= (getenv "BENCH") "true")
  (deftest bench-one-to-many-batch
    (let [user-ids (map :id (:user *data*))]
      (println "benchmarking with plain `with`")
      (cr/quick-bench
       (->>
        (select user
                (where {:id [in user-ids]})
                (with address
                      (with state)))
        (map
         #(update-in % [:address] doall))
        doall))
      (println "benchmarking with `with-batch`")
      (cr/quick-bench
       (select user
               (where {:id [in user-ids]})
               (with-batch address
                 (with-batch state)))))))

(deftest test-one-to-many-batch-limitations
  (doseq [banned [#(order % :street)
                  #(group % :street)
                  #(limit % 1)
                  #(offset % 1)]]
    (is (thrown? Exception
                 (select user
                         (with-batch address
                           banned))))))
