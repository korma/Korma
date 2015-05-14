(ns korma.test.integration.driver-properties
  (:refer-clojure :exclude [update])
  (:use clojure.test
        korma.db
        korma.core
        [korma.test.integration.helpers :only [reset-schema]]))

(defdb mem-db (h2 {:db "mem:driver_properties"}))
(defdb mem-db-no-literals-pooled (h2 {:db "mem:driver_properties" :ALLOW_LITERALS "NONE"}))
(defdb mem-db-no-literals-without-pool (h2 {:db "mem:driver_properties" :ALLOW_LITERALS "NONE" :make-pool? false}))

(deftest driver-properties-can-be-set-in-db-spec
  (with-db mem-db
    (reset-schema))
  (testing "Using connection pool"
    (is (thrown-with-msg? org.h2.jdbc.JdbcSQLException
                          #"Literals of this kind are not allowed"
                          (with-db mem-db-no-literals-pooled
                            (exec-raw "select * from \"users\" where id = 1")))))
  (testing "Without connection pool"
    (is (thrown-with-msg? org.h2.jdbc.JdbcSQLException
                          #"Literals of this kind are not allowed"
                          (with-db mem-db-no-literals-without-pool
                            (exec-raw "select * from \"users\" where id = 1"))))))
