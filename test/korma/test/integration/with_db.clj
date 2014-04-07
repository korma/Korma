(ns korma.test.integration.with-db
  (:require [clojure.string :as string]
            [clojure.java.jdbc.deprecated :as jdbc]
            [criterium.core :as cr]
            [korma.core :as kcore]
            [korma.db :as kdb])
  (:use clojure.test
        korma.test.integration.helpers))

(defmacro with-db-broken [db & body]
  `(jdbc/with-connection (kdb/get-connection ~db)
     ~@body))

(def ^:dynamic *data*)

(deftest fails-with-old-style-with-db
  (testing "with-db without setting @_default in with-db binding"
    (kdb/default-connection nil)
    (with-db-broken (mem-db) (populate 2))
    (is (thrown? Exception (dorun (with-db-broken (mem-db)
                                    (with-naming dash-naming-strategy
                                      (kcore/select user (kcore/with address)))))))))

(deftest with-db-success
  (testing "with-db with setting @_default in with-db binding"
    (is (> (count (:address (first (kdb/with-db (mem-db)
                                     (with-naming dash-naming-strategy
                                       (kcore/select user (kcore/with address)))))))) 1)))
