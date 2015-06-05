(ns korma.test.integration.global-config
  (:refer-clojure :exclude [update])
  (:require [korma.test.integration.helpers :as helpers]
            [korma.config :as config]
            [clojure.string])
  (:use clojure.test
        korma.db
        korma.core))

(defdb mem-db (h2 {:db "mem:global_config"}))

(deftest global-config-can-be-changed
  (helpers/reset-schema)
  (insert :users (values {:id 1 :name "John" :age 50}))
  (testing "Key naming can be changed"
    (config/set-naming {:keys clojure.string/upper-case})
    (is (= [{:ID 1 :NAME "John" :AGE 50}] (select :users)))
    (config/set-naming {:keys identity})))
