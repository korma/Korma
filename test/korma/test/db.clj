(ns korma.test.db
  (:use [korma.db]
        [clojure.test]))

(deftest spec-with-missing-keys-should-return-itself
  (defdb valid {:datasource :from-app-server})
  (is (= {:datasource :from-app-server} (get-connection valid))))
