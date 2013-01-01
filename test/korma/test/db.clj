(ns korma.test.db
  (:use clojure.test
        korma.db))

(deftest spec-with-missing-keys-returns-itself
  (defdb valid {:datasource :from-app-server})
  (is (= {:datasource :from-app-server} (get-connection valid))))
