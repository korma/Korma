(defproject korma "0.3.0-beta13"
  :description "Tasty SQL for Clojure"
  :url "http://github.com/ibdknox/korma"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [c3p0/c3p0 "0.9.1.2"]
                 [org.clojure/java.jdbc "0.2.2"]]
  :codox {:exclude [korma.sql.engine
                    korma.sql.fns
                    korma.sql.utils]}
  :dev-dependencies [[com.h2database/h2 "1.3.164"]
                     [postgresql "9.0-801.jdbc4"]])
