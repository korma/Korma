(defproject korma "0.3.0-beta13"
  :description "Tasty SQL for Clojure"
  :url "http://github.com/ibdknox/korma"
  :codox {:exclude [korma.sql.engine
                    korma.sql.fns
                    korma.sql.utils]}
  
  ;;Lein2 - the way of the future
  :profiles {:user {:dependencies [[org.clojure/clojure "1.4.0"]
                                   [c3p0/c3p0 "0.9.1.2"]
                                   [org.clojure/java.jdbc "0.2.2"]]}
             :dev {:dependencies [[com.h2database/h2 "1.3.164"]
                                  [postgresql "9.0-801.jdbc4"]
                                  [slamhound "1.3.1"]]}}
  :aliases {"slamhound" ["run" "-m" "slam.hound"]}

  ;; Lein1 - will be removed at some point
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [c3p0/c3p0 "0.9.1.2"]
                 [org.clojure/java.jdbc "0.2.2"]]
  :dev-dependencies [[com.h2database/h2 "1.3.164"]
                     [postgresql "9.0-801.jdbc4"]])