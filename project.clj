(defproject korma "0.3.0-beta7"
  :description "Tasty SQL for Clojure"
  :url "http://github.com/ibdknox/korma"
  :dependencies [[org.clojure/clojure "[1.2.1],[1.3.0]"]
                 [c3p0/c3p0 "0.9.1.2"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]
                 [org.clojure/java.jdbc "0.1.0"]]
  :codox {:exclude [korma.sql.engine korma.sql.fns korma.sql.utils]}
          :dev-dependencies [[org.clojars.rayne/autodoc "0.8.0-SNAPSHOT"]
                             [postgresql "9.0-801.jdbc4"]])
