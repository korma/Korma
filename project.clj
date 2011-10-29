(defproject korma "0.1.0-SNAPSHOT"
  :description "Delicious SQL for Clojure"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [postgresql "8.4-701.jdbc4"]
                 [c3p0/c3p0 "0.9.1.2"]
                           [log4j "1.2.15" :exclusions [javax.mail/mail
                                                        javax.jms/jms
                                                        com.sun.jdmk/jmxtools
                                                        com.sun.jmx/jmxri]]

                 [org.clojure/java.jdbc "0.0.6"]])
