(defproject org.clojars.making/cloudure "0.1.0-SNAPSHOT" 
  :description "a Hadoop(MapReduce) library for Hadoop" 
  :dependencies [[org.clojure/clojure "1.1.0"] 
                 [org.clojure/clojure-contrib "1.1.0"]
                 [org.apache.mahout.hadoop/hadoop-core "0.20.1"]
                 [commons-cli/commons-cli "1.2"]
                 [commons-codec/commons-codec "1.3"]
                 [commons-el/commons-el "1.0"]
                 [commons-httpclient/commons-httpclient "3.0.1"]
                 [commons-logging/commons-logging "1.0.4"]
                 [commons-logging/commons-logging-api "1.0.4"]
                 [commons-net/commons-net "1.4.1"]
                 ]
  :dev-dependencies [[leiningen/lein-swank "1.1.0"]
                     [lein-clojars "0.5.0-SNAPSHOT"]
                     ]
  )