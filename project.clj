(defproject r4f-data "0.1.0"
  :description "Code to extract and analyse json extracts of the Retrofit for the Future embed database."
  :url "http://github.com/mastodonc/r4f-data"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [cascalog "1.10.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [cheshire "5.0.2"]
                 [clj-time "0.5.0"]]
  :profiles {:dev {:dependencies [[midje "1.5.1"
                                   :exclusions [org.apache.httpcomponents/httpcore
                                                commons-io]]
                                  [org.apache.hadoop/hadoop-core "0.20.2-dev"
                                   :exclusions [org.slf4j/slf4j-api
                                                commons-logging
                                                commons-codec
                                                org.slf4j/slf4j-log4j12
                                                log4j]]]}
             :provided {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]]}}
  :pedantic :warn
  :aot [r4f-data.core]
  :main r4f-data.core
  :uberjar-name "r4f-data.jar"
  :warn-on-reflection true
  :exclusions [org.apache.hadoop/hadoop-core
               org.clojure/clojure
               midje])
