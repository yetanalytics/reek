(defproject com.yetanalytics/reek "0.1.0-SNAPSHOT"
  :description "Simple Riak k/v library"
  :url "https://github.com/yetanalytics/reek"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.yetanalytics/kria "0.2.0-SNAPSHOT"]]
  :profiles
  {:dev {:source-paths ["dev"]
         :dependencies [[org.clojure/data.fressian "0.2.1"]
                        [com.taoensso/nippy "2.10.0"]]}})
