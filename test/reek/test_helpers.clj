(ns reek.test-helpers
  (:require [reek.core :refer [connect]]))

;; following https://github.com/bluemont/kria/blob/master/test/kria/test_helpers.clj
(defn rand-bucket-name
  []
  (->> (rand-int 100000000)
       (format "B-%08d")))

(defn rand-key
  []
  (->> (rand-int 100000000)
       (format "K-%08d")))

(defn rand-value
  []
  (->> (rand-int 10000)
       (format "V-%04d")))

(defn rand-string-value
  [n]
  (let [s "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ"]
    (-> (repeatedly n #(str (rand-nth s)))
        (clojure.string/join))))

(defn connect! []
  (connect "127.0.0.1" 8087))
