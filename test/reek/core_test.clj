(ns reek.core-test
  (:require [clojure.test :refer :all]
            [reek.core :refer :all]
            [reek.test-helpers :refer :all])
  (:import [reek.impl.client ReekClient]))

(deftest connect-test
  (testing "connect"
    (let [client (connect "127.0.0.1" 8087)]
      (is (= (class client) ReekClient))
      (is (:conn client))
      (.shutdown client))))

(deftest ops-test
  (let [client (connect!)
        bucket-name (rand-bucket-name)
        k (rand-key)
        v (rand-string-value 10)
        indexes {:foo "bar"}]
    (testing "store"
      (is (store client bucket-name k v "text/plain" indexes)))
    (testing "fetch"
      (is (= (fetch client bucket-name k java.lang.String)
             v)))
    (testing "query"
      (is (= (count (query client bucket-name :foo "bar"))
             1)))
    (testing "delete"
      (is (nil? (delete client bucket-name k)))
      (is (nil? (fetch client bucket-name k))))
    (.shutdown client)))
