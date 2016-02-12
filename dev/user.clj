(ns user
  (:require [reek.core :as reek]))


(comment (def conn
           (reek/connect "127.0.0.1" 8087))

(reek/store conn "test-bucket" "test-key" "foo" :string "application/foo" {})
(reek/store conn "test-bucket" "test-key2" "foo" :string "application/foo" {:foo "bar"})
(reek/fetch conn  "test-bucket" "test-key2" :string)

(reek/delete conn "test-bucket" "test-key2")

(reek/query-eq conn "test-bucket" :foo "baz")
(reek/query-range conn "test-bucket" :foo "bar" "baz")

(reek/shutdown conn)
)
