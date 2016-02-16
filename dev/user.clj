(ns user
  (:require [reek.core :as reek]
            [clojure.data.fressian :as fress]
            [taoensso.nippy :as nip]
            [clojure.set :as cset]))

(defn store-fress [conn b k clj-data idx-map]
  (reek/store conn b k (fress/write clj-data) :byte-buffer "application/fressian" idx-map))

(defn fetch-fress [conn b k]
  (fress/read (:value (reek/fetch conn b k :byte-buffer))))

(defn store-nippy [conn b k clj-data idx-map]
  (reek/store conn b k (nip/freeze clj-data) :byte-array "application/fressian" idx-map))

(defn fetch-nippy [conn b k]
  (nip/thaw (:value (reek/fetch conn b k :byte-array))))


(comment

  (def conn
    (reek/connect "127.0.0.1" 8087))

  (reek/store conn "test-bucket" "a-string" "foo" :string "text/plain" {})
  (reek/fetch conn  "test-bucket" "a-string" :string)

  (reek/delete conn "test-bucket" "a-string")

  (reek/store conn "test-bucket"
              "a-string-with-an-index" "quxx"
              :string "text/plain" {:foo "bar"})
  (reek/store conn "test-bucket"
              "another-string-with-an-index" "wizz" :string "text/plain" {:foo "bar"})
  (reek/store conn "test-bucket"
              "a-third-string-with-a-different-index" "wazz" :string "text/plain" {:foo "baz"
                                                                                   :baz "quxx"})

  (reek/query-eq conn "test-bucket" :foo "bar")
  (reek/query-eq conn "test-bucket" :foo "baz")

  (reek/query-range conn "test-bucket" :foo "bar" "baz")

  (reek/query-and conn "test-bucket" {:foo "bar"
                                      :baz "quxx"})

  (reek/delete conn "test-bucket" "a-string-with-an-index")
  (reek/delete conn "test-bucket" "another-string-with-an-index")
  (reek/delete conn "test-bucket" "a-third-string-with-a-different-index")

  (time
   (do
     (store-fress conn "test-bucket" "a-fressian-value" {:some {:fressian ["stuff"]}} {})
     (fetch-fress conn "test-bucket" "a-fressian-value")))

  (reek/delete conn "test-bucket" "a-fressian-value")

  (time
   (do
    (store-nippy conn "test-bucket" "a-nippy-value" {:some {:nippy ["stuff"]}} {})
    (fetch-nippy conn "test-bucket" "a-nippy-value")))

  (reek/delete conn "test-bucket" "a-nippy-value")

  (reek/shutdown conn)
  )
