(ns reek.impl.bucket
  (:import [com.basho.riak.client.core.query Namespace]))

(defn bucket-named ^Namespace [bucket-name]
  (Namespace. bucket-name))
