(ns reek.core
  (:require [reek.impl.client :as client]
            [clojure.set :as cset]))

(set! *warn-on-reflection* true)

(defn connect
  "instantiate a new Riak connection. Returns a connected ReekClient"
  [host port]
  (client/connect
   (client/map->ReekClient {:host host
                            :port port})))

(defn shutdown [client]
  (client/shutdown client))

(defn fetch
  [^reek.impl.client.ReekClient client
   ^String bucket-name
   ^String key-name
   ^clojure.lang.Keyword vtype]
  (client/fetch client
                bucket-name
                key-name
                vtype))

(defn store
  ([^reek.impl.client.ReekClient client
    ^String bucket-name
    ^String key-name
    value
    ^clojure.lang.Keyword vtype
    ^String content-type]
   (client/store client
                 bucket-name
                 key-name
                 value
                 vtype
                 content-type
                 {}))
  ([^reek.impl.client.ReekClient client
    ^String bucket-name
    ^String key-name
    value
    ^clojure.lang.Keyword vtype
    ^String content-type
    index-map]
   (client/store client
                 bucket-name
                 key-name
                 value
                 vtype
                 content-type
                 index-map)))

(defn delete
  [^reek.impl.client.ReekClient client
   ^String bucket-name
   ^String key-name]
  (client/delete client
                 bucket-name
                 key-name))

(defn query-eq
  [^reek.impl.client.ReekClient client
   ^String bucket-name
   ^clojure.lang.Keyword index-key
   ^String index-val]
  (client/query-eq client
                   bucket-name
                   index-key
                   index-val))

(defn query-and [conn b multi-idx-map]
  (apply cset/union
         (for [[k v] multi-idx-map]
           (query-eq conn b k v))))

(defn query-range
  [^reek.impl.client.ReekClient client
   ^String bucket-name
   ^clojure.lang.Keyword index-key
   ^String min-val
   ^String max-val]
  (client/query-range client
                      bucket-name
                      index-key
                      min-val
                      max-val))
