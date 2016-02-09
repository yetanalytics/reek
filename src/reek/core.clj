(ns reek.core
  (:require [reek.impl.client :as client]
            [reek.impl.kv :as kv]
            [reek.impl.query :as query]
            [reek.impl.bucket :as bucket]
            [reek.impl.object :as object]
            [reek.impl.indexes :as indexes]))


(defn connect
  "instantiate a new Riak connection. Returns a connected ReekClient"
  [uri port]
  (client/connect
   (client/map->ReekClient {:uri uri
                            :port port})))

(defn store
  ([^reek.impl.client.ReekClient client
    ^String bucket-name
    ^String key-name
    value
    ^String content-type]
   (client/execute client
                   (kv/store-value
                    (object/location (bucket/bucket-named bucket-name) key-name)
                    (object/new-riak-object
                     value
                     content-type))))
  ([^reek.impl.client.ReekClient client
    ^String bucket-name
    ^String key-name
    value
    ^String content-type
    index-map]
   (client/execute client
                   (kv/store-value
                    (object/location (bucket/bucket-named bucket-name) key-name)
                    (indexes/apply-indexes (object/new-riak-object
                                            value
                                            content-type) index-map)))))

(defn fetch
  [^reek.impl.client.ReekClient client
   ^String bucket-name
   ^String key-name
   & [?resp-class]]
  (let [resp (client/execute client
                             (kv/fetch-value (object/location
                                              (bucket/bucket-named bucket-name)
                                              key-name)))]
    (if ?resp-class
      (kv/get-response-value
       resp
       ?resp-class)
      (kv/get-response-value resp))))

(defn query
  [^reek.impl.client.ReekClient client
   ^String bucket-name
   ^clojure.lang.Keyword index-key
   ^String index-val]
  (query/response-entries
   (client/execute client
                   (query/bin-index-query (bucket/bucket-named bucket-name)
                                          (name index-key)
                                          index-val))))

(defn delete
  [^reek.impl.client.ReekClient client
   ^String bucket-name
   ^String key-name]
  (client/execute client
                  (kv/delete-value (object/location
                                    (bucket/bucket-named bucket-name)
                                    key-name))))
