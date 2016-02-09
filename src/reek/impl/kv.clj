(ns reek.impl.kv
  (:import [com.basho.riak.client.api.commands.kv
            FetchValue
            FetchValue$Builder
            FetchValue$Response]
           [com.basho.riak.client.api.commands.kv
            StoreValue
            StoreValue$Builder
            StoreValue$Response]
           [com.basho.riak.client.core.query Location RiakObject]))


(defn fetch-value ^FetchValue
  [^Location location]
  (.build (FetchValue$Builder. location)))

(defn store-value ^StoreValue
  [^Location location
   ^RiakObject riak-obj]
  (-> (StoreValue$Builder. riak-obj)
      (.withLocation location)
      .build))

(defn get-response-value
  ([^FetchValue$Response resp]
   (get-response-value resp RiakObject))
  ([^FetchValue$Response resp
    klass]
   (.getValue resp klass)))
