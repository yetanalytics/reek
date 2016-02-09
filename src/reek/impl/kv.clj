(ns reek.impl.kv
  (:import [com.basho.riak.client.api.commands.kv
            FetchValue
            FetchValue$Builder
            FetchValue$Response

            StoreValue
            StoreValue$Builder
            StoreValue$Response

            DeleteValue
            DeleteValue$Builder]
           [com.basho.riak.client.core.query Location RiakObject]))

;; build a fetch value command
(defn fetch-value ^FetchValue
  [^Location location]
  (.build (FetchValue$Builder. location)))

;; get a value from the response
(defn get-response-value
  ([^FetchValue$Response resp]
   (get-response-value resp RiakObject))
  ([^FetchValue$Response resp
    klass]
   (.getValue resp klass)))

;; build a store value command
(defn store-value ^StoreValue
  [^Location location
   ^RiakObject riak-obj]
  (-> (StoreValue$Builder. riak-obj)
      (.withLocation location)
      .build))

(defn delete-value ^DeleteValue
  [^Location location]
  (.build (DeleteValue$Builder. location)))
