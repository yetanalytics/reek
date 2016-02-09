(ns reek.impl.object
  (:import [com.basho.riak.client.core.query Namespace Location RiakObject]
           [com.basho.riak.client.core.query.indexes StringBinIndex]
           [com.basho.riak.client.core.util BinaryValue]))

(defn location ^Location
  [^Namespace
   bucket
   ^String
   key-name]
  (Location. bucket key-name))

(defn set-data ^RiakObject
  [^RiakObject riak-obj data]
  (doto riak-obj
    (.setValue (BinaryValue/create data))))

(defn set-content-type ^RiakObject
  [^RiakObject riak-obj ^String content-type]
  (doto riak-obj
    (.setContentType content-type)))

(defn new-riak-object ^RiakObject
  [& {:keys [content-type data]}]
  (doto (RiakObject.)
    (cond-> content-type (set-content-type content-type))
    (cond-> data (set-data data))))

(defn get-content-type ^String [^RiakObject riak-obj]
  (.getContentType riak-obj))

(defn get-value [^RiakObject riak-obj]
  (.getValue riak-obj))
