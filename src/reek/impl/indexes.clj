(ns reek.impl.indexes
  (:import [com.basho.riak.client.core.query RiakObject]
           [com.basho.riak.client.core.query.indexes StringBinIndex]))

(defn apply-indexes
  ^RiakObject
  [^RiakObject riak-obj index-map]
  (reduce
   (fn [^RiakObject ro [k v]]
     (doto ro
       (-> .getIndexes
           (.getIndex
            (StringBinIndex/named (name k)))
           (.add v))))
   riak-obj
   index-map))
