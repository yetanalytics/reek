(ns reek.impl.query
  (:import
   [com.basho.riak.client.core.query Namespace]
   [com.basho.riak.client.api.commands.indexes
    BinIndexQuery
    BinIndexQuery$Builder
    BinIndexQuery$Response]
   [com.basho.riak.client.]))

(defn bin-index-query ^BinIndexQuery
  [^Namespace bucket
   ^String index-key
   ^String index-val]
  (.build (BinIndexQuery$Builder. bucket index-key index-val)))

(defn response-entries
  "take a bin index query response, and get the keys+locs for the results,
  if any."
  [^BinIndexQuery$Response resp]
  (for [entry (.getEntries resp)
        :let [loc (.getRiakObjectLocation entry)
              k (.getKey loc)]]
    {:key (str k)
     :location loc}))
