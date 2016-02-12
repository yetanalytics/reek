(ns reek.impl.client
  (:require [kria.client :as client]
            [kria.conversions :as conv]
            [kria.object :as object]
            [kria.index :as index])
  (:import [java.nio.channels
            AsynchronousSocketChannel
            ;; AsynchronousChannelGroup
            ;; CompletionHandler
            ]))


(defprotocol IReekClient
  "Protocol for client ops"
  (connect [this]
    "Connects the client")
  (shutdown [this]
    "Shuts down the client")
  (fetch [this bucket-name key-name vtype]
    "Get a value for a key")
  (store [this bucket-name key-name value vtype content-type index-map]
    "Store a value w/o indexes")
  (delete [this bucket-name key-name]
    "Delete a value for key")
  (query-eq [this bucket-name index-key index-val]
    "Get exact matches for 2i indexes")
  (query-range [this bucket-name index-key min-val max-val]
    "Query a range of 2i indexes"))


(defn conn-cb [p]
  (fn [asc e a]
    (if e
      (throw (Exception. "Connection operation failed"))
      (deliver p asc))))

(defn result-cb [p]
  (fn [asc e a]
    (deliver p (or e a))))


(defn store-converter [vtype]
  (case vtype
    :string conv/byte-string<-utf8-string
    :byte-array conv/byte-string<-byte-array
    :byte-buffer conv/byte-string<-byte-buffer
    :byte-string identity
    (throw (Exception. "Invalid vtype"))))

(defn fetch-converter [vtype]
  (case vtype
    :string conv/utf8-string<-byte-string
    :byte-array conv/byte-array<-byte-string
    :byte-buffer conv/byte-buffer<-byte-string
    :byte-string identity
    (throw (Exception. "Invalid vtype"))))




(defn trim-tag
  "trim the _bin tag, as we only do strings"
  [s]
  (let [c (count s)]
    (subs s 0 (- c 4))))

(defn kw->ikey [kw]
  (str (name kw) "_bin"))

(defn ikey->kw [ikey]
  (keyword (trim-tag ikey)))

(defn index-map->indexes [index-map]
  (into []
        (for [[k v] index-map]
          {:key (kw->ikey k)
           :value (str v)})))

(defn indexes->index-map [indexes]
  (into {}
        (for [{:keys [key value]} indexes]
          [(ikey->kw key) value])))


(defn resolve-content
  "Simple sibling resolution by last mod"
  [content]
  (if (seq content)
    (apply max-key :last-mod content)
    nil))

(defn content-map [{:keys [value last-mod indexes content-type] :as content} cv-fn]
  {:value (cv-fn value)
   :content-type content-type
   :last-mod last-mod
   :indexes (indexes->index-map indexes)})


(defrecord ReekClient [^String host
                       port
                       ^AsynchronousSocketChannel conn]
  IReekClient
  (connect [this]
    (let [result (promise)]
      (client/connect host port (conn-cb result))
      (assoc this :conn @result)))

  (shutdown [this]
    (if conn
      (do
        (client/disconnect conn)
        (dissoc this :conn))
      (throw (Exception. "No riak connection."))))

  (fetch [this bucket-name key-name vtype]
    {:pre [(string? bucket-name)
           (string? key-name)
           (keyword? vtype)]}
    (let [result (promise)
          cv-fn (fetch-converter vtype)]
      (object/get
       conn
       (conv/byte-string<-utf8-string bucket-name)
       (conv/byte-string<-utf8-string key-name)
       {}
       (result-cb result))
      (some-> @result
              :content
              resolve-content
              (content-map cv-fn))))

  (store [this bucket-name key-name value vtype content-type index-map]
    {:pre [(string? bucket-name)
           (string? key-name)
           (keyword? vtype)
           (string? content-type)
           ]}
    (let [vclock (promise)
          vclock-cb (fn [asc e a]
                      (if e
                        (throw (Exception. "Couldn't get vclock"))
                        (deliver vclock (:vclock a))))
          result (promise)
          cv-fn (store-converter vtype)]

      ;; Get a vclock
      (object/get
       conn
       (conv/byte-string<-utf8-string bucket-name)
       (conv/byte-string<-utf8-string key-name)
       {}
       vclock-cb)

      (object/put
       conn
       (conv/byte-string<-utf8-string bucket-name)
       (conv/byte-string<-utf8-string key-name)
       {:value (cv-fn value)
        :content-type content-type
        :indexes (index-map->indexes index-map)}
       {:vclock @vclock}
       (result-cb result))
      @result))

  (delete [this bucket-name key-name]
    {:pre [(string? bucket-name)
           (string? key-name)]}
    (let [result (promise)]
      (object/delete
       conn
       (conv/byte-string<-utf8-string bucket-name)
       (conv/byte-string<-utf8-string key-name)
       {}
       (result-cb result))
      @result))

  (query-eq [this bucket-name index-key index-val]
    {:pre [(string? bucket-name)
           (keyword? index-key)
           (string? index-val)]}
    (let [result (promise)]
      (index/get-2i
       conn
       (conv/byte-string<-utf8-string bucket-name)
       (conv/byte-string<-utf8-string (kw->ikey index-key))
       (conv/byte-string<-utf8-string index-val)
       {:qtype :eq}
       (result-cb result))
      (-> @result :keys set)))

  (query-range [this bucket-name index-key min-val max-val]
    {:pre [(string? bucket-name)
           (keyword? index-key)
           (string? min-val)
           (string? max-val)]}
    (let [result (promise)]
      (index/get-2i
       conn
       (conv/byte-string<-utf8-string bucket-name)
       (conv/byte-string<-utf8-string (kw->ikey index-key))
       (conv/byte-string<-utf8-string min-val)
       (conv/byte-string<-utf8-string max-val)
       {:qtype :range}
       (result-cb result))
      (-> @result :keys set))))
