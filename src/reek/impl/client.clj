(ns reek.impl.client
  (:import [com.basho.riak.client.api RiakClient]))

;; TODO: handle multiple uris, cluster opts
(defn new-client ^RiakClient [uri port]
  (RiakClient/newClient port [uri]))


(defprotocol IReekClient
  "Protocol for client ops"
  (connect [this]
    "Connects the client")
  (shutdown [this]
    "Shuts down the client")
  (execute [this command]
    "Execute a command"))


(defrecord ReekClient [^String uri ^Integer port ^RiakClient conn]
  IReekClient
  (connect [this]
    (assoc this :conn (new-client uri port)))
  (shutdown [this]
    (if conn
      (.shutdown conn)
      (throw (Exception. "No riak connection."))))
  (execute [this command]
    (if conn
      (.execute conn command)
      (throw (Exception. "No riak connection.")))))
