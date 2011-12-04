(ns korma.schema
  (:use [korma.core :only [empty-query exec raw]]))

(defn empty-schema []
  {:columns []
   :constraints []
   :attrs []})

(defn schema* [ent schema]
  (assoc ent :schema schema))

(defmacro schema [ent & body]
  `(let [s# (-> (empty-schema)
                ~@body)] 
     (schema* ~ent s#)))

(defn column [schema name type & attrs]
  (update-in schema [:columns] conj {:col name
                                     :type type
                                     :attrs attrs}))

(defn attr [type]
  (let [as {:pk "PRIMARY KEY"
            :not-null "NOT NULL"
            :auto "AUTO_INCREMENT"
            :serial "SERIAL"}]
    (raw (get as type (name type)))))

(defn primary-key [schema name]
  (column schema name :integer (attr :pk) (attr :not-null) (attr :serial)))

(defn create-query [ent]
  (merge (empty-query ent)
         {:type :create
          :columns (get-in ent [:schema :columns] [])}))

(defn create! [ent]
  (-> (create-query ent)
      (exec)))

(defn drop-query [ent]
  (merge (empty-query ent)
         {:type :drop}))

(defn drop! [ent]
  (-> (drop-query ent)
      (exec)))

(defn alter-query [ent]
  (merge (empty-query ent)
         {:type alter
          :columns []}))

(defn alter! [ent action params]
  (let [query (alter-query ent)
        query (condp = action
                :add (apply column query params)
                :drop (assoc query :drop (first params))
                :alter (assoc query :alter params)
                (throw (Exception. (str "Unknown alteration: " action))))]
    (exec query)))

(comment
(defentity users
  (schema
    (column :name [:varchar 256] :unique)
    (column :cool [:text] :unique)))
 

  (create! users)

  (alter! users :drop [:chris])
  (alter! users :add [:chris [:varchar 256] :not-null])
  (alter! users :modify [:chris :default "hey"])

  (drop! users)
  )
