(ns korma.config)

(def options (atom {:delimiters ["\"" "\""]
                    :naming {:fields identity
                             :keys identity}}))

(defn- ->delimiters [cs]
  (if cs
    (let [[begin end] cs
          end (or end begin)]
      [begin end])
    ["\"" "\""]))

(defn- ->naming [strategy]
  (merge {:keys identity
          :fields identity}
         strategy))

(defn extract-options [{:keys [naming delimiters subprotocol]}]
  {:naming (->naming naming)
   :delimiters (->delimiters delimiters)
   :subprotocol subprotocol})

(defn set-delimiters
  "Set the global default for field delimiters in connections. Delimiters can either be
  a string or a vector of the start and end:

  (set-delimiters \"`\")
  (set-delimiters [\"[\" \"]\"])"
  [& cs]
  (swap! options assoc :delimiters (->delimiters cs)))

(defn set-naming
  "Set the naming strategy to use. The strategy should be a map with transforms
  to be applied to field names (:fields) and the keys for generated maps (:keys)
  e.g:

  (set-naming {:keys string/lower-case :fields string/upper-case})"
  [strategy]
  (swap! options assoc :naming (->naming strategy)))

(defn merge-defaults
  [opts]
  (swap! options merge opts))
