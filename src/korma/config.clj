(ns korma.config)

(def options (atom {:delimiters ["\"" "\""]
                    :naming {:fields identity
                             :keys identity}}))

(defn ->delimiters
  [cs]
  (if cs
    (let [[begin end] cs
          end (or end begin)]
      [begin end])
    ["\"" "\""]))

(defn ->naming
  [strategy]
  (merge {:keys identity
          :fields identity}
         strategy))

(defn extract-options
  [{:keys [naming delimiters]}]
  {:naming (->naming naming)
   :delimiters (->delimiters delimiters)})

(defn set-delimiters 
  [& cs]
  (swap! options assoc :delimiters (->delimiters cs)))

(defn set-naming 
  [strategy]
  (swap! options assoc :naming (->naming strategy)))

(defn merge-defaults
  [opts]
  (reset! options opts))
