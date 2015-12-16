(ns tabular-functions.datatypes
  (
  :require
     [grafter.tabular :refer :all]
     [grafter.rdf :refer [s]]
     [grafter.rdf.protocols :refer [->Quad]]
     [grafter.rdf.templater :refer [graph]]
     [grafter.vocabularies.rdf :refer :all]
     [grafter.vocabularies.foaf :refer :all]
     [incanter.core :as inc]  ))
(defn convert-literal "Converts given value to the specified datatype.
              
              " [x dtype  &{:keys [on-empty on-error] :or {on-error false on-empty 0}}]
(let [f (case dtype 
              "byte" #(byte %)
              "short" #(short %)
              "double" #(double %)
              "decimal" #(bigdec %)
              "integer" #(int %)
              "long" #(long %))
  ;    parser (case dtype 
   ;                "byte" #(byteByte/parseByte %)
    ;               "short" #(Short/parseShort %)
     ;              "double" #(Double/parseDouble %)
      ;             "decimal" #(bigdec %)
       ;            "integer" #(int (Double/parseDouble %)))
      ]
;numeric types
(case dtype
      ("byte" "short" "double" "decimal" "integer" "long") (cond   (or (nil? x) (empty? (str x))) (f on-empty)
                                        (and (number? on-error) (nil? (re-matches #"[0-9.]+" (str x)))) (f on-error)
                                        :else (f (Double/parseDouble (apply str (re-seq #"[\d.]+" (str x))))))

  

  )
)
  )

(defn integer-literal-new "Takes a string and converts it to datatype Integer.  Empty values are replaced with value given as a second parameter, 
  non-valid values are replaced with value given as a third parameter." 
  [n on-empty on-error] 
  (letfn [(str->int [x] (unchecked-int (Double/parseDouble (str x))))
          (clean-commas [x] (clojure.string/replace (str x) #"," ""))]
   (cond (or (nil? n) (empty? (clean-commas n))) (str->int on-empty) 
        (nil? (re-matches #"[0-9.]+" (clean-commas n))) (str->int on-error)
        :else (str->int (clean-commas n) ) ))) 

(defn double-literal "Takes a string and converts it to datatype Integer. Empty values are replaced with value given as a second parameter, 
  non-valid values are replaced with value given as a third parameter." 
  [n on-empty on-error] 
  (cond (or (nil? n) (empty? (str n))) on-empty 
        (nil? (re-matches #"[0-9.]+" (str n))) on-error 
        :else (Double/parseDouble (str n)) )) 

(defn date-literal "Takes a string and converts it to datatype Date in given format. Empty values are replaced with value given as a second 
  parameter, non-valid values are replaced with value given as a third parameter." 
;TODO: look at clj-time for conversions
  [n on-empty on-error date-format] (
.parse (java.text.SimpleDateFormat. date-format) n)                       )
