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

(defn integer-literal "Takes a string and converts it to datatype Integer. Non-valid values are replaced with value given as a second parameter, 
  null values are replaced with value given as a third parameter." 
  [n on-error on-null] (if (nil? (re-matches #"[0-9.]+" s)) 0 (Integer/parseInt s)))

(defn double-literal "Takes a string and converts it to datatype Double. Non-valid values are replaced with value given as a second parameter, 
  null values are replaced with value given as a third parameter." 
  [n on-error on-null] ())

(defn date-literal "Takes a string and converts it to datatype Date in given format. Non-valid values are replaced with integer given as a second 
  parameter, null values are replaced with value given as a third parameter." 
  [n on-error on-null date-format] ())
