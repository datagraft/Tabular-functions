(ns tabular-functions.aggregations
  (
  :require
     [grafter.tabular :refer :all]
     [grafter.rdf :refer [s]]
     [grafter.rdf.protocols :refer [->Quad]]
     [grafter.rdf.templater :refer [graph]]
     [grafter.vocabularies.rdf :refer :all]
     [grafter.vocabularies.foaf :refer :all]
     [incanter.core :as inc]  ))

(defn MIN [coll] ())
(defn MAX [coll] ())
(defn SUM [coll] (apply + coll))
(defn AVG [coll] ())
(defn COUNT [coll] ())
