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

(defn MIN [& args] (apply min args))
(defn MAX [& args] (apply max args))
(defn SUM [& args] (apply + args))
(defn COUNT [& args] (count (into [] args)))
(defn AVG [& args] (/ (apply SUM args) (apply COUNT args)))
(defn COUNT-DISTINCT [& args] (count (distinct (into [] args))))
(defn MEDIAN [& args] ())
