(ns ctim-types.core
  (:require [ctim.schemas.actor :as actor]
            [ctim.schemas.campaign :as campaign]
            [ctim.schemas.coa :as coa]
            [ctim.schemas.data-table :as data-table]
            [ctim.schemas.exploit-target :as exploit-target]
            [ctim.schemas.feedback :as feedback]
            [ctim.schemas.incident :as incident]
            [ctim.schemas.indicator :as indicator]
            [ctim.schemas.judgement :as judgement]
            [ctim.schemas.relationship :as relationship]
            [ctim.schemas.sighting :as sighting]
            [ctim.schemas.ttp :as ttp]
            [ctim.schemas.verdict :as verdict])
  (:import [flanders.types
            StringType IntegerType InstType BooleanType
            SequenceOfType MapType MapEntry EitherType]))

(defn switch2
  "Reverse the arguments of a 2 argument function"
  [fn2]
  (fn [a b] (fn2 b a)))

(defprotocol Descend
  (leaf? [e] "true if a leaf type"))

(extend-protocol Descend
  Object
  (leaf? [_] false)

  BooleanType
  (leaf? [_] true)

  StringType
  (leaf? [_] true)

  IntegerType
  (leaf? [_] true)

  InstType
  (leaf? [_] true))

(defprotocol TreeElt
  (node-type [entity] "Returns a label for any leaf type"))

(extend-protocol TreeElt
  Object
  (node-type [e] nil)

  SequenceOfType
  (node-type [_] "array")

  MapType
  (node-type [_] "object")

  EitherType
  (node-type [_] "object")

  BooleanType
  (node-type [_] "boolean")

  InstType
  (node-type [_] "string")
  
  StringType
  (node-type [_] "string")
  
  IntegerType
  (node-type [_] "long"))

(defprotocol Recursable
  (recurse-types [entity s] "add descriptions to the sequence, recursing subtypes as necessary"))

(declare accumulate-recursion)

(extend-protocol Recursable
  Object
  (recurse-types [o s]
    (println "Unhandled: " (type o))
    (println o)
    s)

  MapType
  (recurse-types [mt s]
    (accumulate-recursion s (:entries mt)))

  MapEntry
  (recurse-types [me s]
    (let [t (:type me)
          nt (node-type t)
          knm (:default (:key me))]
      (if knm
        (let [nm (name knm)]
          (if (leaf? t)
            (conj s [nm nt])
            (do
              (when-not nt
                (println "Unknown Name: " nm)
                (println me)
                (println))
              (conj
               (recurse-types t s)
               [nm nt]))))
        s)))

  EitherType
  (recurse-types [et s]
    (accumulate-recursion s (:choices et)))

  SequenceOfType
  (recurse-types [se s]
    (accumulate-recursion s (:entries (:type se)))))


(def accumulate-recursion
  "Function for reducing recursion over a seq"
  (partial reduce (switch2 recurse-types)))


(def data-types
  "List of all object types to store in graphs"
  [actor/Actor campaign/Campaign coa/COA data-table/DataTable exploit-target/ExploitTarget
   feedback/Feedback incident/Incident indicator/Indicator judgement/Judgement relationship/Relationship
   sighting/Sighting ttp/TTP verdict/Verdict])

(defn all-type-info
  "Get type pairs for a sequence of data types"
  [types]
  (->> types
       (accumulate-recursion [])
       (into #{})
       (sort-by first)))

(defn -main
  "Go down the type tree and print"
  [& x]
  (let [type-info (all-type-info data-types)]
    (doseq [[property pname] type-info]
      (println property pname))))
