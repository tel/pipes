(ns pipes.conduits
  (:refer-clojure
   :exclude [println map filter merge
             
             chunk
             chunk-append 	chunk-buffer
             chunk-cons chunk-first
             chunk-next chunk-rest])
  (:require [clojure.core :as clj]
            [clojure.string :as str])
  (:use [pipes builder types]))

(defn pass
  "[Integer -> Conduit a a] Passes `n` values then returns EOF
  forevermore. Not identical to take."
  [n]
  ;; Note that we can't use conduit1 here since we *might* want to
  ;; pass along nil values
  (conduit1 [val] [left (atom n)]
    (do (swap! left dec)
        (if (neg? @left)
          (eof)
          (chunk [val])))))

(defn println []
  (conduit1 [val] []
    (do (clj/println val)
        (chunk [val]))))

(defn map [f]
  (conduit [cont? vals] []
    (if cont?
      (chunk (clj/map f vals))
      (eof))))

(defn filter [pred]
  (conduit [cont? vals] []
    (if cont?
      (let [vals (clj/filter pred vals)]
       (if (empty? vals)
         (nothing)
         (chunk vals)))
      (eof))))

(defn lines
  "[Conduit String [String]] Conduit chunking a string into lines. If
  the string terminates, returns whatever the last trailing string has
  been even if it is not newline terminated."
  []
  (conduit [cont? strs] [buffer (atom "")]
    (if cont?
      (let [s (apply str @buffer strs)
            finds   (re-seq #"[^\r\n]+(\r\n|\r|\n)*" s)
            ;; lines are those where the group matched
            lines   (clj/map (comp str/trim first) (clj/filter second finds))
            ;; if the group doesn't match, that line is incomplete
            remains (first (first (clj/filter (comp not second) finds)))]
        (swap! buffer (constantly remains))
        (if (not (empty? lines))
          (chunk lines)
          (nothing)))
      (if (empty? @buffer)
        (eof)
        (chunk @buffer)))))

(defn split-every
  "[Integer -> Conduit a [a]] Conduit that breaks an input stream into
  vectors of length `n`."
  [n]
  (conduit [cont? vals] [rem (atom [])]
    (if cont?
      (let [vals (concat @rem vals)
            [parts remainder] (loop [acc [] vals vals]
                                (let [[part rest] (split-at n vals)]
                                  (if (= n (count part))
                                    (recur (cons part acc) rest)
                                    [(reverse acc) part])))]
        (swap! rem (constantly remainder))
        (if (empty? parts)
          (nothing)
          (chunk parts)))
      (chunk @rem))))

(defn groups-of
  "[Integer -> Conduit a [a]] Conduit that returns groupings of size
  `n` from the stream. A lot like split-every but assured to only
  return groups of size `n`."
  [n]
  (conduit [cont? vals] [pcond (prepare (split-every n))]
    (if cont?
      (pcond true vals)
      (eof))
    (close pcond)))

(defn merge
  "[Conduit [a] a] Conduit that merges sequential objects into a
  single sequence. Takes an optional merging function (\"merge-with\")
  applied on the stream to merge objects; defaults to concat."
  ([] (merge concat))
  ([merger]
     (map (partial apply merger))))