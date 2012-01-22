(ns pipes.sinks
  (:refer-clojure
   :exclude [take list peek reduce
             
             chunk
             chunk-append 	chunk-buffer
             chunk-cons chunk-first
             chunk-next chunk-rest])
  (:require [clojure.core :as clj])
  (:use [pipes builder types]))

(defn take
  "[n -> Sink a [a]] Accumulates `n` values into a Vector. Terminating."
  [n] (sink1* [val] [left (atom n)
                     acc  (atom [])]
              (do (swap! left dec)
                  (if (= 0 @left)
                    (reverse (cons val @acc))
                    (do (swap! acc (partial cons val))
                        nil)))))

(defn list
  "[Sink a [a]] Accumulates values into a list. Non-terminating."
  [] (sink [cont? vals] [acc (atom [])]
       (if cont?
         (do (swap! acc #(concat % vals))
             (nothing))
         (yield @acc vals))))

(defn peek
  "[Sink a a] Peeks a value off the stream, yielding it and leaving
  the stream unaffected. Terminating."
  [] (sink1 [val] []
       (yield val [val])))

(defn reduce
  "[(b -> a -> b) -> Sink a b] Folds over an incoming stream, yielding
  a reduced value when the stream EOFs. Non-terminating."
  ([f] (reduce f ::none))
  ([f x0]
     (sink [cont? vals] [acc (atom x0)]
       (if cont?
         (do (swap! acc #(if (= % ::none)
                           (clj/reduce f vals)
                           (clj/reduce f % vals)))
             (nothing))
         (yield @acc [])))))