(ns pipes.sources
  (:refer-clojure
   :exclude [rand rand-int repeatedly rand-nth seq
             constantly
             
             chunk
             chunk-append 	chunk-buffer
             chunk-cons chunk-first
             chunk-next chunk-rest])
  (:require [clojure.core :as clj])
  (:use [pipes builder types]))

(defn seq
  "[[a] -> Source a] Source streaming from a sequential object, in
  order, in chunks of count `n` (default 1). Terminating."
  ([thing] (seq 1 thing))
  ([n thing]
     (source [rem (ref (clj/seq thing))]
       (dosync
        (let [[as rest] (split-at n @rem)]
          (ref-set rem rest)
          (if (empty? as)
            (eof)
            (chunk as)))))))

(defn naturals
  "[Source a] A source of numbers parameterized by a starting
  point (0) and a delta value (1). Non-terminating."
  [& {:keys [from step] :or {from 0 step 1}}]
  (source1* [at (atom from)]
    (swap! at (partial + step))))

(defn repeatedly
  "[(() -> a) -> Source a] Infinite source of calls to function
  `f`. Non-terminating."
  [f & args]
  (source [] (chunk [(apply f args)])))

(defn rand
  "[Source a] Inifinite source of random numbers; arguments are passed
  to `rand`. Non-terminating."
  [& args] (repeatedly clj/rand args))

(defn rand-int
  "[n -> Source a] Infinite source of random integers; arguments passed to
  `rand-int`. Non-terminating."
  [n] (repeatedly clj/rand-int n))

(defn rand-nth
  "[[a] -> Source a] Infinite source of random choices from sequential
  object `thing`. Non-terminating."
  [thing] (repeatedly clj/rand-nth (clj/seq thing)))

(defn constantly
  "[a -> Source a] Source a constant value forever. Non-terminating."
  [v] (source [] (chunk [v])))