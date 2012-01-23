(ns pipes.sources
  (:refer-clojure
   :exclude [rand rand-int repeatedly rand-nth seq
             constantly
             
             chunk
             chunk-append chunk-buffer
             chunk-cons chunk-first
             chunk-next chunk-rest])
  (:require [clojure.core :as clj])
  (:use [pipes builder types]
        [slingshot.slingshot :only [throw+ try+]]))

(defn constantly
  "[a -> Source a] Source a constant value forever. Non-terminating."
  [v] (source [] (chunk [v])))

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
  "[Integer -> Source a] Infinite source of random integers; arguments passed to
  `rand-int`. Non-terminating."
  [n] (repeatedly clj/rand-int n))

(defn rand-nth
  "[[a] -> Source a] Infinite source of random choices from sequential
  object `thing`. Non-terminating."
  [thing] (repeatedly clj/rand-nth (clj/seq thing)))

(defn unfold
  "[(s -> [[a] s]) -> s0 -> Source a] Sources an unfolded sequence
  using an update function which transforms a state into either `nil`
  or a vector of an updated state and a set of values. If no values
  can be returned at this iteration, `vals` can be empty and the
  unfold repeats. May terminate: depends on `f`."
  [f s0]
  (source [state (atom s0)]
    (loop []
      (let [out (f @state)]
        (if (nil? out)
          (eof)
          (let [[vals s1] out]
            (swap! state (clj/constantly s1))
            (if (empty? vals)
              (recur)
              (chunk vals))))))))

(defn seq
  "[[a] -> Source a] Source streaming from a sequential object, in
  order, in chunks of count `n` (default 1). Terminating."
  ([thing] (seq 1 thing))
  ([n thing]
     (unfold
      (fn [rem]
        (let [[chunk rest] (split-at n rem)]
          (when (not (empty? chunk))
            [chunk rest])))
      (clj/seq thing))))

(defn naturals
  "[Source a] A source of numbers parameterized by a starting
  point (0) and a delta value (1). Non-terminating."
  [& {:keys [from step] :or {from 0 step 1}}]
  (unfold (fn [s] [[s] (+ step s)]) from))

(defn breadth-first
  "[Tree a -> Source a] Descends into a nested list breadth first and
  sources each element as encountered. Terminating."
  [tree]
  (unfold (fn bf-unfolder [forrest]
            (let [{leaves false branches true}
                  (apply merge-with concat
                         (map (partial group-by sequential?) forrest))]
              (when (or leaves branches)
                [leaves branches])))
          [tree]))

;;; SOURCE TRANSFORMATION
;;;

(defn catch-and-default
  "[Source a -> Source a] The returned Source's body is protected from
  errors returning a default value instead."
  [default src]
  (source [psrc (prepare src)]
    (try+
     (psrc true)
     (catch Object o (eof)))
    (close psrc)))

(defn fail-to-eof
  "[Source a -> Source a] Takes a Source which could return failures
  and replaces them with EOFs."
  [src]
  (source [psrc (prepare src)]
    (match-source (psrc true) a
      [:error] (eof))))

(defn source-repeat
  "[Source a -> Source a] Creates a source which repeats instead of
  sending EOFs infinitely (or finitely for a particular positive
  `n`). If the source EOFs "
  ([src] (source-repeat 0 src))
  ([n src]
     (source [psrc (atom (prepare src))
              ct   (atom 1)]
       (loop []
         (match-source (@psrc true) a
           [:eof]
           (if (or (not (pos? n))
                   (and (pos? n) (< @ct n)))
             (do (swap! ct inc)
                 (swap! psrc (constantly (prepare src)))
                 (recur))
             a)))
       (close @psrc))))