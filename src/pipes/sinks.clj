(ns pipes.sinks
  (:refer-clojure
   :exclude [take list peek reduce empty
             
             chunk
             chunk-append 	chunk-buffer
             chunk-cons chunk-first
             chunk-next chunk-rest])
  (:require [clojure.core :as clj]
            [clojure.algo.monads :as m])
  (:use [pipes builder types]))

(defn take
  "[n -> Sink a [a]] Accumulates `n` values into a Vector. Terminating."
  [n] (sink [cont? vals] [left (atom n)
                          acc  (atom [])]
        (if cont?
          (let [[taken remain] (split-at @left vals)
                ct             (count taken)]
            (swap! left #(- % ct))
            (if (= 0 @left)
              (yield (concat @acc taken) remain)
              (do (swap! acc #(concat % taken))
                  (nothing))))
          (yield @acc vals))))

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
  [] (sink [cont? vals] []
       (if cont?
         (yield (first vals) vals)
         (fail-leaving vals {:error "Cannot peek an EOF."}))))

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

(defn empty
  "[Sink a nil] Immediately yields nil. Terminating."
  []
  (sink [cont? vals] [] (yield nil vals)))

(defn one
  "[Sink a b] Immediately yields `v`. Terminating."
  [v]
  (sink [cont? vals] [] (yield v vals)))

;;; SINK MONADIC INTERFACE
;;;
;;; There is an instance Monad Sink that can be defined where Sinks
;;; are tried sequentially, short-circuiting errors.

(m/defmonad sink-m
  "Monad describing sequential Stream Sinking operations. The monadic
  values are Sink objects."
  [m-result one
   m-bind   (fn m-bind-sink [mv f]
              (sink [cont? vals] [psink1  (prepare mv)
                                  psink2  (atom nil)]
                (if @psink2
                  (@psink2 cont? vals)
                  (match-sink (psink1 cont? vals) a
                    [:yield b rem]
                    (do (swap! psink2 (constantly (prepare (f b))))
                        (if (empty? rem)
                          (nothing)
                          (@psink2 true rem)))))
                (do (close psink1)
                    (if @psink2
                      (close @psink2)))))])