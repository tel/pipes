(ns pipes.core
  (:refer-clojure
   :exclude [chunk
             chunk-append chunk-buffer
             chunk-cons chunk-first
             chunk-next chunk-rest])
  (:require [pipes
             [conduits :as co]
             [sinks    :as si]
             [sources  :as so]])
  (:use [pipes types builder]
        [slingshot.slingshot :only [throw+ try+]]))

;;; CONNECTION
;;;
;;; The first major idea of pipes is that a program, a Pipeline, is
;;; built when a source is CONNECTED to a sink.

(defn connect
  "Builds the pipeline connecting `source` to `sink`."
  [source sink]
  (mkpipeline
   (prepare-let [psrc  source
                 psink sink]
     (loop [return nil]
       (if return
         return
         ;; Else we pull from the source
         (match-source (psrc true) a
           [:error] (do (close psink) a)

           [:eof]
           (match-sink (psink false []) b
             [:error] (do (close psrc) b))
             
           [:stream as]
           (match-sink (psink true as) b
             [:error]   (do (close psrc) b)
             [:nothing] (recur nil))))))))

(defn run-pipeline*
  "Runs a pipeline."
  [pipeline]
  (pipeline))

(defn run-pipeline
  "Runs the Pipeline. Returns a type like (Error (b, Stream a)),
   compare to `connect` which just raises the errors which propagate."
  [pipeline]
  (match-sink (run-pipeline* pipeline) val
    [:error err] (throw+ err)
    [:nothing]   (throw+ {:fatal "Pipeline invariant violated: returned Nothing."})
    [:yield out] out))

(defn connect-and-run*
  "Runs the Pipeline formed when `source` is connected to
   `sink`. Returns a type like (Error (b, Stream a)), compare to
   `connect-and-run` which just raises the errors which propagate."
  [source sink]
  (run-pipeline* (connect source sink)))

(defn connect-and-run
  "Runs the Pipeline formed when `source` is connected to
   `sink`. Returns the yielded value or, if an errorful value is
   returned, raises it."
  [source sink]
  (run-pipeline (connect source sink)))


;;; FUSION
;;;
;;; Connection by itself is sufficient to define some rather useful
;;; pipes, but for ease of composability and reusability we also have
;;; FUSION. There are left, right, and center fusions which have types
;;; like
;;; 
;;; left-fuse   :: Source a -> Conduit a b -> Source b
;;; right-fuse  :: Conduit a b -> Sink b -> Sink a
;;; center-fuse :: Conduit a b -> Conduit b c -> Conduit a c
;;;
;;; Clearly, fusion is used to build more complex pipelines from
;;; conduit transformation before connecting the ends.

(defn fuse<
  "Fuse a source and one or more conduits returning a new source."
  ([s c]
     (source [psrc  (prepare s)
              pcond (prepare c)]
       (loop []
         (match-source (psrc true) a
           ;; Since conduits can't return Nothing on EOF, its output
           ;; is safe to output from a sink
           [:eof] (pcond false [])

           [:stream as]
           (match-conduit (pcond true as) b
             [:nothing] (recur))))
       (do (close psrc)
           (close pcond))))
  ([s c & cs]
     (reduce fuse< (fuse< s c) cs)))

(defn fuse>
  "Fuse a conduit and a sink returning a new sink."
  ([c s]
     (sink [cont? vals] [pcond (prepare c)
                         psink (prepare s)]
       (match-conduit (pcond cont? vals) a
         [:stream as] (psink true as)
         [:eof]       (psink false []))
       (do (close pcond)
           (close psink))))
  ([c1 c2 & rest]
     ;; There is no right fold (right reduce) in Clojure, so we have
     ;; to get tricker
     (let [[sink & conds] (reverse rest)]
       (loop [acc sink conds conds]
         (if (empty? conds)
           ;; No more conduits in conds
           (fuse> c1
                  (fuse> c2 acc))
           ;; Apply the first conduit in conds
           (let [[head & rest] conds]
             (recur (fuse> head acc) rest)))))))

(defn fuse=
  "Fuse two conduits in order (horizontal composition) returning a new
  conduit. This relation introduces a category on conduits."
  ;; This is the easiest fusion since the interfaces are basically
  ;; identical.
  ([ca cb]
     (conduit [cont? vals] [pca (prepare ca)
                            pcb (prepare cb)]
       (match-conduit (pca cont? vals) a
         [:eof]     (pcb false [])
         [:stream as] (pcb true as))
       (do (close pca)
           (close pcb))))
  ([ca cb & cs]
     (reduce fuse= (fuse= ca cb) cs)))

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