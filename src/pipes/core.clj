(ns pipes.core
  (:refer-clojure
   :exclude [chunk
             chunk-append 	chunk-buffer
             chunk-cons chunk-first
             chunk-next chunk-rest])
  (:require [clojure.algo.monads :as m]
            [clojure.string :as str]
            [http.async.client :as c])
  (:use [pipes.types]
        [pipes.builder]
        [slingshot.slingshot :only [throw+ try+]]))


(defn list-source [lst]
  (source [rem (ref lst)]
    (dosync
     (let [[head & tail] @rem]
       (ref-set rem tail)
       (if head
         (chunk [head])
         (eof))))))

(defn list-sink []
  (sink [cont? vals] [acc (atom [])]
    (do (swap! acc #(concat % vals))
        (if cont? (nothing) (yield @acc [])))))

(defn take-conduit [n]
  (conduit1 [val] [left (atom n)]
    (do (swap! left dec)
        (if (neg? @left)
          (eof)
          (chunk [val])))))

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

(defn left-fuse
  "Fuse a source and a conduit returning a new source. Short circuits
   errors."
  ([source conduit]
     (mkSource
      (let [psrc (prepare* source)
            pcond (prepare* conduit)]
        (fn produce [in]
          (match-enumeration (psrc in) a
            [:error err] (do (close pcond) a)
            [:eof]       (pcond a)
            ;; TODO: Can we eliminate this m-e call? Both get passed
            ;; along? When can this be eliminated?
            [:stream]
            (match-piping (pcond a) b
              [:error]   (do (close psrc) b)
              [:eof]     (do (close psrc) b)
              [:nothing] (produce (block []))))))))
  ([source conduit & conduits]
     (reduce left-fuse (left-fuse source conduit) conduits)))

(defn right-fuse
  "Fuse a conduit and a sink returning a new sink."
  ([conduit sink]
     (mkSink
      (let [pcond (prepare* conduit)
            psink (prepare* sink)]
        (fn consume [in]
          (match-piping (pcond in) a
            [:error] (do (close psink) a)
            [:eof]
            (match-yielded (psink a) b
              [:error] (do (close pcond) b)
              [:nothing] (fail {:fatal "Sink returned Nothing when passed EOF."}))
            
            [:stream] (psink a))))))
  ([conduit1 conduit2 & conduits-and-sink]
     ;; There is no right fold (right reduce) in Clojure, so we have
     ;; to get tricker
     (let [[sink & conds] (reverse conduits-and-sink)]
       (loop [acc sink conds conds]
         (if (empty? conds)
           ;; No more conduits in conds
           (right-fuse conduit1
                       (right-fuse conduit2 acc))
           ;; Apply the first conduit in conds
           (let [[head & rest] conds]
             (recur (right-fuse head acc) rest)))))))

(defn center-fuse
  "Fuse two conduits in order (horizontal composition) returning a new
  conduit. This relation introduces a category on conduits."
  ;; This is the easiest fusion since the interfaces are basically
  ;; identical.
  ([ca cb]
     (mkConduit
      (let [pca (prepare* ca)
            pcb (prepare* cb)]
        (fn passage [in]
          (match-piping (pca in) a
            [:error]   (do (close pcb) a)
            [:eof]     (pcb a)
            [:stream]  (pcb a))))))
  ([ca cb & conds]
     (reduce center-fuse (center-fuse ca cb) conds)))

;;; SOURCE TRANSFORMATION
;;;

(defn default-on-error
  "Converts a source which could potentially return errors to one that
  returns a default type in place of errors. Ignores the consumed
  errors."
  [default source]
  (mkSource
   (let [psrc (atom (prepare* source))]
     (fn [in]
       (match-enumeration (psrc in) a
         [:error] default)))))

(defn eof-on-error
  "Converts a source which could potentially return errors to one that
  EOFs in their place. Ignores the consumed errors."
  [source] (default-on-error (eof) source))

(defn nothing-on-error
  "Converts a source which could potentially return errors to one that
  Nothings instead. Ignores the consumed errors."
  [source] (default-on-error (block) source))

(defn source-repeatedly
  "Take a source and change it into one which repeats an infinite or
  finite number of times. Takes the source and every time the source
  signals an EOF reinitializes the same source again."
  ([inner-source]
     (mkSource
      (let [pinner (atom (prepare* inner-source))]
        (fn loop [in]
          (let [out (@pinner in)]
            (if (not (eof? out))
              out
              (do
                ;; Reinitialize the wrapped Source and loop.
                (swap! pinner (constantly (prepare* inner-source)))
                (loop in))))))))
  ([n inner-source]
     (mkSource
      (let [pinner    (atom (prepare* inner-source))
            eof-count (atom 0)]
        (fn loop [in]
          (let [out (@pinner in)]
            (if (not (eof? out))
              out
              (do
                ;; Increase the counter
                (swap! eof-count inc)
                (if (<= @eof-count n)
                  (do ;; Reinitialize the wrapped Source and loop.
                    (swap! pinner (constantly (prepare* inner-source)))
                    (loop in))
                  ;; Or just pass it through
                  (eof))))))))))

;;; Some example sources
(defn constant-source [v]
  (source []
    (next [] (block [v]))))

(defn naturals-source [& {:keys [from inc] :or {from 0 inc 1}}]
  (source [n (atom (- from inc))]
    (next [] (block [(swap! n (partial + inc))]))
    (replace [vals] (swap! n (first vals)))))

(defn random-source []
  (source []
    (next [] (block [(rand)]))))
            
(defn list-source [vals & {:keys [by] :or {by 1}}]
  (source [memory (ref vals)]
    (next []
          (dosync
           (let [[take rest] (split-at by @memory)]
             (ref-set memory rest)
             (if (empty? take)
               (eof)
               (block take)))))
    (replace [vals]
             (dosync (alter memory (partial concat vals))))))

(defn streaming-http-source
  "Manages an asynchronous HTTP connection and streams string chunks
  from the body of the response."
  [method url & options]
  (source [client (c/create-client)
             resp (apply c/stream-seq client method url options)]
    (next []
      (let [pull (first (c/string resp))]
        (if pull
          (block [pull])
          (eof))))
    (close [] (.close client))))

(defn print-conduit []
  (conduit []
    (pass [val]
      (when (not (empty? val))
        (println val))
      (block val))))

(defn take-conduit [n]
  (conduit [limit (atom (inc n))]
    (pass [vals]
          (let [[add rest] (split-at @limit vals)]
            (swap! limit #(- % (count add)))
            (if (<= @limit 0)
              (eof)
              (block add))))))

(defn map-conduit [f]
  (conduit []
    (pass [vals] (block (map f vals)))))

(defn filter-conduit [pred]
  (conduit []
    (pass [vals]
      (let [vals (filter pred vals)]
        (if (empty? vals)
          (block)
          (block vals))))))

(defn lines-conduit
  "Conduit taking a stream of stream blocks and ensuring that each
  block going forward is a single line of the original stream. Will
  block forever if there are no newlines."
  []
  (conduit [buffer (atom "")]
    (pass [strs]
      (let [s (apply str @buffer strs)
            finds   (re-seq #"[^\r\n]+(\r\n|\r|\n)*" (str @buffer s))
            ;; lines are those where the group matched
            lines   (map (comp str/trim first) (filter second finds))
            ;; if the group doesn't match, that line is incomplete
            remains (first (first (filter (comp not second) finds)))]
        (swap! buffer (constantly remains))
        (if (not (empty? lines))
          (block lines)
          (block))))))

(defn peek-sink []
  (sink []
    (update [vals] (yield (first vals) vals))))

(defn list-sink []
  (sink [memory (atom [])]
    (update [vals] (swap! memory #(concat % vals))
            (block))
    (close [] (yield @memory))))

(defn take-sink [n]
  (right-fuse (take-conduit n) (list-sink)))

(defn reduction-sink
  ([f x0]
     (sink [acc (atom x0)]
       (update [vals]
         (if (= ::none @acc)
           (swap! acc (constantly (reduce f vals)))
           (swap! acc (partial f (reduce f vals))))
         (yield))
       (close []
         (yield @acc))))
  ([f] (reduction-sink f ::none)))