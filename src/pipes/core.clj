(ns pipes.core
  (:require [clojure.algo.monads :as m])
  (:use [clojure.set :only [subset?]]
        [slingshot.slingshot :only [throw+ try+]]))


;;; The wrapper types for which we'll later define fusion, connection,
;;; and monadic interfaces for. These are the objects that are created
;;; and managed when building pipes. The prepare method returns the
;;; appropriate piping function function closed over the prepared
;;; state. These override deref for convenience.
(defrecord Source [prepare]
  clojure.lang.IDeref
  (deref [source] (.prepare source)))

(defrecord Conduit [prepare]
  clojure.lang.IDeref
  (deref [cond] (.prepare cond)))

(defrecord Sink [prepare]
  clojure.lang.IDeref
  (deref [sink] (.prepare sink)))

;;; In Haskell, the non-IO or stateful types dealt with are (loosely)
;;; of the forms:
;;;
;;; data Stream a = Stream a | EOF
;;; type SourceResult a  = Either Error (Stream a)
;;; type ConduitResult a = Either Error (Maybe (Stream a))
;;; type SinkResult a    = Either Error (Maybe (b, Stream a))
;;;
;;; These are mapped into native Clojure hashes with tags
;;; {:error    <<error type, probably a hash>>
;;;  :eof      true
;;;  :nothing  true
;;;  :stream  [a]
;;;  :yield    b}
;;;
;;; So that there are some Haskell --> Clojure translation
;;; correspondences
;;;
;;; Right Nothing              --> {:nothing true}
;;; Left Error                 --> {:error ...}
;;; Right (Just EOF)           --> {:eof true}
;;; Right (Just (b, Stream a)) --> {:yield b :stream [a]}

;;; For convenience, there are a few simple constructors of these
;;; types

(defn- errorT         [err]   {:error err})
(defn- nothingT       []      {:nothing true})
(defn  eof            []      {:eof true})
(defn  chunk          ([xs]   {:stream xs})
                      ([]     {:stream []}))
(defn  yield          ([b xs] {:yield b :stream xs})
                      ([b]    (yield b [])))

;;; And a little helper macro for deconstruction
(defmacro keymatch
  "A cond form which executes a form, binding keys, iff all the keys
  in the binding form are in the target hash."
  [hash & forms]
  (let [biforms (partition 2 forms)
        hash# (gensym)]
    `(let [~hash# ~hash]
       (cond ~@(apply concat
                      (map (fn [[kys form]]
                             `((every?
                                #(not= ::nope
                                       (% ~hash# ::nope))
                                [~@(filter keyword? kys)])
                               (let [~@(apply concat
                                              (map (fn [[key var]]
                                                     `(~var (~key ~hash#)))
                                                   (partition 2 kys)))]
                                 ~form)))
                           biforms))))))

;;; CONNECTION
;;;
;;; The first major idea of pipes is that a program is executed (and
;;; indeed, runs immediately) when a source is CONNECTED to a sink.

(defn connect
  "Executes the pipe formed when `source` is connected to `sink`."
  [source sink]
  (let [psrc  (@source)
        psink (@sink)]
    (try+
      (loop [cyc (chunk)
             ret nil]
        (if (not ret)
          ;; Get a value off the source
          (let [val0 (psrc cyc)]
            (keymatch val0
              ;; :: Left Error
              [:error _] (recur cyc val0)
              ;; :: Right (Stream a)
              [:stream as]
              ;; Pass it to the sink
              (let [val1 (psink val0)]
                (keymatch val1
                  ;; :: Left Error
                  [:error _]     (recur cyc val1)
                  ;; :: Right Nothing
                  [:nothing _]   (recur cyc nil) ; keep going
                  ;; :: Right (Just (b, Stream a))
                  [:yield b
                   :stream as]   (recur {:stream as} b)
                  [] (throw+ {:fatal "Strange value passed to source."
                              :value val1})))
              ;; else
              [] (throw+ {:fatal "Strange value passed to sink."
                          :value val0})))
          ;; Return a value after the sink yields
          ret))
      ;; And be sure to kill state on both of them!
      (finally (psrc  (eof))
               (psink (eof))))))

;;; FUSION
;;;
;;; Connection by itself is sufficient to define some rather useful
;;; pipes, but for ease of composability and reusability we also have
;;; FUSION. There are left, right, and center fusions which have types
;;; like
;;; 
;;; leftFuse   :: Source a -> Conduit a b -> Source b
;;; rightFuse  :: Conduit a b -> Sink b -> Sink a
;;; centerFuse :: Conduit a b -> Conduit b c -> Conduit a c
;;;
;;; Clearly, fusion is used to build more complex pipelines from
;;; conduit transformation before connecting the ends.

(defn leftFuse
  "Fuse a source and a conduit returning a new source."
  [source conduit]
  (Source.
   (fn []
     (let [psrc (@source)
           pcond (@conduit)]
       (fn loop [stream0]
         (let [stream1 (psrc stream0)]
           (keymatch stream1
             ;; Pass errors on through (m-fail like)
             ;; TODO: this isn't correct, I don't want error to be
             ;; true, I just want it to really be a key
             [:error _] stream1
             ;; If there's a legitimate stream, then pass it to the
             ;; conduit
             [:stream as]
             (let [stream2 (pcond stream1)]
               (keymatch stream2
                 ;; Nothing was returned by the conduit? Okay, well,
                 ;; sources can't behave this way, so we loop until it
                 ;; succeeds.
                 [:nothing _] (loop (chunk))
                 ;; Otherwise, in Haskell we'd have to manipulate the
                 ;; types, but in Clojure we're good! Just push it along
                 [] stream2)))))))))

(defn rightFuse
  "Fuse a conduit and a sink returning a new sink."
  [conduit sink]
  (Sink.
   (fn []
     (let [pcond (@conduit)
           psink (@sink)]
       (fn loop [stream0]
         (let [stream1 (pcond stream0)]
           (keymatch stream1
             ;; The conduit returned some values
             [:stream vals]
             (let [stream2 (psink stream1)]
               (keymatch stream2
                 ;; If the sink yields, rewrap the yield dropping the
                 ;; remainder of the inner stream.  This is weird, but
                 ;; clearly necessary for type safety.
                 ;;
                 ;; rightFuse :: Conduit a b -> Sink c b -> Sink c a
                 ;;
                 ;; Therefore, the resultant sink returns leftover
                 ;; streams of type [a], but we don't have those any
                 ;; more (unless we collect from and store them off
                 ;; the conduit).
                 [:yield b] (yield b)
                 [] stream2))
             ;; If the conduit doesn't return values, then there's
             ;; nothing to give to the sink.
             [] stream1)))))))

;;; BUILDS
;;;
;;; Building sources, sinks, (or conduits) by themselves is fairly
;;; easy: just pass a prepare function to the record constructors that
;;; returns an appropriately returning operative function. These will
;;; be described below.

;;; SOURCES
;;; 
;;; Prepared sources are functions of the psuedo-Haskell (ignoring
;;; state and IO monads) type
;;;
;;; PreparedSource :: Stream a -> Either Error (Stream a)
;;;
;;; And they must satisfy the 1-off indepotence
;;; 
;;; EOF^2 == constantly (Right EOF)
;;;
;;; I.e. after they've recieved one EOF, they should always return
;;; EOFs themselves.

;; some helpers
(defn- open?  [atom] @atom)
(defmacro short-on-closed [atom val] `(if (open? ~atom) ~val (eof)))
(defn- close! [atom] (swap! atom (constantly nil)))

(defn- function-named
  "From the list of fundef forms, pull the one named the given symbols
  and turn it into a lambda form."
  [sym list]
  (let [[name & rest] (first (filter #(= sym (first %)) list))]
    (if (empty? rest)
      nil
      `(fn ~@rest))))

(defmacro source [binds & calls]
  (let [next-form
        (or (function-named 'next calls)
            (throw+ {:fatal "Cannot create a source without a definition of next."}))
        close-form
        (or (function-named 'close calls)
            `(fn []))
        replace-form
        (or (function-named 'replace calls)
            `(fn [_#]))]
    `(Source.
      (fn []
        (let [doors# (atom true)
              ~@binds
              closer# ~close-form
              replacer# ~replace-form
              nexter# ~next-form]
          (fn pull# [val#] ;; :: Stream a
            (keymatch val#
              ;; cleanup and eof on eof
              [:eof _#] (do (close! doors#)
                           (closer#)
                           (eof))

              [:stream vals#]
              (if (empty? vals#)
                ;; on empty stream, play a next value
                (short-on-closed doors# (chunk (nexter#)))
                ;; on replacement stream, replace then loop with an
                ;; empty stream
                (do (replacer# vals#)
                    (pull# (chunk))))

              [] (throw+ {:fatal "Strange value passed to source."
                          :value val#}))))))))

;;; Some example sources

(defn- constantlySource [v]
  (source []
    (next [] [v])))

(defn- naturalsSource [& {:keys [from inc] :or {from 0 inc 1}}]
  (source [n (atom (- from inc))]
    (next [] [(swap! n (partial + inc))])
    (replace [vals] (swap! n (first vals)))))
