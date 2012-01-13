(ns pipes.core
  (:require [clojure.algo.monads :as m])
  (:use [clojure.set :only [subset?]]
        [slingshot.slingshot :only [throw+ try+]]))


;;; The wrapper types for which we'll later define fusion, connection,
;;; and monadic interfaces for. These are the objects that are created
;;; and managed when building pipes. The prepare method returns the
;;; appropriate piping function function closed over the prepared
;;; state. These override deref for convenience.
(defrecord Source [prepareFn])
(defrecord Conduit [prepareFn])
(defrecord Sink [prepareFn])

(defmacro mkSource [& body]
  `(Source.
    (fn []
      ~@body)))

(defmacro mkConduit [& body]
  `(Conduit.
    (fn []
      ~@body)))

(defmacro mkSink [& body]
  `(Sink.
    (fn []
      ~@body)))

(defn prepare*
  "Prepares a SourceConduitSink by calling its stored prepare
  function."
  [scs] ((.prepareFn scs)))

;;; TODO: This method of preparation doesn't work because we need to
;;; have the let form surround the function, but the try forms need to
;;; be inside the function.
;; 
;; (defmacro prepare
;;   "Binds prepared forms of sources, sinks, and conduits while ensuring
;;   that errors raised by the sequential preparation don't prevent all
;;   pipes from closing."
;;   [binds & body]
;;   (let [[[sym form] rest] (split-at 2 binds)]
;;     (if (and sym form)
;;       `(let [~sym (prepare* ~form)]
;;          (try+
;;            ;; Expand the rest of the binding forms
;;            (prepare ~rest ~@body)
;;            ;; And ensure that upon errors, EOFs are properly passed
;;            (finally (~sym (eof)))))
;;       `(do ~@body))))

;;; In Haskell, the non-IO or stateful types dealt with are (loosely)
;;; of the forms:
;;;
;;; data Stream a = Stream a | EOF
;;; type SourceResult a  = Stream a
;;; type ConduitResult a = Maybe (Stream a)
;;; type SinkResult a    = Maybe (b, Stream a))
;;;
;;; These are mapped into native Clojure hashes with tags
;;; {:eof      true
;;;  :nothing  true
;;;  :stream   [a]
;;;  :yield    b}
;;; 
;;; So that there are some Haskell --> Clojure translation
;;; correspondences
;;;
;;; Right Nothing              --> {:nothing true}
;;; Left Error                 --> {:error ...}
;;; Right (Just EOF)           --> {:eof true}
;;; Right (Just (b, Stream a)) --> {:yield b :stream [a]}
;;; Right (Just (b, EOF))      --> {:yield b :eof true}
;;;
;;; For convenience, there are a few simple constructors of these
;;; types

(defn  eof             []     {:eof true})
(defn  block          ([]     {:nothing true})
                      ([xs]   {:stream xs}))
(defn  yield          ([]     {:nothing true})
                      ([b]    {:yield b :eof true})
                      ([b xs] {:yield b :stream xs}))

;;; There are only three kinds of "type matching" that need to happen,
;;; one for each kind of result.
(defmacro match-enumeration
  ;; matches: Stream a
  [obj objsym
   eof-binds    eof-form
   stream-binds stream-form]
  `(let [~objsym ~obj]
     (cond
      ;; EOF
      (:eof ~objsym)
      (let [~@(mapcat list eof-binds
                      [])]
        ~eof-form)

      ;; Stream a
      (:stream ~objsym)
      (let [~@(mapcat list stream-binds
                      `[(:stream ~objsym)])]
        ~stream-form)

      ;; Else
      true
      (throw+ {:fatal "Expecting type (Stream a)."
               :obj   ~objsym}))))

(defmacro match-piping
  ;; matches: Maybe (Stream a)
  [obj objsym
   eof-binds     eof-form
   nothing-binds nothing-form
   stream-binds  stream-form]
  `(let [~objsym ~obj]
     (cond
      ;; Just EOF
      (:eof ~objsym)
      (let [~@(mapcat list eof-binds
                      [])]
        ~eof-form)

      ;; Nothing
      (:nothing ~objsym)
      (let [~@(mapcat list nothing-binds
                      [])]
        ~nothing-form)
      
      ;; Just (Stream a)
      (:stream ~objsym)
      (let [~@(mapcat list stream-binds
                      `[(:stream ~objsym)])]
        ~stream-form)

      ;; Else
      true
      (throw+ {:fatal "Expecting type Maybe (Stream a)."
               :obj   ~objsym}))))

(defmacro match-yielded
  ;; matches: Maybe (b, Stream a)
  [obj objsym
   nothing-binds nothing-form
   yield-binds   yield-form]
  `(let [~objsym ~obj]
     (cond
      ;; Nothing
      (:nothing ~objsym)
      (let [~@(mapcat list nothing-binds
                      [])]
        ~nothing-form)
      
      ;; Just (b, Stream a)
      (:yield ~objsym)
      (let [~@(mapcat list yield-binds
                      `[(:yield  ~objsym)
                        (:stream ~objsym)])]
        ~yield-form)

      ;; Else
      true
      (throw+ {:fatal "Expecting type Maybe (Stream a)."
               :obj   ~objsym}))))

;;; CONNECTION
;;;
;;; The first major idea of pipes is that a program is executed (and
;;; indeed, runs immediately) when a source is CONNECTED to a sink.

(defn connect
  "Executes the pipe formed when `source` is connected to `sink`."
  [source sink]
  (let [psrc  (prepare* source)
        psink (prepare* sink)]
    (loop [return nil]
      (if return return
          ;; Push the source an empty chunk
          (match-enumeration (psrc (block [])) a
            [] (match-yielded (psink a) b
                 [] (throw+ {:fatal "Sink did not yield on EOF"})
                 [out] out)
            [] (match-yielded (psink a) b
                 [] (recur nil)
                 [out] out))))))

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
  "Fuse a source and a conduit returning a new source."
  [source conduit]
  (mkSource
   (let [psrc (prepare* source)
         pcond (prepare* conduit)]
     (fn produce [in]
       (match-enumeration (psrc in) a
         [] (pcond a)
         ;; TODO: Can we eliminate this m-e call? Both get passed
         ;; along? When can this be eliminated?
         [] (match-piping (pcond a) b
              [] b
              [] (produce (block []))
              [] b))))))

(defn right-fuse
  "Fuse a conduit and a sink returning a new sink."
  [conduit sink]
  (mkSink
   (let [pcond (prepare* conduit)
         psink (prepare* sink)]
     (fn consume [in]
       (match-piping (pcond in) a
         [] (match-yielded (psink a) b
              [] (throw+ {:fatal "Sink returned Nothing when passed EOF."})
              [] b)
         [] a
         [] (psink a))))))

(defn center-fuse
  "Fuse two conduits in order (horizontal composition) returning a new
  conduit. This relation introduces a category on conduits."
  ;; This is the easiest fusion since the interfaces are basically
  ;; identical.
  [ca cb]
  (mkConduit
   (let [pca (prepare* ca)
         pcb (prepare* cb)]
     (fn passage [in]
       (match-piping (pca in) a
         [] (pcb a)
         [] a
         [] (pcb a))))))

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
;;; PreparedSource :: Stream a -> Stream a
;;;
;;; And they must satisfy the law that
;;;
;;; EOF -> EOF; * -> EOF forever
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

(defmacro source
  "Define a Source optionally closed over a let binding. A minimal
  definition requires one definition form

  (next [] ...) ;:: Either Error (Stream a)

  which assumes that empty next chunks means EOF. Note, importantly,
  that (nothing) is not a valid value for next. If the source can
  replace unused chunks, you should also define replace

  (replace [stream] ...) ;:: ()

  and if this source opens scarce resources, you should define

  (close [] ...) ;:: ()

  which is called when the source is considered dead, either by
  producing an EOF or when the sink yields a final value."

  [binds & def-forms]
  (let [next-form
        (or (function-named 'next def-forms)
            (throw+ {:fatal "Cannot create a source without a definition of next."}))
        close-form
        (or (function-named 'close def-forms)
            `(fn []))
        replace-form
        (or (function-named 'replace def-forms)
            `(fn [_#]))]
    `(mkSource
      (let [doors# (atom true)
            ~@binds
            closer# ~close-form
            replacer# ~replace-form
            nexter# ~next-form]
        (fn pull# [in#] ;; :: Stream a
          (short-on-closed doors#
            (match-enumeration in# a#
              [] (do (close! doors#)
                     (closer#)
                     ;; since it's an EOF, return it
                     a#)
              ;; These values are the replacement values.
              [vals#] (if (not (empty? vals#))
                        (do (replacer# vals#)
                            ;; loop again now that we can ensure there's
                            ;; nothing to replace
                            (pull# (block [])))
                        (nexter#)))))))))

(defmacro sink
  "Define a Sink optionally closed over a let binding. A minimal
  definition requires one definition form

  (update [vals] ...) ;:: Either Error (Maybe (b, Stream a))

  but if this sink opens scarce resources or can intelligently handle
  EOFs, you should define

  (close [] ...) ;:: Either Error (Maybe (b, Stream a))

  which is called when the sink is considered dead, either when it
  yields or is passed an EOF."
  [binds & def-forms]
  (let [update-form
        (or (function-named 'update def-forms)
            (throw+ {:fatal "Cannot create a source without a definition of update."}))
        close-form
        (or (function-named 'close def-forms)
            `(fn [] (yield nil)))]
    `(mkSource
      (let [doors# (atom true)
            ~@binds
            updater# ~update-form
            closer# ~close-form]
        (fn pull# [in#] ;;:: Stream a
          (if (open? doors#)
            (match-enumeration in# a#
              [] (do (close! doors#)
                     (closer#))
              [vals#] (updater# vals#))
            (throw+ {:fatal "Sink recieved values after being closed."
                     :obj in#})))))))

(defmacro conduit
  "Define a Conduit optionally closed over a let binding. A minimal
  definition requires one definition form like

  (pass [vals] ...) ;:: Either Error (Maybe (Stream a))

  but may also define another form

  (close [] ...) ;:: Either Error (Maybe (Stream a))

  which is called when the conduit either receives or returns an EOF."
  [binds & def-forms]
  (let [pass-form
        (or (function-named 'pass def-forms)
            (throw+ {:fatal "Cannot create a source without a definition of pass."}))
        close-form
        (or (function-named 'close def-forms)
            `(fn [] (eof)))]
    `(Conduit.
      (fn []
        (let [doors# (atom true)
              ~@binds
              passer# ~pass-form
              closer# ~close-form]
          (fn pull# [in#] ;;:: Stream a
            (short-on-closed doors#
              (match-enumeration in# a#
                [] (do (close! doors#)
                       (closer#))
                [vals#] (passer# vals#)))))))))

;;; VERTICAL COMPOSITION
;;; 
;;; Sinks are monads in their return values under vertical
;;; (sequential) composition.
(m/defmonad sink-m
  [m-result (fn [v] (sink []
                      (update [_] (yield v))
                      (close  []  (yield v)))
              (mkSink (fn [_] (yield v))))
   m-bind (fn [sink f]
            (mkSink
             (let [psink (prepare* sink)
                   inner (atom nil)]
               (fn consume [in]
                 (if @inner
                   ;; If we've already passed control to the inner
                   ;; sink, just pass it through
                   (@inner in)
                   ;; Otherwise, we need to exhaust the outer one
                   ;; first
                   (match-yielded (psink in) a
                     [] a
                     [result leftover]
                     (do (swap! inner (constantly (prepare* (f result))))
                         (consume (block leftover)))))))))])

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

(defn map-conduit [f]
  (conduit []
    (pass [vals] (block (map f vals)))))

(defn take-conduit [n]
  (conduit [limit (atom n)]
    (pass [vals]
          (let [[add rest] (split-at @limit vals)]
            (swap! limit #(- % (count add)))
            (if (< @limit 0)
              (eof)
              (block add))))))

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