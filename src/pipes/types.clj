(ns pipes.types
  (:refer-clojure
   :exclude [chunk
             chunk-append 	chunk-buffer
             chunk-cons chunk-first
             chunk-next chunk-rest])
  (:use [slingshot.slingshot :only [throw+ try+]]))

;;; In Haskell, the non-IO or stateful types dealt with are (loosely)
;;; of the forms:
;;;
;;; data Stream a          = Stream a | EOF
;;; type SourceResult a    = Either (Err, Stream a) (Stream a)
;;; type ConduitResult a b = Either (Err, Stream a) (Maybe (Stream b))
;;; type SinkResult a b    = Either (Err, Stream a) (Maybe (b, Stream a))
;;;
;;; These are mapped into native Clojure hashes with tags
;;; {:error     <obj>
;;;  :remaining [a]
;;;  :eof       true
;;;  :nothing   true
;;;  :stream    [a]
;;;  :yield     b}
;;; 
;;; So that there are some Haskell --> Clojure translation
;;; correspondences
;;;
;;; Left (err, Chunk [a])     --> {:error err,
;;;                                :stream [a]}
;;; Left (err, EOF)           --> {:error err,
;;;                                :eof true}
;;; Rt Nothing                --> {:nothing true}
;;; Rt (Just EOF)             --> {:eof true}
;;; Rt (Just (Chunk [a]))     --> {:stream [a]}
;;; Rt (Just (b, Chunk [a]))  --> {:yield b
;;;                                :stream [a]}
;;; Rt (Just (b, EOF))        --> {:yield b :eof true}
;;;
;;; For convenience, there are a few simple constructors of these
;;; types

(defn fail-leaving
  "Describe a failed computation. It's mandatory to pass remaining
  stream elements as `rem` if they exist. Extra arguments are added to
  the thrown hash for debugging purposes."
  ([rem] (fail-leaving rem {}))
  ([rem terms]
     {:error (merge terms
                    {:pipe-fail true})
      :stream rem}))

(defn nothing
  "Describes a Waiting computation, suitable for passing as either a
  ConduitResult or a SinkResult"
  [] {:nothing true})

(defn chunk
  "Wraps a vector into a Chunk message for passing as a SourceResult
   or ConduitResult."
  [xs]
  (if (empty? xs)
    (throw+ {:fatal "Stream invariant violated: cannot send an empty chunk."
             :obj xs})
    {:stream xs}))

(defn eof
  "Describe an end of a Stream. Suitably passed as either a
   SourceResult or a ConduitResult."
  [] {:eof true})

(defn yield
  "Describes a Yielded computation. Suitable only as a SinkResult"
  [b xs] {:yield b :stream xs})


(defn fail? [x] (contains? x :error))
(defn eof? [x] (true? (:eof x)))
(defn nothing? [x] (true? (:nothing x)))
(defn yield? [x] (contains? x :yield))

(defn stream
  "Extracts the stream from a Stream object or returns nil if it's
  EOF."
  [x] (if (not (eof? x))
        (let [chk (:stream x)]
          (if (not (empty? chk))
            chk
            (throw+ {:fatal "Stream invariant violated: received empty Chunk [a] object."
                     :obj x})))))

(defn remainder
  "Extracts the remaining stream chunks from either a Failed or a Yielded
  computation"
  [x] (let [rem (:stream x)]
        (if (not (empty? rem))
          rem)))

(defn- the-types
  "Get a dictionary of types available for cond-pair-for-type
  specialized on a particular object symbol."
  [objsym]
  {:error   {:test-form `(fail? ~objsym)
             :binding-forms `[(:error ~objsym) (remainder ~objsym)]}
   :nothing {:test-form `(nothing? ~objsym)
             :binding-forms `[]}
   :stream  {:test-form `(stream ~objsym)
             :binding-forms `[(stream ~objsym)]}
   :eof     {:test-form `(eof? ~objsym)
             :binding-forms `[]}
   :yield   {:test-form `(yield? ~objsym)
             :binding-forms `[(:yield ~objsym) (remainder ~objsym)]}})

;;; CORE TYPES
;;; 

;;; The actual objects we manipulate have stateful and statefree
;;; (thunked) interfaces. The statefree interfaces are the ones that
;;; should be manipulated, but to build the statefree interfaces we
;;; need to understand the stateful ones.
;;; 
;;; The statefree types for which we'll later define fusion,
;;; connection, and monadic interfaces for. These are the objects that
;;; are created and managed when building pipes. The prepare method
;;; returns the appropriate piping function function closed over the
;;; prepared state.
(defprotocol PPreparable
  "Preparable types are thunks over function which can be 'prepared'
  to initialize some state closed in the function. Sources, Sinks, and
  Conduits all implement this protocol."
  (prepare [thing] "Should return a PClosable and IFn'd Pipe object"))

(defrecord Source [prepFn]
  PPreparable (prepare [_] (prepFn)))

(defrecord Conduit [prepFn]
  PPreparable (prepare [_] (prepFn)))

(defrecord Sink [prepFn]
  PPreparable (prepare [_] (prepFn)))

;;; There's also the Pipeline type which occurs when a Source and Sink
;;; are combined.

(defrecord Pipeline [run]
  clojure.lang.IFn
  (invoke [_] (run))
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

;;; The stateful types are ***Pipes and have simple type signature
;;; interfaces:
;;;
;;; SourcePipe  a   = Bool        -> SourceResult a
;;; ConduitPipe a b = Bool -> [a] -> ConduitResult a b
;;; SinkPipe    a b = Bool -> [a] -> SinkResult a b
;;;
;;; Where the first Boolean argument is the continuation signal and
;;; the second Vector argument is the chunk being passed.
;;; 
;;; Note that the IFn interface enforces that they only take single
;;; input arguments.
(defrecord SourcePipe  [pipe]
  clojure.lang.IFn
  (invoke [_ continue?] (pipe continue?))
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

(defrecord ConduitPipe [pipe]
  clojure.lang.IFn
  (invoke [_ continue? chunk] (pipe continue? chunk))
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

(defrecord SinkPipe    [pipe]
  clojure.lang.IFn
  (invoke [_ continue? chunk] (pipe continue? chunk))
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

;;; All pipes have the action of being closed, but they don't have the
;;; same functional interface: Conduits and Sinks are closed if they
;;; receive `EOF`, but Sources close iff they receive `false`.

(defprotocol PClosable
  "Closable types are Prepared Pipes which can be closed."
  (close [thing] "Closes the Pipe."))

(extend-protocol PClosable
  SourcePipe  (close [pipe] (pipe false))
  ConduitPipe (close [pipe] (pipe false []))
  SinkPipe    (close [pipe] (pipe false [])))


;;; MATCHING
;;;
;;; We define custom "type matching" machinery to make the rest of
;;; these types and macros easy.

(defn- cond-pair-for-type
  "Returns a pair of quoted forms for use in cond. The first is a test
  which determines if the object stored in objsym is of the right
  type, the second is a quoted form where `form` is evaluated in a
  context of the deconstructed type accoring to `binds`."
  [types binds form]
  (let [[kwd & binds] binds]
    `[~(get-in types [kwd :test-form])
      (let [~@(mapcat list binds (get-in types [kwd :binding-forms]))]
        ~form)]))

(defn- matcher
  "Builds a let/cond structure appropriate for the match-* series of
  macros. By default, if no clause is given for a type which can be
  accepted then it's just passed through."
  [types description obj objsym forms]
  (when (not (even? (count forms)))
    (throw+ {:fatal "matcher macro needs an even number of conditional forms."
             :forms forms}))
  `(let [~objsym ~obj]
     (cond
      ~@(let [ ;; Apply passthrough defaults to forms
              forms (concat forms
                            (mapcat (fn [kwd] [[kwd] objsym])
                                    types))
              types (select-keys (the-types objsym) types)]
          (mapcat
           #(apply cond-pair-for-type types %)
           (partition 2 forms)))
      ;; Else
      true (throw+ {:fatal ~description
                    :obj ~objsym}))))

;;; MATCHER MACROS
;;;
;;; Using the above machinery we can build matchers for the various
;;; types expected to be seen.

(defmacro match-stream
  "Matcher for types of Stream a.

   Stream a = Chunk a | EOF"
  [obj objsym & forms]
  (matcher [:eof :stream]
           "Expecting type like: Stream a."
           obj objsym
           forms))

(defmacro match-source
  "Matcher for types of SourceResult a.

   SourceResult a = Left (Err, Stream a)
                  | Right (Stream a)
                  | Right EOF"
  [obj objsym & forms]
  (matcher [:error :eof :stream]
           "Expecting type like: Error (Stream a)."
           obj objsym
           forms))

(defmacro match-conduit
  "Matcher for type of ConduitResult a b.

   ConduitResult a b = Left (Err, Stream a)
                     | Right Nothing
                     | Right (Just (Stream b))
                     | Right (Just EOF)"
  [obj objsym & forms]
  (matcher [:error :nothing :eof :stream]
           "Expecting type like: Error (Maybe (Stream a))."
           obj objsym
           forms))

(defmacro match-sink
  "Matcher for type of SinkResult a b.

   SinkResult a b = Left (Err, Stream a)
                  | Right Nothing
                  | Right (b, Stream a)"
  [obj objsym & forms]
  (matcher [:error :nothing :yield]
           "Expecting type like: Error (Maybe (b, Stream a))."
           obj objsym
           forms))