(ns pipes.core
  (:require [clojure.algo.monads :as m]
            [clojure.string :as str]
            [http.async.client :as c])
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
;;; type Error a  = Either ErrorType a
;;; type SourceResult a  = Error (Stream a)
;;; type ConduitResult a = Error (Maybe (Stream a))
;;; type SinkResult a    = Error (Maybe (b, Stream a))
;;;
;;; These are mapped into native Clojure hashes with tags
;;; {:error    <obj>
;;;  :eof      true
;;;  :nothing  true
;;;  :stream   [a]
;;;  :yield    b}
;;; 
;;; So that there are some Haskell --> Clojure translation
;;; correspondences
;;;
;;; Left ErrorType          --> {:error <obj>}
;;; Rt Nothing              --> {:nothing true}
;;; Rt (Just EOF)           --> {:eof true}
;;; Rt (Just (b, Stream a)) --> {:yield b :stream [a]}
;;; Rt (Just (b, EOF))      --> {:yield b :eof true}
;;;
;;; NOTE: Why not use {:stream []} for Nothing? What would (yield)
;;; mean then? What would getting {:stream []} from a Sink mean?
;;;
;;; For convenience, there are a few simple constructors of these
;;; types

(defn fail           ([]     {:error {:fatal true}})
                     ([err]  {:error err}))
(defn eof             []     {:eof true})
(defn block          ([]     {:nothing true})
                     ([xs]   {:stream xs}))
(defn yield          ([]     {:nothing true})
                      ([b]    {:yield b :stream []})
                      ([b xs] {:yield b :stream xs}))

(defn fail? [x] (contains? x :error))
(defn eof? [x] (true? (:eof x)))
(defn nothing? [x] (true? (:nothing x)))
(defn yield? [x] (contains? x :yield))

(defn- the-types
  "Get a dictionary of types available for cond-pair-for-type
  specialized on a particular object symbol."
  [objsym]
  {:error   {:test-form `(fail? ~objsym)
             :binding-forms `[(:error ~objsym)]}
   :nothing {:test-form `(nothing? ~objsym)
             :binding-forms `[]}
   :stream  {:test-form `(:stream ~objsym)
             :binding-forms `[(:stream ~objsym)]}
   :eof     {:test-form `(eof? ~objsym)
             :binding-forms `[]}
   :yield   {:test-form `(yield? ~objsym)
             :binding-forms `[(:yield ~objsym) (:stream ~objsym)]}})

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

(defmacro match-stream
  [obj objsym & forms]
  (matcher [:eof :stream]
           "Expecting type like: Stream a."
           obj objsym
           forms))

(defmacro match-enumeration
  [obj objsym & forms]
  (matcher [:error :eof :stream]
           "Expecting type like: Error (Stream a)."
           obj objsym
           forms))

(defmacro match-piping
  [obj objsym & forms]
  (matcher [:error :eof :nothing :stream]
           "Expecting type like: Error (Maybe (Stream a))."
           obj objsym
           forms))

(defmacro match-yielded
  [obj objsym & forms]
  (matcher [:error :nothing :yield]
           "Expecting type like: Error (Maybe (b, Stream a))."
           obj objsym
           forms))

;;; CLOSURE
;;;
;;; Every Prepared Sink, Source, or Conduit is CLOSED by passing it an
;;; EOF value.

(defn close [thing] (thing (eof)))

;;; CONNECTION
;;;
;;; The first major idea of pipes is that a program is executed (and
;;; indeed, runs immediately) when a source is CONNECTED to a
;;; sink.

(defn connect*
  "Executes the pipe formed when `source` is connected to
  `sink`. Returns a type like (Error (b, Stream a)), compare to
  `connect` which just raises the errors which propagate."
  [source sink]
  (let [psrc  (prepare* source)
        psink (prepare* sink)]
    (loop [return nil]
      (if return
        return
        ;; Else we push the source an empty chunk
        (match-enumeration (psrc (block [])) a
          [:error] (do (close psink) a)

          [:eof]
          (match-yielded (psink a) b
            [:error] (do (close psrc) b)
            ;; Throw this error b/c it just shouldn't happen: a sink
            ;; was malformed.
            [:nothing]   (throw+ {:fatal "Sink did not yield on EOF"}))

          [:stream]
          (match-yielded (psink a) b
            [:error]   (do (close psrc) b)
            [:nothing] (recur nil)))))))

(defn connect
  "Executes the pipe formed when `source` is connected to
  `sink`. Returns the yielded value or, if an errorful value is
  returned, raises it."
  [source sink]
  (match-yielded (connect* source sink) val
    [:error err] (throw+ err)
    [:nothing]   (throw+ {:fatal "CONNECT returned Nothing. Something weird is happening."})
    [:yield out] out))

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
(defn open?  [atom] @atom)
(defmacro short-on-closed [atom val] `(if (open? ~atom) ~val (eof)))
(defn close! [atom] (swap! atom (constantly nil)))

(defmacro eof1fn [arg eof-then & body]
  `(let [doors# (atom true)]
     (fn [arg]
       (if (eof? arg))
       (if @doors#
         ~@body))))

(defn- function-named
  "From the list of fundef forms, pull the one named the given symbols
  and turn it into a lambda form."
  [sym list]
  (let [[name & rest] (first (filter #(= sym (first %)) list))]
    (if (empty? rest)
      nil
      `(fn ~name ~@rest))))

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
            (try+
             (match-stream in# a#
               [:eof]
               (do (close! doors#)
                   (closer#)
                   ;; since it's an EOF, return it
                   a#)

               ;; These values are the replacement values.
               [:stream vals#]
               (if (not (empty? vals#))
                 (do (replacer# vals#)
                     ;; loop again now that we can ensure there's
                     ;; nothing to replace
                     (pull# (block [])))
                 (nexter#)))
             (catch Object o#
               (closer#)
               (fail o#)))))))))

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
    `(mkSink
      (let [doors# (atom true)
            ~@binds
            updater# ~update-form
            closer# ~close-form]
        (fn pull# [in#] ;;:: Stream a
          (if (open? doors#)
            (try+
             (match-stream in# a#
               [:eof]
               (do (close! doors#)
                   (closer#))
               
               [:stream vals#] (updater# vals#))
             (catch Object o#
               (closer#)
               (fail o#)))
            (throw+ {:fatal "Sink cannot recieve data after EOF."
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
    `(mkConduit
      (let [doors# (atom true)
            ~@binds
            passer# ~pass-form
            closer# ~close-form]
        (fn pull# [in#] ;;:: Stream a
          (short-on-closed doors#
            (try+
             (match-stream in# a#
               [:eof] (do (close! doors#)
                          (closer#))
               [:stream vals#] (passer# vals#))
             (catch Object o#
               (closer#)
               (fail o#)))))))))

;;; VERTICAL COMPOSITION
;;; 
;;; Sinks are monads in their return values under vertical
;;; (sequential) composition.
(m/defmonad sink-m
  [m-result (fn [v] (sink []
                      (update [_] (yield v))
                      (close  []  (yield v))))
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
                     [:yield result leftover]
                     (do (swap! inner (constantly (prepare* (f result))))
                         (consume (block leftover)))))))))])

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