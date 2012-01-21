(ns pipes.util
  (:refer-clojure
   :exclude [chunk
             chunk-append 	chunk-buffer
             chunk-cons chunk-first
             chunk-next chunk-rest])
  (:use [pipes.types]
        [slingshot.slingshot :only [throw+ try+]])
  (:import  [pipes.types
             Pipeline
             Source  SourcePipe
             Conduit ConduitPipe
             Sink    SinkPipe]))

(defn nillify [atom] (swap! atom (constantly false)))

(defn source-protected-run
  "Rewrites run forms to be protected to follow the source EOF
  conventions."
  ([args body]
     ;; If the doors are open, pass values on, else just pass EOFs
     `(let [doors# (atom true)]
        (SourcePipe.
         (fn run-source ~args
           (if (not @doors#)
             ;; If the doors are already closed, just return EOF
             (eof)
             
             ;; Otherwise, process the body
             (match-source (do ~@body) out#
               [:error] (do (nillify doors#) out#)
               [:eof]   (do (nillify doors#) out#)
               [:stream] (if (not ~(first args))
                           (do (nillify doors# out#)
                               (eof))
                           out#))))))))

(defn conduit-protected-run
  "Rewrites run forms to be protected to follow the conduit EOF
  conventions. Unlike source EOF conventions, conduits can return up
  to one more object after receiving EOF."
  ([args body]
     ;; If the doors are open, pass values on, else just pass EOFs
     `(let [doors# (atom true)]
        (ConduitPipe.
         (fn run-conduit ~args
           (if (not @doors#)
             ;; If the doors are already closed, just return EOF
             (eof)
             
             ;; Otherwise, process the body
             (let [out# (do ~@body)]
               ;; If the body returns an Error, EOF or the fn was
               ;; passed a kill signal, close the doors so future
               ;; results are EOF'd
               (when (or (fail? out#)
                         (eof? out#)
                         (not ~(first args)))
                 (swap! doors# (constantly false)))
               ;; But ALWAYS return the body if it makes it this far
               out#)))))))

(defn sink-protected-run
  "Rewrites run forms to be protected to follow the sink EOF
  conventions. Note that Sinks cannot pass EOFs so they raise errors
  instead."
  ([args body]
     ;; If the doors are open, pass values on, else just pass EOFs
     `(let [doors# (atom true)]
        (SinkPipe.
         (fn run-sink ~args
           (if (not @doors#)
             ;; If the doors are already closed then this is an
             ;; errorful situation, raise it!
             (throw+
              {:fatal "Sink invariant violated: cannot receive more data after Yielding or closure"
               :input ~(second args)})
             
             ;; Otherwise, process the body
             (let [out# (do ~@body)]
               ;; If the body returns and Error, Yield, or the fn was
               ;; passed a kill signal, close the doors so future
               ;; results are EOF'd
               (when (or (fail? out#)
                         (eof? out#)
                         (not ~(first args)))
                 (swap! doors# (constantly false))
                 ;; Furthermore, since this is a closing move, the
                 ;; output needs to either be a failure or a
                 ;; yield. Otherwise, raise an error.
                 (when (not (or (fail? out#) (yield? out#)))
                   (throw+
                    {:fatal "Sink invariant violated: Sink did not Yield or Fail on closure."
                     :return out#})))
               ;; But ALWAYS return the body if it makes it this far
               out#)))))))