(defproject pipes/pipes "0.0.2"
  :description "A Clojure implementation of pipey stream processing."
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/algo.monads "0.1.0"]
                 [org.clojure/tools.macro "0.1.1"]
                 [http.async.client "0.4.0"]
                 [slingshot "0.10.1"]]
  :repl-options [:init nil :caught clj-stacktrace.repl/pst+])