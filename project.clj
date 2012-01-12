(defproject pipes "1.0.0-SNAPSHOT"
  :description "A Clojure implementation of Conduit IO."
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/algo.monads "0.1.0"]
                 [slingshot "0.10.1"]]
  :repl-options [:init nil :caught clj-stacktrace.repl/pst+])