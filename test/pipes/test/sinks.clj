(ns pipes.test.sinks
  (:require
   [pipes.sources :as so]
   [pipes.sinks   :as si]
   [pipes.core    :as p]
   [clojure.algo.monads :as m])
  (:use [clojure.test]))

(deftest sink-monad-laws
  (let [test-sources [(so/naturals :from 1 :step 1)
                      (so/seq [30 40 50])]
        test-sinks   [(si/take 30)]]
    
    (doseq [source test-sources
            sink1  test-sinks]
      (testing "Left identity: return a >>= f  ≡  f a"
        (is (= (p/connect-and-run source (si/take 3))
               (p/connect-and-run
                source
                (m/m-bind (m/m-result 3) (fn [n] (si/take n))))))))))