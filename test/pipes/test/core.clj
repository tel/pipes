(ns pipes.test.core
  (:use [pipes.core])
  (:use [clojure.test]))

(defn list-indepotent [lst]
  (connect (list-source lst) (list-sink)))

(deftest listIndepotency
  (is (= (range 30)
         (listIndepotent (range 30)))))
