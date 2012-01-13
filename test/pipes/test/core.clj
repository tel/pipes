(ns pipes.test.core
  (:use [pipes.core])
  (:use [clojure.test]))

(defn list-indepotent [lst]
  (connect (list-source lst) (list-sink)))

(deftest list-indepotency
  (is (= (range 30)
         (list-indepotent (range 30)))))

(deftest take-test
  (is (= (count (connect (constant-source 1) (take-sink 30)))
         30)))