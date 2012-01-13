# pipes

<img src="https://github.com/downloads/tel/pipes/thepipe.png"
 alt="Pipes logo" title="Stylish, functional, iconic." align="right" />

Pipey stream processing for Clojure. 
Build yourself a streaming pipeline from orthogonal pieces

## Background

Originally concieved by [Oleg](http://okmij.org/ftp/) for Haskell as 
["Enumerators and Iteratees"](http://okmij.org/ftp/Streams.html), 
pipey processing was a solution for building IO pipelines which can 
process input in constant space while keeping tight control of file 
handles and other scare resources. The concept was expanded in Yesod 
into [Conduits](http://www.yesodweb.com/blog/2012/01/conduits-conduits)
which admitted that mutable state was easier to handle than monadic
state.

Pipes is a Clojure implementation of these ideas. As Clojure has
easily accessible mutable state and try+/catch+ (via
[Slingshot](https://github.com/scgilardi/slingshot)) the
implementation is fairly simple allowing new Sources, Sinks, and
Conduits to be written easily.

Use pipey computation to handle streaming data and light parsing
today!

## Usage

Use lein or cake

```Clojure
[pipes/pipes "0.0.1"]
```

then

```Clojure
(use 'pipes.core)
(use 'cheshire.core)

;; Get yourself 20 tweets
(p/connect
 (p/left-fuse
  (streaming-http-source
   :get "https://stream.twitter.com/1/statuses/sample.json"
   :auth {:user u :password p})
  (p/nothing-on-error 
   (p/map-conduit #(parse-string % true)))
  (p/map-conduit #(select-keys % [:text])))
 (p/take-sink 20))
```

## Wait, what just happened?

Pipey processing starts out generalizing `reduce`. Normally there 
are two things going on in a `reduce`

1. A value is popped off a seq
2. That value is fed into an accumulation function along with the 
   current state (in the parlance "it's sunk into a Sink").

things get pipey when you separate those concerns.

```Clojure
(partial reduce + 0)
```

is the same as

```Clojure
(fn [lst]
  (connect (list-source lst)
           (sink [acc (atom 0)]
             (update [vals]
               (swap! acc (partial + (reduce + vals)))
               (yield))
             (close []
               (yield @acc)))))
```

which looks much more complex, but now the `(list-source lst)` 
component is free to be exchanged. Besides, you can also write
it as `(reduction-sink + 0)`, but now you see how to write your
own sinks.

Generally, pipey computation occurs when you `connect` a `Sink` and 
a `Source`. Sources generate (possibly infinite) streams of data and 
Sinks consume it producing some kind of output. You can customize your
Sources or Sinks by affixing a Conduit. For instance, the `naturals-source`
counts upward forever, let's change it to a `range-source` which stops
after generating `n` numbers.

```Clojure
(defn range-source [n]
  (left-fuse (naturals-source) (take-conduit n)))
```

or build a summation Sink that only sums positive numbers

```Clojure
(defn sum+sink []
  (right-fuse (filter-conduit pos?) (reduction-sink + 0)))
```

or even a Conduit which passes the first `n` positive numbers

```Clojure
(defn take-pos-conduit [n]
  (center-fuse (filter-conduit pos?) (take-conduit n)))
```

## Disclaimer

**Everything will change soon.** Don't trust the API until version 1.0.0. You have been warned.

## License

Copyright (C) 2012 Joseph Abrahamson

Distributed under the Eclipse Public License, the same as Clojure.
