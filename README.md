# pipes

<img src="https://github.com/downloads/tel/pipes/thepipe.png"
 alt="Pipes logo" title="Stylish, functional, iconic." align="right" />

Pipey stream processing for Clojure. Build yourself a reusable
streaming pipeline from highly orthogonal pieces.

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
(connect (list-source (range 50)) (take-sink 10))
; (0 1 2 3 4 5 6 7 8 9)
```

**Everything will change soon.** Don't trust the API until version 1.0.0. You have been warned.

## License

Copyright (C) 2012 Joseph Abrahamson

Distributed under the Eclipse Public License, the same as Clojure.
