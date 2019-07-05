## 2. Configuration

### Project Dependencies

dbee is distributed through [Clojars](https://clojars.org) with the identifier
`tessellator/dbee`. You can find the version information for the latest release
at https://clojars.org/tessellator/dbee.

dbee does not make any assumptions about which database you are using. In order
to connect to a database, you must provide a database adapter. You can find a
list of adapters in the
[hikari-cp docs](https://github.com/tomekw/hikari-cp#adapters-and-corresponding-datasource-class-names).

dbee requires Clojure 1.9 or later.

### Logging

dbee uses `tools.logging` internally for logging debug and warn messages. In
order to get these messages, you will need to configuration a specific logging
implementation. Refer to the
[tools.logging README](https://github.com/clojure/tools.logging) for more
information.

As an example, you could include the following in your project dependencies:

```
[org.slf4j/slf4j-log4j12 <VERSION>]
```

If you do not configure logging, you will see some SLF4J warnings output and
will not receive logs from dbee.

### Base API

If you want to use the base dbee API, you must configure your database
connection in the same way as java.jdbc. Some example usages can be found in the
[java.jdbc docs](https://github.com/clojure/java.jdbc#example-usage).

Following is an example using the base API at the REPL.

```clojure
> (def pg-db {:dbtype "postgresql"
              :dbname "mydb"
              :host "localhost"
              :user "postgres"
              :password ""})
;; => #'user/pg-db

> (dbee.core/all pg-db {:from [:users] :select [:*])
;; => ({:id 1 :name "John" :username "jdoe"} ...)

> (dbee.core/get pg-db :users 1)
;; => {:id 1 :name "John" :username "jdoe"}
```

### Local Database API

As discussed in the [introduction](/doc/01_introduction.md), one of the use
cases to create a local database API based on the functions provided in
`dbee.core`. The local API will contain a HikariCP connection pool and wrappers
around the dbee query functions. `dbee.core/defdb` generates the API in the
calling namespace.

`defdb` accepts a configuration map as its argument. The map will be evaluated
in a `delay` so you can load some the data after the application has started.
This is useful for loading credentials from somewhere outside your application.

The configuration map may include the following:

* hikari-cp configuration options

  Refer to the
  [hikari-cp docs](https://github.com/tomekw/hikari-cp#configuration-options).
  It should be noted that dbee does not make any assumptions about which
  adapter you are using, so you must provide the specific dependency in your
  project. This is the same behavior as hikari-cp.

* `:row-fn`

  A function to be executed on every query unless overridden. An example of
  how you might want to use this is to convert all keys in records returned
  from queries from snake_case to kebab-case. This value can be overridden by
  providing a different `:row-fn` function in the `opts` map of a query function.
  Defaults to `identity`.

* `:long-running-threshold`

  The number of milliseconds to use as a long-running threshold. If queries
  exceed this value, a warning will be logged that includes the query and
  the run time. This value can be overridden by providing a different value
  for `:long-running-threshold` in the `opts` map of a query function. Defaults
  to 500.

Following is a sample configuration of building a local database API with a
PostgreSQL database. The `:row-fn` will ensure only kebab-cased keys are
returned from queries (they are snake_case by default), and the
`:long-running-threshold` will log warnings for queries taking longer than 250ms
instead of the default 500ms.

```clojure
(ns myproject.db
  (:refer-clojure :exclude [get update])
  (:require [camel-snake-kebab.core :as csk]
            [camel-snake-kebab.extras :as cske]
            [dbee.core :refer [defdb]]))

(defdb {:adapter "postgresql"
        :database-name "mydb"
        :server-name "localhost"
        :username "postgres"
        :password ""
        :maximum-pool-size 10
        :row-fn (partial cske/transform-keys csk/->kebab-case)
        :long-running-threshold 250}
```

Following is an example using the local database API on the REPL. Note that the
only difference from the base dbee API is that the local database API does not
require you to provide a database connection.

```clojure
> (require '[myproject.db :as db])
;; => nil

> (dbee.core/all {:from [:users] :select [:*])
;; => ({:id 1 :name "John" :username "jdoe"} ...)

> (dbee.core/get :users 1)
;; => {:id 1 :name "John" :username "jdoe"}
```
