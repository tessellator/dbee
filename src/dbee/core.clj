(ns dbee.core
  "Functions to query databases and a macro to generate a local database API.

  The functions in this namespace provide a java.jdbc-like API for querying
  databases using HoneySQL queries. It assumes you will manage database
  connections in your application.

  [[dbee.core/defdb]] will create an opinionated local database API based on the
  functions defined in this namespace. For configuration options and example
  usages, refer to the `defdb` docstring.

  The functions used with either use case accept a number of common options.
  Those options include the following:

  * long-running-threshold

  The number of milliseconds to use as a long-running threshold. If queries
  exceed this value, a warning will be logged that includes the query and
  the run time. This value will override any default or configured value.

  * row-fn

  A function that will be executed against every row returned from the
  database. This value will override any default or configured value.

  * result-set-fn

  A function that will be applied to the entire returned result set."
  (:refer-clojure :exclude [get update])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [hikari-cp.core :as hikari-cp]
            [honeysql.core :as sql]
            [honeysql.helpers :as sqlh]))

;; -----------------------------------------------------------------------------
;; Query helpers

(defn expand-query
  "Expands a query from an abbreviated form into a valid HoneySQL query.

  The expansion works as follows:

  map       -> returned as-is (assumed to be a query)
  nil       -> empty query
  keyword   -> select-all query with keyword as table name
  function  -> calls the function (assumed to be a generator)
  otherwise -> throws clojure.lang.ExceptionInfo
  "
  [query]
  (cond
    (map? query)     query
    (nil? query)     {}
    (keyword? query) {:from [query] :select [:*]}
    (fn? query)      (query)
    :else            (throw (ex-info "Unknown query type"
                                     {:query query
                                      :type (type query)}))))

(defn by
  "Creates or updates a query with a where clause based on the kv pairs in `m`.

  The resulting query will have an updated where clause that searches for each
  column specified by the keys in `m` to equal their associated values.

  This function will expand `query` so abbreviated forms may be used.


  Examples:

  ;; Search for all users with the name \"John\".
  (all (-> :users
           (by {:name \"John\"}))
  "
  ([m]
   (by {} m))
  ([query m]
   (reduce (fn [curr [k v]]
             (sqlh/merge-where curr [:= k v]))
           (expand-query query)
           m)))

(defn- primary-key-field [opts]
  (or (:primary-key opts) :id))

(defn- primary-key [id-or-record opts]
  (if (map? id-or-record)
    (let [field (primary-key-field opts)]
      (clojure.core/get id-or-record field))
    id-or-record))

(defn- by-primary-key [query id-or-record opts]
  (let [field (primary-key-field opts)
        value (primary-key id-or-record opts)]
    (if value
      (-> (expand-query query)
          (by {field value}))
      (throw (ex-info "No primary key found in column %s from record %s"
                      field
                      id-or-record)))))

(defn- select-query? [query]
  (contains? query :select))

(defn- ensure-select [query]
  (merge {:select [:*]} query))


;; -----------------------------------------------------------------------------
;; API

(defn- query-executor [query]
  (if (select-query? query)
    jdbc/query
    jdbc/execute!))

(defn- elapsed-ms [start stop]
  (/ (double (- stop start)) 1000000.0))

(defn- now []
  (System/nanoTime))

(def ^{:no-doc true :dynamic true} *row-fn* nil)
(def ^{:no-doc true :dynamic true} *long-running-threshold* nil)

(defn execute
  "Executes the specified query.

  This function will return a map with the following keys:

  `:query`  - the expanded HoneySQL query executed
  `:ms`     - the elapsed time during the period of this call
  `:sql`    - the raw SQL executed
  `:result` - the query result

  A warning containing the run time and query will be logged if the elapsed time
  exceeds a long-running threshold. The threshold is 500ms by default but may be
  set by providing a `:long-running-threshold` value in `opts`.

  If an exception is thrown during the execution of the query, the exception is
  logged with the elapsed time and raw SQL. The exception is re-thrown.

  This function will expand `query` so that abbreviated forms may be used.
  "
  ([conn query]
   (execute conn query {}))
  ([conn query opts]
   (let [start (now)
         opts (merge {:row-fn (or *row-fn* identity)
                      :long-running-threshold (or *long-running-threshold* 500)}
                     opts)
         query (expand-query query)
         raw-sql (sql/format query)
         exec (query-executor query)]
     (try
       (let [result (exec conn raw-sql opts)
             elapsed (elapsed-ms start (now))
             threshold (:long-running-threshold opts)]
         (log/debugf "Query completed in %.2fms\n%s" elapsed raw-sql)
         (when (> elapsed threshold)
           (log/warnf "Query completed in %.2fms, exceeding long-running threshold of %dms\n%s"
                      elapsed threshold raw-sql))
         {:query query
          :ms elapsed
          :sql raw-sql
          :result result})
       (catch Throwable t
         (let [elapsed (elapsed-ms start (now))]
           (log/errorf t "Query threw exception after %.2fms\n%s" elapsed raw-sql)
           (throw t)))))))

(defn all
  "Fetches all records from the database matching the given query.

  This function will expand `query` so abbreviated forms may be used.

  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.


  Examples:

  ;; get all users
  (all :users)

  ;; get all users with long-running threshold of 100ms
  (all :users {:long-running-threshold 100})

  ;; get all with a HoneySQL query
  (all {:from [:users] :select [:id :name :username]})
  "
  ([conn query]
   (all conn query {}))
  ([conn query opts]
   (:result (execute conn query opts))))

(defn- -one [conn query opts]
  (let [query (expand-query query)
        {:keys [sql result] :as resp} (execute conn query opts)]
    (when (not-empty (rest result))
      (throw (ex-info (format "More than one result found for query: %s" sql)
                      {:query query})))
    resp))

(defn one
  "Fetches a single result from the database matching the given query.

  Throws an exception if more than one matching record is found.

  Returns `nil` if no matching record is found. You can use `dbee.core/one!` to
  throw an exception instead if desired.

  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.


  See also: [[dbee.core/get]] and [[dbee.core/get-by]]


  Examples:

  ;; Get the one user record where the username is \"jdoe\"
  (one {:from [:users] :where [:= :username \"jdoe\"]})
  "
  ([conn query]
   (one conn query {}))
  ([conn query opts]
   (let [{result :result} (-one conn query opts)]
     (first result))))

(defn one!
  "Fetches a single result from the database matching the given query.

  Throws an exception if more than one matching record is found.

  Unlike `dbee.core/one`, this function will also throw an exception if no
  matching record is found.

  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.


  See also: [[dbee.core/get!]] and [[dbee.core/get-by!]]


  Examples:

  ;; Get the one user record where the username is \"jdoe\"
  (one {:from [:users] :where [:= :username \"jdoe\"]})
  "
  ([conn query]
   (one! conn query {}))
  ([conn query opts]
   (let [{:keys [sql result]} (-one conn query opts)]
     (if (not-empty result)
       (first result)
       (throw (ex-info (format "No results found for query: %s" sql)
                       {:query query}))))))

(defn get
  "Fetches a single record from the database with the specified primary key.

  The primary key is assumed to be a column named `id`. This can be overridden
  by providing the name of the column as the value associated with
  `:primary-key` in `opts`.

  Throws an exception if more than one matching record is found.

  Returns `nil` if no matching record is found. You can use `dbee.core/get!` to
  throw an exception instead if desired.

  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.

  This function will expand `query` so abbreviated forms may be used.


  See also: [[dbee.core/one]] and [[dbee.core/get-by]]


  Examples:

  ;; Get the record from the users table with id 2
  (get :users 2)

  ;; Get the record from the users table with user_id 2
  (get :users 2 {:primary-key :user_id})
  "
  ([conn query id]
   (get conn query id {}))
  ([conn query id opts]
   (one conn (by-primary-key query id opts))))

(defn get!
  "Fetches a single record from the database with the specified primary key.

  The primary key is assumed to be a column named `id`. This can be overridden
  by providing the name of the column as the value associated with
  `:primary-key` in `opts`.

  Throws an exception if more than one matching record is found.

  Unlike `dbee.core/get`, this function will also throw an exception if no
  matching record is found.

  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.

  This function will expand `query` so abbreviated forms may be used.


  See also: [[dbee.core/one!]] and [[dbee.core/get-by!]]


  Examples:

  ;; Get the record from the users table with id 2
  (get! :users 2)

  ;; Get the record from the users table with user_id 2
  (get! :users 2 {:primary-key :user_id})
  "
  ([conn query id]
   (get! conn query id {}))
  ([conn query id opts]
   (one! conn (by-primary-key query id opts))))

(defn get-by
  "Fetches a single record using `query` and the kv-pairs in `m`.

  `m` is expanded into a set of where clauses where the column with each key is
  equal to the associated value.

  Throws an exception if more than one matching record is found.

  Returns `nil` if no matching record is found. You can use `dbee.core/get-by!`
  to throw an exception instead if desired.

  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.

  This function will expand `query` so abbreviated forms may be used.


  See also: [[dbee.core/one]] and [[dbee.core/get]]


  Examples:

  ;; Get the record from the users table where the name is \"John\" and the
  ;; username is \"jdoe\"
  (get-by :users {:name \"John\" :username \"jdoe\"})
  "
  ([conn query m]
   (get-by conn query m {}))
  ([conn query m opts]
   (one conn (by query m) opts)))

(defn get-by!
  "Fetches a single record using `query` and the kv-pairs in `m`.

  `m` is expanded into a set of where clauses where the column with each key is
  equal to the associated value.

  Throws an exception if more than one matching record is found.

  Unlike `dbee.core/get-by!`, this function will also throw an exception if no
  matching record is found.

  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.

  This function will expand `query` so abbreviated forms may be used.


  See also: [[dbee.core/one!]] and [[dbee.core/get!]]


  Examples:

  ;; Get the record from the users table where the name is \"John\" and the
  ;; username is \"jdoe\"
  (get-by! :users {:name \"John\" :username \"jdoe\"})
  "
  ([conn query m]
   (get-by! conn query m {}))
  ([conn query m opts]
   (one! conn (by query m))))

(def ^:private aggregate-fns #{:avg :count :max :min :sum})

(defn- aggregate-selector [aggregate field]
  {:pre [(contains? aggregate-fns aggregate)]}
  (keyword (str "%" (name aggregate) "." (name field))))

(defn aggregate
  "Executes the specified `aggregate` function on the `field` from `query`.

  Supported aggregate functions include `:avg`, `:count`, `:max`, `:min`, and
  `sum`.

  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.

  This function will expand `query` so abbreviated forms may be used.


  Examples:

  ;; Get the record count of the users table:
  (aggregate :users :count :id)
  "
  [conn query aggregate field]
  (let [selector (aggregate-selector aggregate field)
        query (-> (expand-query query)
                  (sqlh/select selector))]
    (-> (one conn query)
        (clojure.core/get aggregate))))

(defn delete
  "Deletes `id-or-record` from `table` and returns affected row count.

  The primary key of a record is assumed to be named `id`. This can be
  overridden by providing the name of the column as the value associated with
  `:primary-key` in `opts`.

  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.


  Examples:

  ;; Delete the user with id = 1
  (delete :users 1)

  ;; Delete the user specified by the record, which has id = 1
  (delete :users {:id 1 :name \"John\" :username \"jdoe\"})

  ;; Delete user the user with :user_id = 1
  (delete :users 1 {:primary-key :user_id})
  "
  ([conn table id-or-record]
   (delete conn table id-or-record {}))
  ([conn table id-or-record opts]
   (one conn (-> (sqlh/delete-from table)
                 (by-primary-key id-or-record opts)))))

(defn delete-all
  "Deletes records from `table` matching `query` and returns affected row count.


  Examples:

  ;; delete all users with id > 15
  (delete-all :users {:where [:> :id 15]})
  "
  ([conn table query]
   (delete-all conn table query {}))
  ([conn table query opts]
   (one conn (sqlh/delete-from (expand-query query) table) opts)))

(defn exists?
  "Returns a value indicating whether any records match `query`.

  For performance reasons, the query is executed and limited to returning one
  result.

  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.

  This function will expand `query` so abbreviated forms may be used.


  Examples:

  ;; Are there any users with the name John?
  (exists? (by :users {:name \"John\"}))
  "
  ([conn query]
   (exists? conn query {}))
  ([conn query opts]
   (let [query (-> (expand-query query)
                   (sqlh/limit 1))]
     (boolean (one conn query opts)))))

(defn insert
  "Inserts `record` into `table` and returns the resulting record.

  Each key-value pair in `record` will be treated as a column name and the
  associated value.

  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.


  Examples:

  (insert :users {:name \"John\" :username \"jdoe\"})
  "
  ([conn table record]
   (insert conn table record {}))
  ([conn table record opts]
   (let [data-seq (seq record)
         query {:insert-into table
                :columns (mapv first data-seq)
                :values [(mapv second data-seq)]}
         opts (merge {:return-keys true} opts)
         {result :result} (execute conn query opts)]
     result)))

(defn- all-columns [records]
  (distinct (mapcat keys records)))

(defn- build-row [columns record]
  (mapv #(clojure.core/get record %) columns))

(defn insert-all
  "Inserts `records` into `table` and returns affected row count.

  This function will determine which columns to use for insertion based on the
  distinct collection of keys from all of the `records` unless other specified.

  A `:columns` value may be provided in `opts` that is a sequence of column
  names to be used for update. Providing `:columns` will override the
  calculation of the distinct collection of keys as previously described.


  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.

  Examples:

  (insert-all :users [{:name \"John\" :username \"jdoe\"}
                      {:name \"Jane\" :last_name \"Doe\"}])
  "
  ([conn table records]
   (insert-all conn table records {}))
  ([conn table records opts]
   (let [columns (or (:columns opts)
                     (all-columns records))
         values (mapv #(build-row columns %) records)
         query {:insert-into table
                :columns columns
                :values values}
         {result :result} (execute conn query opts)]
     (first result))))

(defn update
  "Updates `record` in `table` and returns the updated record.

  Each key-value pair in the record except the one representing the primary key
  is used to set new values using the key as the column name and the associated
  value as the new value for that column. Note that only the keys present in
  `record` will be updated.

  The primary key of a record is assumed to be named `id`. This can be
  overridden by providing the name of the column as the value associated with
  `:primary-key` in `opts`. Throws an exception if the primary key cannot be
  found in the record.

  `opts` is an optional map that may contain any of the shared options. Refer to
  the `dbee.core` namespace docstring for information about shared options.


  Examples:

  ;; Assume we have a record {:id 1 :name \"John\" :username \"jdoe\"}
  ;; Update John's name to Jane and leave the username as jdoe
  (update :users {:id 1 :name \"Jane\"})

  ;; Update John's name to Jane with the primary key being on the user_id column
  (update :users {:user_id 1 :name \"Jane\"} {:primary-key :user_id})
  "
  ([conn table record]
   (update conn table record {}))
  ([conn table record opts]
   (let [field (primary-key-field opts)
         key (primary-key record opts)
         query (-> (sqlh/update table)
                   (sqlh/where [:= field key])
                   (sqlh/sset (dissoc record field)))
         opts (merge {:return-keys true} opts)]
     (if (nil? key)
       (throw (ex-info (format "Value for primary key %s not found." field)
                       {:record record
                        :primary-key field}))
       (:result (execute conn query opts))))))


;; -----------------------------------------------------------------------------
;; Transaction processing

(def ^{:no-doc true :dynamic true} *tx-conn* nil)

(defn in-transaction?
  "Indicates whether current execution is inside a transaction."
  []
  (boolean *tx-conn*))

(defn rollback
  "Marks the transaction for rollback.

  Throws an exception if the current execution is not inside a transaction."
  []
  (if *tx-conn*
    (jdbc/db-set-rollback-only! *tx-conn*)
    (throw (ex-info "Rollback is only applicable inside a transaction" {}))))

(defmacro with-transaction
  "Executes `body` within the context of a transaction created with `conn`."
  [conn & body]
  `(jdbc/with-db-transaction [conn# ~conn]
     (binding [*tx-conn* conn#]
       ~@body)))


;; -----------------------------------------------------------------------------
;; Unpacking

(defn- unpack-api-fn
  "Unpacks a dbee API function into a target namespace.

  This function takes a var containing a function and creates a wrapper function
  in the target namespace. The wrapper function will use the *tx-conn* or
  datasource var defined in that namespace to get the conn value to provide to
  the `dbee.core` function.

  The wrapper works for all arities, though it does assume that `conn` is the
  first argument for every arity."
  [f]
  (let [{:keys [name doc arglists]} (meta f)]
    `(defn ~(symbol name) ~doc
       ~@(for [[conn & args] arglists]
           `(~(vec args)
             (binding [*row-fn* (:row-fn @~'dbee-config)
                       *long-running-threshold* (:long-running-threshold @~'dbee-config)]
               (if *tx-conn*
                 (~(var-get f) *tx-conn* ~@args)
                 (jdbc/with-db-connection [conn# {:datasource (deref ~'datasource)}]
                   (~(var-get f) conn# ~@args)))))))))

(defn- unpack-tx-fn
  "Unpacks a dbee transaction function into a target namespace."
  [f]
  (let [{:keys [name doc arglists]} (meta f)
        args (first arglists)]
    `(defn ~(symbol name) ~doc ~args
       (~(var-get f) ~@args))))

(defmacro defdb
  "Unpacks an opinionated API implementation into the calling namespace.

  This macro will create a database connection pool in the calling namespace and
  create functions that wrap the database functions in `dbee.core` with the
  connection pool.

  If using this macro, it is very possible that the only reference to the dbee
  library will be to call this macro; everything else can use the unpacked
  functions defined in the calling namespace.

  The `config` value will be wrapped in a delay, so you can use functions to
  populate the configuration map without worry of them being executed at compile
  time.

  Because this will unpack functions with the names `get` and `update`, you will
  likely want to include the following line of code in your ns definition:

  `(:refer-clojure :exclude [get update])`


  Configuration options include the following:

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
    providing a different `:row-fn` function in the opts map of a query function.

    Default: `identity`

  * `:long-running-threshold`
    The number of milliseconds to use as a long-running threshold. If queries
    exceed this value, a warning will be logged that includes the query and
    the run time. This value can be overridden by providing a different value
    for `:long-running-threshold` in the opts map of a query function.

    Default: 500


  Examples:

  ;; Simple configuration
  (defdb {:adapter \"postgresql\"
          :database-name \"my_database\"
          :server-name \"localhost\"
          :username \"postgres\"
          :password \"\"
          :maximum-pool-size 10})

  ;; Configuration happens at runtime, so the following will load the
  ;; username and password when your application is running (not at compile
  ;; time)
  (defdb {:adapter \"postgresql\"
          :database-name \"my_database\"
          :server-name \"localhost\"
          :username (load-my-username)
          :password (load-my-password)
          :maximum-pool-size 10})

  ;; Configuration with queries returning kebab-case by default (using the
  ;; camel-snake-kebab library) and with a shorter long-running-threshold
  (defdb {:adapter \"postgresql\"
          :database-name \"my_database\"
          :server-name \"localhost\"
          :username \"postgres\"
          :password \"\"
          :maximum-pool-size 10
          :row-fn (partial cske/transform-keys csk/->kebab-case)
          :long-running-threshold 100})
  "
  [config]
  `(do

     (def ~'dbee-config (delay ~config))

     (defonce ~'datasource
       (delay (-> @~'dbee-config
                  (dissoc :row-fn :long-running-threshold)
                  (hikari-cp/make-datasource))))

     (let [target# (str *ns*)]
       (defmacro ~'with-transaction
         "Executes `body` within a transaction.

  The transaction will be created if necessary using the datasource var
  in this namespace, if necessary. If already running within a
  transaction, the existing transaction is used.


  See also: [[dbee.core/with-transaction]]"
         [& ~'body]
         `(if *tx-conn*
            (do
              ~@~'body)
            (dbee.core/with-transaction
              {:datasource (-> (symbol ~target# "datasource")
                               resolve
                               deref
                               deref)}
              ~@~'body))))

     ~@(map unpack-tx-fn [#'in-transaction?
                          #'rollback])

     ~@(map unpack-api-fn [#'aggregate
                           #'all
                           #'delete
                           #'delete-all
                           #'execute
                           #'exists?
                           #'get
                           #'get!
                           #'get-by
                           #'get-by!
                           #'insert
                           #'insert-all
                           #'one
                           #'one!
                           #'update])))
