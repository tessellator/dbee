## 3. Queries

### Query Expansion

Many of the functions in `dbee.core` claim to expand a query and that
abbreviated forms may be used. There are three abbreviated forms, and they are
described as follows:

1. `nil` represents an empty query and is expanded to an empty map
2. A keyword  represents a table name and is expanded to a select-all query
3. A parameterless function is a query generator and is expanded to the result of
   calling the function

Note that maps are not considered abbreviated are equal to their expansion.

The expansion happens in [[dbee.core/expand-query]]. This function should be
used in query construction functions that you want to be able to accept
abbreviated forms.


### Common Options

Many of the API functions provided by dbee accept a number of common options.
Those options include the following:

* `:long-running-threshold`

  The number of milliseconds to use as a long-running threshold. If queries
  exceed this value, a warning will be logged that includes the query and
  the run time. This value will override any default or configured value.

* `:row-fn`

  A function that will be executed against every row returned from the
  database. This value will override any default or configured value.

* `:result-set-fn`

  A function that will be applied to the entire returned result set.

### Transactions

dbee provides a macro [[dbee.core/with-transaction]] for executing queries
inside of a transaction. The API is similar to that provided by java.jdbc.

Transactions are committed unless they are rolled back using
[[dbee.core/rollback]]. However, `rollback` will throw an exception if it is not
called from within a transaction. This is to help ensure correctness in
transaction code. You may use [[dbee.core/in-transaction?]] to determine whether
the current execution is occurring within a transaction.

Note that each of these functions will be generated as part of the local
database API.

**Note:** There is a subtle difference between the behavior of
`with-transaction` using the base API and the local database API. Since the
base API requires you to provide your own connection for the transaction, the
API cannot guarantee that `rollback` will rollback the _outermost_ transaction
when working with embedded transactions. However, the local database API can
make this guarantee because `with-transaction` will use the same connection as
the outermost transaction.
