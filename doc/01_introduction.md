## 1. Introduction

dbee is a library that provides a convenient API for executing
[HoneySQL](https://github.com/jkk/honeysql) queries.

It covers two primary use cases:
1. Augment the java.jdbc API with functions that operate on HoneySQL queries
2. Generate a local database API with a connection pool wrapping the dbee API

In your application, you may use either use case (or both if really necessary).
The use case you select will affect how you configure dbee. However, both use
cases provide the same query functionality.

The first use case will operate very much like java.jdbc, and you must provide
your own connection management. The second use case provides an opinionated
implementation inside your project. The local API contains an HikariCP
connection pool and generates functions that incorporate the connection pool.
The implementation is designed to deal with all the boilerplate in creating the
connection pool and managing connections so you can focus on your application.
The local database API is generated using `dbee.core/defdb`.

The local API generated is exactly the same as the `dbee.core` API with the
exception of having to pass database `conn`s as parameters.

The API includes functions for getting a collection of records, getting a single
record, inserting records, updating records, and deleting records. The functions
accept and return maps as representations of rows in the database.

The design of the local database API is inspired by
[Ecto.Repo](https://hexdocs.pm/ecto/Ecto.Repo.html).
