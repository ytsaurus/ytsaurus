# Query Tracker

Query Tracker is a {{product-name}} component that allows working with the system using human-readable queries in SQL-like languages. The user sends queries to Query Tracker, they are executed by the engine, and the execution result is returned to the user.

With Query Tracker, you can:

+ Send ad hoc queries for execution.
+ Track query execution.
+ Save query results.
+ View query history.

[Example of working with Query Tracker](#example).

Queries are defined by engine and text. The engine controls the execution of the query. The query text is engine-specific.

In this way, you can process data using different languages and runtime environments whilst honoring the guarantees and performance targets.

## Engines {#engines}

Currently supported execution engines include:

+ [YT QL](../../user-guide/dynamic-tables/dyn-query-language.md)
   + A query language built in YT. Only supports dynamic tables.
+ [YQL](../../yql/index.md)
   + Executes the query on YQL agents, which break the query into individual YT operations (map, reduce, ...), start them, then retrieve and return the result.
+ [CHYT](../../user-guide/data-processing/chyt/about-chyt.md)
   + Executes the query on a clique.
+ [SPYT](../../user-guide/data-processing/spyt/overview.md)
   + Executes the query on the Spark cluster.

## API {#api}

All operations accept the optional `query_tracker_stage` parameter that can be used to select the Query Tracker installation where you want your queries to be run. The default value is `production`.

### start_query {#start-query}

Sends the query for execution. Returns the query ID.

Required parameters:

+ `engine`: Query execution engine. Supported values are `ql`, `yql`, `chyt`, and `spyt`.
+ `query`: Query text.

Optional parameters:

+ `files`: List of files for the query in YSON format.
+ `settings`: Additional query parameters in YSON format.
   + For [CHYT](../../user-guide/data-processing/chyt/about-chyt.md), you must set a clique alias using the `clique` parameter. Default value is the public clique.
   + For [SPYT](../../user-guide/data-processing/spyt/overview.md), you must set the housekeeping directory of an existing Spark cluster using the `discovery_path` parameter.
+ `draft`: Used to mark draft queries. These queries are terminated automatically without execution.
+ `annotations`: Arbitrary annotations to the query. They can make it easier to search for queries. Specified in YSON format.
+ `access_control_object`: Name of the object at `//sys/access_control_object_namespaces/queries/` that controls access to the query for other users.

Example: `start_query(engine="yql", query="SELECT * FROM my_table", access_control_object="my_aco")`

### abort_query {#abort-query}

Aborts query execution. Doesn't return anything.

Required parameters:

+ `query_id`: Query ID.

Optional parameters:

+ `message`: Abort message.

Example: `abort_query(query_id="my_query_id")`.

### get_query_result {#get-query-result}

Returns meta information about query execution results.

Required parameters:

+ `query_id`: Query ID.
+ `result_index`: Result ID.

Example: `get_query_result(query_id="my_query_id", result_index=0)`.

### read_query_result {#read-query-result}

Returns the query results.

Required parameters:

+ `query_id`: Query ID.
+ `result_index`: Result ID.

Optional parameters:

+ `columns`: List of columns to read.
+ `lower_row_index`: First row from which to start reading the results.
+ `upper_row_index`: Last row at which to stop reading the results.

Example: `read_query_result(query_id="my_query_id", result_index=0)`.

### get_query {#get-query}

Returns information about the query.

Required parameters:

+ `query_id`: Query ID.

Optional parameters:

+ `attributes`: Filters the returned attributes.
+ `timestamp`: Returned query information will be consistent with the system as at the specified point in time.

Example: `get_query(query_id="my_query_id")`.

### list_queries {#list-queries}

Gets a list of queries that match the set filters.

Optional parameters:

+ `from_time`: Lower threshold for the query start time.
+ `to_time`: Upper threshold for the query start time.
+ `cursor_direction`: Direction in which to sort queries by start time.
+ `cursor_time`: Cursor stop time. Only applicable if `cursor_direction` is specified.
+ `user_filter`: Filter by query creator.
+ `state_filter`: Filter by query state.
+ `engine_filter`: Filter by query engine.
+ `substr_filter`: Filter by query ID, annotations, and [access control object](#access-control).
+ `limit`: Number of items to return. The default value is 100.
+ `attributes`: Filters the returned attributes.

Example: `list_queries()`.

### alter_query {#alter-query}

Modifies the query. Doesn't return anything.

Required parameters:

+ `query_id`: Query ID.

Optional parameters:

+ `annotations`: New annotations for the query.
+ `access_control_object`: New access control object for the query.

Example: `alter_query(query_id="my_query_id", access_control_object="my_new_aco")`.

## Access control {#access-control}

To manage access to queries and their results, the query can store an optional `access_control_object` string that points to `//sys/access_control_object_namespace/queries/[access_control_object]`.

An Access Control Object (ACO) is an object with the `@principal_acl` attribute. It sets access rules in the same manner as `@acl` does for Cypress nodes. For more information, see [Access control](../../user-guide/storage/access-control.md).

You can create an ACO through the user interface or by calling the [create](../../user-guide/storage/cypress-example.md#create) command: `yt create access_control_object --attr '{namespace=queries;name=my_aco}'`.

All APIs use ACOs to verify access:

+ `start_query` checks whether the passed ACO exists.
+ `alter_query` checks whether the query ACO grants the `Administer` permission to the user and whether the passed ACO exists.
+ `get_query` checks whether the query ACO grants the `Use` permission to the user.
+ `list_queries` checks whether the query ACO grants the `Use` permission to the user.
+ `get_query_result` checks whether the query ACO grants the `Read` permission to the user.
+ `read_query_result` checks whether the query ACO grants the `Read` permission to the user.
+ `abort_query` checks whether the query ACO grants the `Administer` permission to the user.

A few things to keep in mind:

+ The query creator always has access to their queries.
+ If a query doesn't have an ACO, the default `nobody` ACO is used for verification.

## Example {#example}

A typical usage scenario for Query Tracker is as follows:

1. Sending a query for execution.
   + `start_query(engine="yql", query="SELECT * from //home/me/my_table", access_control_object="my_team_aco")`.
2. Getting a list of our available queries.
   + `list_queries()`.
3. Getting information about our query.
   + `get_query(query_id="my_query_id")`.
4. Waiting for the query to complete.
   + `get_query(query_id="my_query_id").state == "completed"`.
5. Getting meta information about the results (result size, table schema).
   + `get_query_result(query_id="my_query_id", result_index=0)`.
6. Reading the result.
   + `read_query_result(query_id="my_query_id", result_index=0)`.
7. Changing the ACO of the query to make it accessible to everyone.
   + `alter_query(query_id="my_query_id", access_control_object="share_with_everyone_aco")`.
