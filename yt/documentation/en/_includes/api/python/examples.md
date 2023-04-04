# Python API with examples

Before using the examples, read the [instructions](../../../user-guide/storage/auth.md) for obtaining the token.

We recommend running all programs on `Linux`.

## Basic level { #base }

The modern method of working with the Python API is typed.
Data stored in tables and processed by operations is represented in the code by [classes](../../../api/python/userdoc.md#dataclass) with typed fields (similar to [dataclasses](https://docs.python.org/3/library/dataclasses.html)). An unrecommended (but sometimes unavoidable, especially when working with old tables) method is untyped when table rows are represented by dicts.
This method is much slower, fraught with errors, and inconvenient when working with composite types. Therefore, this section of the documentation provides examples of working with the typed API and the untyped examples can be found in the [corresponding section](#untyped_tutorial).

### Reading and writing tables { #read_write }

{{product-name}} enables you to write data to tables, as well as to append data to the end of existing tables. Several modes are available for reading tables: reading the entire table, reading individual ranges by row number or key.

The example is located at [yt/python/examples/table_read_write_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/table_read_write_typed).

### Simple map { #simple_map }

Suppose there is a table with the login, name, and uid columns at //home/dev/tutorial/staff_unsorted.
You need to make a table with email addresses, calculating them from the login: `email = login + "@ytsaurus.tech"`.

A simple mapper is suitable for this job.

The example is located at [yt/python/examples/simple_map_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/simple_map_typed)

### Sorting a table and a simple reduce operation { #sort_and_reduce }

Using the same table as in the previous example, you can calculate the statistics: how many times you can see this or that name or which is the longest username among all people with the same name. A Reduce operation is well suited for this job. Since the Reduce operation can only be run on sorted tables, you must first sort the original table.

The example is located at [yt/python/examples/simple_reduce_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/simple_reduce_typed).

### Reduce with multiple input tables { #reduce_multiple_output }

Suppose there is another table besides the table with users and it records which user is a service one. The is_robot field can be true or false.

The following program generates a table in which only service users remain.

The example is located at [yt/python/examples/multiple_input_reduce_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/multiple_input_reduce_typed).

### Reduce with multiple input and output tables { #reduce_multiple_input_output }

This example repeats the previous one, with the difference that two output tables will be written to at once, both with human users and service users.

The example is located at [yt/python/examples/multiple_input_multiple_output_reduce_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/multiple_input_multiple_output_reduce_typed).

### Table schemas { #table_schema }

All tables in {{product-name}} have a [schema](../../../api/python/userdoc.md#table_schema).
The code demonstrates examples of working with the table schema.

The example is located at [yt/python/examples/table_schema_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/table_schema_typed).

### MapReduce { #map_reduce }

{{product-name}} implements the MapReduce merge operation that works faster than Map + Sort + Reduce. Let's try to use the table with users once again to calculate the statistics of how many times you can see this or that name. Before calculating the statistics, we will normalize the names by converting them to lowercase so that people with the names `ARCHIE` and `Archie` are merged in our statistics.

The example is located at [yt/python/examples/map_reduce_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/map_reduce_typed).

### MapReduce with multiple intermediate tables { #map_reduce_multiple_intermediate_streams }

The intermediate data (between the map and reduce stages) in a MapReduce operation can "flow" in multiple streams and have different types. In this example, there are two tables on the operation input: the first displays uid in the name and the second contains information about the events associated with the user with that uid. The mapper selects events like "click" and sends them to one output stream, and all users to another. The reducer counts all clicks on this user.

The example is located at [yt/python/examples/map_reduce_multiple_intermediate_streams_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/map_reduce_multiple_intermediate_streams_typed).

### Decorators for job classes { #job_decorators }

You can mark functions or job classes with special decorators that change the expected interface of interaction with jobs.
Examples of decorators: `with_context`, `aggregator`, `reduce_aggregator`, `raw`, or `raw_io`. You can find a full description in the [documentation](../../../api/python/userdoc.md#python_decorators)

The example is located at [yt/python/examples/job_decorators_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/job_decorators_typed).

### Working with files on the client and in operations { #files }
For more information, see the [documentation](../../../api/python/userdoc.md#file_commands).
For more information about files in Cypress, see the [section](../../../user-guide/storage/files.md).

The example is located at [yt/python/examples/files_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/files_typed).

### Grep { #grep }
The typed API enables you to work with fairly arbitrary data using a single data class and a single operation class. As an example, let's consider the job of filtering a table based on a regular expression matching a given row field.

The example is located at [yt/python/examples/grep_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/grep_typed).

## Advanced level { #advanced }

### Batch queries { #batch_queries }

You can execute "light" queries (create/delete a table, check its existence, and others) in groups. It is reasonable to use this method if you need to perform a large number of single-type operations: batch queries can significantly save execution time.

The example is located at [yt/python/examples/batch_client](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/batch_client).

### Using the RPC { #rpc }

Using the RPC with the CLI.

```bash
yt list / --proxy cluster_name --config '{backend=rpc}'
cooked_logs
home
...
```

A similar example (full code at [yt/python/examples/simple_rpc](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/simple_rpc)) can be built in Python (in the Arcadia build):

Pay attention to the additional `PEERDIR(yt/python/client_with_rpc)` in `ya.make`.

You can work with dynamic tables using the RPC.
The example is located at [yt/python/examples/dynamic_tables_rpc](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/dynamic_tables_rpc).

### Specifying row types using prepare_operation { #prepare_operation }

In addition to using type hints, the `prepare_operation` method where all types are specified using special methods can be defined to specify Python table row types. If there is a `prepare_operation` method in the job class, the library will use the types specified inside the method and there will be no attempts to derive row types from type hints.

The example is located at [yt/python/examples/prepare_operation_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/prepare_operation_typed).

## Different examples { #misc }

### Data classes { #dataclass }

This example demonstrates the features and peculiarities of working with [data classes](../../../api/python/userdoc.md#dataclass) in more detail.

The example is located at [yt/python/examples/dataclass_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/dataclass_typed).

### Context and managing writes to output tables { #table_switches }

To select which output table to write a row to, you must use the `OutputRow` wrapper class, namely the `table_index` argument of its constructor. Having any iterator on data classes (returned from `read_table_structured()` or passed to the `__call__()` method of the job), you can use the `.with_context()` method to make an iterator on pairs `(row, context)` from it. The `context` object has the `.get_table_index()`, `.get_row_index()`, and `.get_range_index()` methods.

When writing a job class to whose `__call__()` method a separate row (for example, mappers) is passed rather than an iterator, you can add the `@yt.wrapper.with_context` decorator to the class. In this case, the `__call__()` method must take a third `context` argument (see the [documentation](../../../api/python/userdoc.md#python_decorators)).

In the reducer, mapper aggregators, and when reading tables — whenever there is an iterator on rows, use the `with_context` method of the iterator.

The example is located at [yt/python/examples/table_switches_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/table_switches_typed).

### Spec builders { #spec_builder }

Use the [spec builder](../../../api/python/userdoc.md#spec_builder) to describe the specification of operations to avoid errors.

The example is located at [yt/python/examples/spec_builder_typed](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/spec_builder_typed).

### Using gevent { #gevent }

The documentation is contained [in the separate section](../../../api/python/userdoc.md#gevent).
The example is located at [yt/python/examples/gevent](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/examples/gevent).
