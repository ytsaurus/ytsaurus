# Usage examples

You need to get a [{{product-name}} token](../../../user-guide/storage/auth.md) before running the examples.

Run the examples on `Linux`, with no arguments.

{% if audience == "internal" %}
```bash
cd /path/to/arcadia
cd yt/cpp/mapreduce/examples/tutorial/simple_map_tnode
ya make
 ./simple_map_tnode
```
{% endif %}

We recommend setting the log level to `INFO` or `DEBUG` to make programs output links to the web interface where you can monitor the execution.

The log level is set using an environment variable with this command (more info is available in the [documentation](../../../api/cpp/description.md#logging)):

```bash
export YT_LOG_LEVEL=INFO
```

For production processes, we recommend saving the most detailed `DEBUG` logs. If any problem occurs, these logs will help you find out what was happening.

## Basic level

### Simple Map (TNode version)

Suppose there is a table with logins. You want to make a table with email addresses, deriving them from the login: `email = login + "@domain"`.

To do this, you can use a simple mapper.

The example is located at [yt/cpp/mapreduce/examples/tutorial/simple_map_tnode]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/simple_map_tnode).

```c++
 'yt/cpp/mapreduce/tutorial/simple_map_tnode/main.cpp'
```

### Simple Map (Protobuf version)

If you know the structure of the table, you may use the [Protobuf](../../../api/cpp/protobuf.md) format, which:

- Is a bit faster.
- Prevents such errors as typos in column names or usage of wrong type for column.

The example is located at [yt/cpp/mapreduce/examples/tutorial/simple_map_protobuf]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/simple_map_protobuf).

{% code '/yt/cpp/mapreduce/examples/tutorial/simple_map_protobuf/data.proto' lang='protobuf' %}

{% code '/yt/cpp/mapreduce/examples/tutorial/simple_map_protobuf/main.cpp' lang='c++' %}

### Simple Map with Protobuf using lambda expressions in C++

Some simple operations can be represented as C++ lambda expressions with an empty capture-list (not capturing variables) using the [yt/cpp/mapreduce/library/lambda](../../../api/cpp/lambda.md) library.

The previous example can be rewritten as follows (`data.proto` is the same):

([yt/cpp/mapreduce/examples/tutorial/simple_map_lambda]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/simple_map_lambda))

{% code '/yt/cpp/mapreduce/examples/tutorial/simple_map_lambda/main.cpp' lang='c++' %}

### Sorting a table and a simple Reduce operation

Given the same input table, we can count how many times each name occurs. To do this we need Reduce operation. Reduce operation requires input table to be sorted, so we run the Sort operation before launching Reduce.

The example is located at [yt/cpp/mapreduce/examples/tutorial/simple_reduce_tnode]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/simple_reduce_tnode).

{% code '/yt/cpp/mapreduce/examples/tutorial/simple_reduce_tnode/main.cpp' lang='c++' %}

### Composite types and Protobuf format (using Reduce as an example)

Let's say we have a table with a set of records on links occurring in web documents. This table has the following columns:

- `DocTitle`: is a string containing the document name.
- `Link` is a struct containing following fields:
  - `Host`: host part of the link url.
  - `Port`: port part of the link url.
  - `Path`: path part of the link url.
- `OccurenceCount`: how many times link appears in the document.

Our input table is sorted by `DocTitle` column. We want to aggregate information about each document into single row of output table.

Let's describe protobuf messages of the following form:

{% code '/yt/cpp/mapreduce/examples/tutorial/protobuf_complex_types/data.proto' lang='c++' %}

`TLinkEntry` corresponds to the input table row, and `TDoc` corresponds to the output table row.

The `TDoc` fields have the following semantics:
- `Title`: Title.
- `Links`: List of links occurring in the document.
- `OccurenceCounts`: List with the number of times the given links occur (this list has same length as the `Links` list).
- `ExtraInfo`: Additional information, which in this case includes the total number of times the links occurs.

{% note warning "Attention" %}

`(NYT.field_serialization_mode)` in `TDoc` and `TLinkEntry` messages. By default, this option equals `PROTOBUF`, which means "serialize the field as a sequence of bytes". The `YT` value we set means that the corresponding nested message will be related to the composite type in the table schema. In `TDoc`, as an example, we marked the `ExtraInfo` field with the `(NYT.serialization_mode) = PROTOBUF` option (note the type of the respective field in the output table).

{% endnote %}


The [reducer](../../../user-guide/data-processing/operations/reduce.md) is straightforward.

{% code '/yt/cpp/mapreduce/examples/tutorial/protobuf_complex_types/main.cpp' lang='c++' %}

### Reduce with multiple input tables { #multiple-input-reduce-tnode }

Imagine that besides the table with users from our previous examples, there is also a second table with robots.

The following program creates a table where non-robot users are filtered out.

The example is located at [yt/cpp/mapreduce/examples/tutorial/multiple_input_reduce_tnode]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/multiple_input_reduce_tnode).

{% code '/yt/cpp/mapreduce/examples/tutorial/multiple_input_reduce_tnode/main.cpp' lang='c++' %}

### Reading files from operations { #map-tnode-with-file }

The previous task can be solved in a different way: upload the table with robots directly to the machines running the operations and read it there as a whole in `yhash_set`.

In this case, you don't have to sort the table and can use Map instead of Reduce. This method is used for small tables up to several GB in size.

Input tables are the same as in the previous example: [with users](https://yt.yandex-team.ru/freud/#page=navigation&path=//home/dev/tutorial/staff_unsorted&offsetMode=row) and [with robots](https://yt.yandex-team.ru/freud/#page=navigation&path=//home/dev/tutorial/is_robot_unsorted&offsetMode=row).

The following program creates a table where non-robot users are filtered out.

The example is located at [yt/cpp/mapreduce/examples/tutorial/map_tnode_with_file]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/map_tnode_with_file).

{% code '/yt/cpp/mapreduce/examples/tutorial/map_tnode_with_file/main.cpp' lang='c++' %}

### Reduce with multiple input and output tables

Here we have a similar task as in the previous example, except this time we'll write two output tables at once: with human users and with robots.

The example is located at [yt/cpp/mapreduce/examples/tutorial/multiple_input_multiple_output_reduce_tnode]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/multiple_input_multiple_output_reduce_tnode).

{% code '/yt/cpp/mapreduce/examples/tutorial/multiple_input_multiple_output_reduce_tnode/main.cpp' lang='c++' %}

### Reduce with multiple input and output tables (Protobuf version)

To create separate tables for human users and robots, we need to rewrite the same reducer to protobuf. The table with human users will contain the `login`, `email`, and `name` fields. The table with robots will contain the `login` and `uid` fields. We need to create separate protobuf message types for these tables.

The example is located at [yt/cpp/mapreduce/examples/tutorial/multiple_input_multiple_output_reduce_protobuf]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/multiple_input_multiple_output_reduce_protobuf).

{% code '/yt/cpp/mapreduce/examples/tutorial/multiple_input_multiple_output_reduce_protobuf/data.proto' lang='c++' %}

{% code '/yt/cpp/mapreduce/examples/tutorial/multiple_input_multiple_output_reduce_protobuf/main.cpp' lang='c++' %}

### Reading and writing tables

When writing a table you can overwrite existing data or append your data to the table. When reading a table you can read the entire table or ranges by row number or key. Learn more about [reading and writing data](../../../user-guide/storage/formats.md#table_data) and [tables](../../../user-guide/storage/objects.md#tables).

The example is located at [yt/cpp/mapreduce/examples/tutorial/table_read_write_tnode]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/table_read_write_tnode).

{% code '/yt/cpp/mapreduce/examples/tutorial/table_read_write_tnode/main.cpp' lang='c++' %}

### Transferring states to jobs

Sometimes you may need to write an operation that takes certain arguments. For example, write a program that filters the source table by user name.

The example is located at [yt/cpp/mapreduce/examples/tutorial/stateful_map_tnode]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/stateful_map_tnode).

{% code '/yt/cpp/mapreduce/examples/tutorial/stateful_map_tnode/main.cpp' lang='c++' %}

### MapReduce operation (Protobuf version)

{{product-name}} has the [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md) merged operation, which works a bit faster than [Map](../../../user-guide/data-processing/operations/map.md) + [Sort](../../../user-guide/data-processing/operations/sort.md) + [Reduce](../../../user-guide/data-processing/operations/reduce.md). Let's return to one of our first examples and once again get the statistics of how many times a name occurs in our table with users. Before we do that, we first need to standardize the names by converting all of them to lowercase so that people with the names `ARCHIE` and `Archie` are counted as one.

The example is located at [yt/cpp/mapreduce/examples/tutorial/mapreduce_protobuf]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/mapreduce_protobuf).

{% code '/yt/cpp/mapreduce/examples/tutorial/mapreduce_protobuf/main.cpp' lang='c++' %}

### MapReduce operation (version with lambda expressions)

The task is the same as in the previous example.

The example is located at [yt/cpp/mapreduce/examples/tutorial/mapreduce_lambda]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/mapreduce_lambda).

{% code '/yt/cpp/mapreduce/examples/tutorial/mapreduce_lambda/main.cpp' lang='c++' %}

### Preparing an operation in a job class { #prepare_operation }

You can set up the operation parameters by overloading the `IJob::PrepareOperation` method. For more information, see [Description](../../../api/cpp/description.md#prepare_operation).

{% code '/yt/cpp/mapreduce/examples/tutorial/prepare_operation/grepper.proto' lang='protobuf' %}

{% code '/yt/cpp/mapreduce/examples/tutorial/prepare_operation/main.cpp' lang='c++' %}

## Advanced level

### Running multiple operations in parallel

To run multiple operations in parallel, use the [TOperationTracker]({{source-root}}/yt/cpp/mapreduce/library/operation_tracker/operation_tracker.h) class.

The example is located at [yt/cpp/mapreduce/examples/tutorial/operation_tracker]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/operation_tracker).

{% code '/yt/cpp/mapreduce/examples/tutorial/operation_tracker/main.cpp' lang='c++' %}

### Reduce with enable_key_guarantee=false

Suppose you want to filter a table with URLs by regular expressions that are located in a separate table with hosts (each host has its own regular expression). And while you could run Reduce on the `host` column, real data is often arranged in such a way that some hosts have a disproportionately large number of URLs. In our example, URLs from the host `https://www.youtube.com` take up more than half of the table. The job responsible for processing such a host will take a disproportionately large amount of time (keys like `https://www.youtube.com` here are also called *monster keys*).

However, you don't need all the records with the same host to go to the same machine to solve the task. You can distribute the table with URLs among several machines: note that the host must be received along with the URL.

![](../../../../images/reduce.svg){ .center}

![](../../../../images/join_reduce.svg){ .center}

The example is located at [yt/cpp/mapreduce/examples/tutorial/join_reduce_tnode]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/join_reduce_tnode).

{% code '/yt/cpp/mapreduce/examples/tutorial/join_reduce_tnode/main.cpp' lang='c++' %}

### Batch requests

You can execute light requests (create/delete a table, check its existence, etc.) in batches. Use this method if you need to run a large amount of uniform operations. Batch requests can save a lot of time.

The example is located at [yt/cpp/mapreduce/examples/tutorial/batch_request]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/batch_request).

{% code '/yt/cpp/mapreduce/examples/tutorial/batch_request/main.cpp' lang='c++' %}

### Delivering a table dump to a job as a file

In {{product-name}}, you can deliver files to jobs (for example, as was described [earlier](#map-tnode-with-file)). A less popular method is delivering a table dump to a job. This can prove useful when a job requires a map stored in a small (a few gigabytes) table. However, don't use this approach to load large tables: if you have a table that is dozens of gigabytes in size, it won't fit on a single node of the cluster where the jobs are running.

The example is located at [yt/cpp/mapreduce/examples/tutorial/pass_table_as_file]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/pass_table_as_file).

{% code '/yt/cpp/mapreduce/examples/tutorial/pass_table_as_file/main.cpp' lang='c++' %}

### Writing and retrieving job statistics

A running operation gathers a lot of statistics (see the [documentation](../../../user-guide/problems/jobstatistics.md)).

Example of a simple Map with statistics collection: [yt/cpp/mapreduce/examples/tutorial/job_statistics]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/job_statistics):

{% code '/yt/cpp/mapreduce/examples/tutorial/job_statistics/main.cpp' lang='c++' %}

### Basic work with dynamic tables

{{product-name}} supports [dynamic tables](../../../user-guide/dynamic-tables/overview.md). You can perform basic operations with such tables by using the C++ wrapper.

{% note warning "Attention" %}

Unlike the other examples, you will have to take additional steps to run this one:

- By default, users don't have the permissions to create dynamic tables, so you first need to get permissions to create and mount dynamic tables on some cluster.
- When running the example, pass the cluster name and the path to the test table (it must not exist). For more information, see the comments in the program text.

{% endnote %}

The example is located at [yt/cpp/mapreduce/examples/tutorial/dyntable_get_insert]({{source-root}}/yt/cpp/mapreduce/examples/tutorial/dyntable_get_insert).

{% code '/yt/cpp/mapreduce/examples/tutorial/dyntable_get_insert/main.cpp' lang='c++' %}

