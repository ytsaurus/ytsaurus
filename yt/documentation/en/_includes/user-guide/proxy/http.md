# HTTP proxy

This section demonstrates how to work with {{product-name}} via the HTTP proxy using the example of running the [Word Count](https://en.wikipedia.org/wiki/Word_count) task.

You can find a full description of all {{product-name}} commands in the [Commands](../../../api/commands.md) section.

{% note info "Note" %}

To understand the material presented in this section, you need to know the [HTTP protocol](https://ru.wikipedia.org/wiki/HTTP) structure, the principles of working via HTTP, and the basics of [data storage](../../../user-guide/storage/cypress.md) in the {{product-name}} system.

{% endnote %}


## Setup { #prepare }

The Word Count example is a typical task for any [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md) system. The general schema is as follows:

1. A [Map](../../../user-guide/data-processing/operations/map.md) operation is performed on the source text, producing a pair (word, 1) for each word.
2. The result is sorted by the first coordinate.
3. A [Reduce](../../../user-guide/data-processing/operations/reduce.md) operation that summarizes the second coordinate is performed on the first coordinate. The result is a set of pairs (word, number of word mentions).

Further, the [curl](https://en.wikipedia.org/wiki/CURL) utility is used to work over HTTP. The environment variables listed below were set for convenience.
{{% if audience == internal %}}
For more information about obtaining the token, see [Getting started](../../../user-guide/overview/try-yt.md).
{{% endif %}}

Setting the environment variables:

```bash
$ export YT_PROXY=cluster-name
$ export YT_TOKEN=`cat ~/.yt/token`
$ export YT_HOME=//tmp/yt_examples
```

To make the examples clearer, irrelevant and duplicate information has been removed from them.

## Uploading data { #upload }

Data in the {{product-name}} system is stored in [tables](../../../user-guide/storage/static-tables.md), so you need to create a table. The table contains two columns: row number and row contents. Use the [create](../../../api/commands.md#create) command to create a table.

Creating a directory and a table:

```bash
# Creating a working directory
$ curl -v -X POST "http://$YT_PROXY/api/v3/create?path=$YT_HOME&type=map_node" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> POST /api/v3/create?path=//tmp/yt_examples&type=map_node HTTP/1.1

< HTTP/1.1 200 OK
"0-3c35f-12f-8c397340"

# Creating a table for source data
$ curl -v -X POST "http://$YT_PROXY/api/v3/create?path=$YT_HOME/input&type=table" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> POST /api/v3/create?path=//tmp/yt_examples/input&type=table HTTP/1.1

< HTTP/1.1 200 OK
"0-6a7-191-8075d1f7"
```

As a result of the command execution, IDs (GUIDs) of created objects in Cypress are returned in response: `0-3c35f-12f-8c397340` and `0-6a7-191-8075d1f7`.

You need to upload the data into the table. To do this, use the [write_table](../../../api/commands.md#write_table) command. The tab-separated format is used as the data format:

- The table rows are separated by a line advance ( `\n` ).
- The columns are separated by a tab ( `\t` ).
- The column name and its contents are separated by an equal sign.

For example, a row: `lineno=1\tsize=6\tvalue=foobar` describes a row with the *lineno*, *size*, and *value* columns with the *1*, *6*, and *foobar* values, respectively. Tab characters are escaped.

Uploading data to the table:

```bash
# Downloading the text
$ wget -O - http://lib.ru/BULGAKOW/whtguard.txt | iconv -f cp1251 -t utf-8 > source.txt

# Reformatting and uploading the text
$ cat source.txt | perl -e '
    $n = 0;
    while(<>) {
      $n++; chomp; s/\t/\\t/g; print "lineno=$n\ttext=$_\n";
    }
  ' > source.tsv

$ HEAVY_YT_PROXY=$(curl -s -H "Accept: text/plain" "http://$YT_PROXY/hosts" | head -n1)
$ curl -v -L -X PUT "http://$HEAVY_YT_PROXY/api/v3/write_table?path=$YT_HOME/input" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: text/tab-separated-values" \
    -H "Transfer-Encoding: chunked" \
    -T source.tsv
> PUT /api/v3/write_table?path=//tmp/yt_examples/input HTTP/1.1
> Expect: 100-continue

< HTTP/1.1 100 Continue
< HTTP/1.1 200 OK
```

{% note info "Note" %}

Note that the input data format is also explicitly specified in this query: "text/tab-separated-values".

{% endnote %}

{% note info "Note" %}

When creating a client, the user needs to occasionally query a list of heavy proxies using the `/hosts` query. A list of heavy proxies ordered by priority is returned in the response. The proxy priority is determined dynamically and depends on its load (CPU+I/O). A good strategy is to re-query the `/hosts` list every minute or every few queries and change the current proxy to which queries are made.

{% endnote %}

Getting a list of heavy proxies:

```bash
$ curl -v -X GET "http://$YT_PROXY/hosts"
> GET /hosts HTTP/1.1

< HTTP/1.1 200 OK
< Content-Type: application/json
<
["n0008-sas.cluster-name","n0025-sas.cluster-name",...]
```

To make sure that the data is written, look at the table attributes.

Getting the table attributes:

```bash
$ curl -v -X GET "http://$YT_PROXY/api/v3/get?path=$YT_HOME/input/@" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> GET /api/v3/get?path=//tmp/yt_examples/input/@ HTTP/1.1

< HTTP/1.1 200 OK
{
  ...
  "uncompressed_data_size" : 993253,
  "compressed_size" : 542240,
  "compression_ratio" : 0.54592334480741566693,
  "row_count" : 8488,
  "sorted" : "false",
  ...
  "channels" : [ ["lineno"], ["text"] ]
}
```

{% note info "Note" %}

Don't be frightened by null values. null does not mean that there is no relevant data. It means that the given key contains a node of a special type (such as a table or a file) or that the data is lazily computable and needs to be explicitly accessed (the case above).

{% endnote %}

{% note info "Note" %}

To upload, for example, JSON, you need to add a bit more options. For more information, see [Formats](../../../user-guide/storage/formats.md#json).

{% endnote %}

Uploading data in JSON format:

```bash
$ cat test.json
{ "color": "красный", "value": "#f00" }
{ "color": "red", "value": "#f00" }

$ curl -X POST "http://$YT_PROXY/api/v3/create?path=//tmp/test-json&type=table" \
       -H "Accept: application/json" \
       -H "Authorization: OAuth $YT_TOKEN"

$ curl -L -X PUT "http://$HEAVY_YT_PROXY/api/v3/write_table?path=//tmp/test-json&encode_utf8=false"
          -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
          -H "Content-Type: application/json"
          -H "Transfer-Encoding: chunked"
          -H "X-YT-Header-Format: <format=text>yson"
          -H "X-YT-Input-Format: <encode_utf8=%false>json"
          -T test.json
```

## Uploading files

To run a [Map](../../../user-guide/data-processing/operations/map.md) or [Reduce](../../../user-guide/data-processing/operations/reduce.md) operation, you will need to upload a script that must be executed into the system. The script for this task is placed on [GitHub](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/examples/python-misc/http_proxy.py) (however, it is locally saved to `exec.py` ). The [write_file](../../../api/commands.md#write_table) command is used to upload files to the {{product-name}} system. Before uploading, create a node of the "file" type, similar to creating the table at the previous step.

Uploading files:

```bash
$ curl -v -X POST "http://$YT_PROXY/api/v3/create?path=$YT_HOME/exec.py&type=file" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> POST /api/v3/create?path=//tmp/yt_examples/exec.py&type=file HTTP/1.1

< HTTP/1.1 200 OK
"0-efd-190-88fec34"

$ curl -v -L -X PUT "http://$YT_PROXY/api/v3/write_file?path=$YT_HOME/exec.py" \
    -H "Transfer-Encoding: chunked" -H "Authorization: OAuth $YT_TOKEN" \
    -T exec.py
> PUT /api/v3/write_file?path=//tmp/yt_examples/exec.py HTTP/1.1
> Expect: 100-continue

< HTTP/1.1 100 Continue
< HTTP/1.1 200 OK
```

For a file to have execution permissions, the *executable* attribute must be set for it.

Setting attributes:

```bash
$ curl -v -L -X PUT "http://$YT_PROXY/api/v3/set?path=$YT_HOME/exec.py/@executable" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary '"true"'
> PUT /api/v3/set?path=//tmp/yt_examples/exec.py/@executable HTTP/1.1

< HTTP/1.1 200 OK
```

To check whether the uploaded file is correct, you can read it.

Reading the uploaded file:

```bash
$ curl -v -L -X GET "http://$YT_HEAVY_PROXY/api/v3/read_file?path=$YT_HOME/exec.py" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> GET /api/v3/read_file?path=//tmp/yt_examples/exec.py HTTP/1.1

< HTTP/1.1 200 OK
#!/usr/bin/python

import re
import sys
import itertools
import operator
...
```

If you need to read a range of bytes from a file, you can use the additional *offset* and *length* parameters of the *read_file* command. The parameters are passed through the `X-YT-Parameters` header for variety.

Reading a range of bytes from a file:

```bash
$ curl -v -L -X GET "http://$YT_HEAVY_PROXY/api/v3/read_file" \
    -H "Authorization: OAuth $YT_TOKEN" \
    -H "X-YT-Parameters: {\"path\": \"$YT_HOME/exec.py\", \"offset\": 11, \"length\": 6}"
> GET /api/v3/read_file HTTP/1.1

< HTTP/1.1 200 OK
python
```

## Running a Map operation { #launch_map }

In the {{product-name}} system, to run a Map operation, there is a [command](../../../api/commands.md#map) with the same name. An important thing to consider is the requirement that an output table must exist **before** an operation is started. You can also create a table using the [create](../../../api/commands.md#create) command.

Creating an output table:

```bash
$ curl -v -X POST "http://$YT_PROXY/api/v3/create?path=$YT_HOME/output_map&type=table" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> POST /api/v3/create?path=//tmp/yt_examples/output_map&type=table HTTP/1.1

< HTTP/1.1 200 OK
"0-843-191-ae506abe"
```

Now you can run the Map operation. Pay attention to its specification: you can see that running the map command requires a structured set of arguments. In the HTTP interface of the system, you can encode arguments through a query string (which happened in the examples above, see the *path* and *type* arguments), as well as in the query body for POST queries. The example describes the specification for running in JSON format.

Running a Map operation:

```bash
$ cat <<END | curl -v -X POST "http://$YT_PROXY/api/v3/map" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary @-
{
  "spec" : {
    "mapper" : {
      "command" : "python exec.py map",
      "file_paths" : [ "$YT_HOME/exec.py" ],
      "format" : "dsv"
    },
    "input_table_paths" : [ "$YT_HOME/input" ],
    "output_table_paths" : [ "$YT_HOME/output_map" ]
  }
}
END
> POST /api/v3/map HTTP/1.1

< HTTP/1.1 200 OK
"381eb9-8eee9ec2-70a8370d-ea39f666"
```

{% note info "Note" %}

Unlike the `set` and `write_file` operations, the returned GUID of the operation is needed to keep track of the operation status. A successful response from the HTTP interface about running the operation indicates that the operation was started, but not that it was completed correctly.

{% endnote %}

You can view the operation status in the web interface or by calling `get_operation`.

Viewing the operation status

```bash
$ curl -v -X GET "http://$YT_PROXY/api/v3/get_operation?operation_id=381eb9-8eee9ec2-70a8370d-ea39f666" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> GET /api/v3/get_operation?operation_id=381eb9-8eee9ec2-70a8370d-ea39f666 HTTP/1.1

< HTTP/1.1 200 OK
{
  "operation_type" : "map",
  "progress" : {
    "jobs" : { "total" : 1, "pending" : 0, "running" : 0, "completed" : 1, "failed" : 0 },
    "chunks" : { "total" : 1,"running" : 0, "completed" : 1, "pending" : 0, "failed" : 0 },
    ...
  },
  "state" : "completed"
}
```

To make sure that the operation was completed correctly, you can read its result.

Reading data

```bash
$ curl -v -L -X GET "http://$YT_HEAVY_PROXY/api/v3/read_table?path=$YT_HOME/output_map" \
    -H "Accept: text/tab-separated-values" -H "Authorization: OAuth $YT_TOKEN" \
  | head
> GET /api/v3/read?path=//tmp/yt_examples/output_map HTTP/1.1


< HTTP/1.1 202 Accepted
word=html count=1
word=head count=1
word=title  count=1
word=михаил count=1
word=булгаков count=1
word=белая  count=1
word=гвардия  count=1
word=title  count=1
word=head count=1
word=body count=1
```

## Running sorting { #launch_sort }

To run sorting, use the [sort](../../../user-guide/data-processing/operations/sort.md) command which is a bit like [Map](../../../user-guide/data-processing/operations/map.md) and [Reduce](../../../user-guide/data-processing/operations/reduce.md) in its input specification.

Running sorting:

```bash
# Creating an output table
$ curl -v -X POST "http://$YT_PROXY/api/v3/create?path=$YT_HOME/output_sort&type=table" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> POST /api/v3/create?path=//tmp/yt_examples/output_map&type=table HTTP/1.1

< HTTP/1.1 200 OK
"0-95d-191-8901298a"

# Running sorting
$ cat <<END | curl -v -X POST "http://$YT_PROXY/api/v3/sort" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary @-
{
  "spec" : {
    "input_table_paths" : [ "$YT_HOME/output_map" ],
    "output_table_path" : "$YT_HOME/output_sort",
    "sort_by" : [ "word" ]
  }
}
END
> POST /api/v3/sort HTTP/1.1

< HTTP/1.1 200 OK
"640310-da8d54f1-6eded631-31c91e76"
```

You can monitor the operation execution process in the web interface.

## Running a Reduce operation { #launch_reduce }

The Reduce operation is started using the [command](../../../api/commands.md#reduce) with the same name.

Running a Reduce operation:

```bash
# Creating an output table
$ curl -v -X POST "http://$YT_PROXY/api/v3/create?path=$YT_HOME/output_reduce&type=table" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN"
> POST /api/v3/create?path=//tmp/yt_examples/output_reduce&type=table HTTP/1.1

< HTTP/1.1 200 OK
"0-95f-191-ba3a4055"

# Running reduce
$ cat <<END | curl -v -X POST "http://$YT_PROXY/api/v3/reduce" \
    -H "Accept: application/json" -H "Authorization: OAuth $YT_TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary @-
{
  "spec" : {
    "reducer" : {
      "command" : "python exec.py reduce",
      "file_paths" : [ "$YT_HOME/exec.py" ],
      "format" : "dsv"
    },
    "input_table_paths" : [ "$YT_HOME/output_sort" ],
    "output_table_paths" : [ "$YT_HOME/output_reduce" ],
    "sort_by" : [ "word" ],
    "reduce_by" : [ "word" ]
  }
}
END
> POST /api/v3/reduce HTTP/1.1

< HTTP/1.1 200 OK
"658ad8-edf7650f-182e4fd0-a1a0fd03"
```

## Reading the result { #read_results }

After you complete the operation, you will get calculated statistics on the frequency of word usage in "The White Guard".
Let's look at its result.

Reading the result:

```bash
$ curl -v -L -X GET "http://$YT_HEAVY_PROXY/api/v3/read_table?path=$YT_HOME/output_reduce" \
    -H "Accept: text/tab-separated-values" -H "Authorization: OAuth $YT_TOKEN" \
  | head
> GET /api/v3/read_table?path=//tmp/yt_examples/output_reduce HTTP/1.1

< HTTP/1.1 200 OK
count=1 word=0
count=1 word=04
count=1 word=05
count=4 word=1
count=2 word=10
count=4 word=11
count=4 word=12
count=4 word=13
count=3 word=14
count=2 word=15
```

