# Switching between tables

Operations can have several input and output tables. This section describes how to switch reading between them.

You can get data about which table an entry came from, and select which table to send it to, in the operation's [Map](../../../user-guide/data-processing/operations/map.md) and [Reduce](../../../user-guide/data-processing/operations/reduce.md) user code. This is done using **[descriptors](#descriptors)** or **[table switches](#switches)**.

{% note info "Note" %}

A user has several ways to specify which table a string is to be sent to: you can indicate the table's descriptor, or use a switch.

There is no technical restriction on using both of these mechanisms simultaneously, but doing so in practice is **not recommended**.

{% endnote %}

{% note warning "Attention!" %}

When no mapper is specified for a [MapReduce](../../../user-guide/data-processing/operations/mapreduce.md) type operation, there will be no data about which input tables entries came from at the reducer input.

{% endnote %}

## Descriptors { #descriptors }


<!-- Общее описание файловых дескрипторов джоба можно прочитать в разделе [Джобы](jobs.md#descriptors_in_job). -->

There are **descriptor numbering rules** that enable a user code (job) to write string X in table 0, and string Y in table 1.

When there's one output table, the job writes all entries and descriptor 1 (in stdout).
When there are multiple output tables, the numbering rules are used. The formula 3k+1, where k is the index of the output table, begins with 0.

- Table 0 - descriptor 1.
- Table 1 - descriptor 4.
- Table 2 - descriptor 7.
- ...

## Table switches { #switches }

The correspondence between descriptor numbers and output tables can be changed while working with a job, by using **table switches**.

The schema of configuring switches for input tables can be read in the [Input/output settings](../../../user-guide/storage/io-configuration.md).

The switches form depends on the format of output data.

### YSON { #yson }

In [YSON](../../../user-guide/storage/yson.md), in the entry stream, you might encounter an [entity](../../../user-guide/storage/yson.md#entity) with attributes for both input and output tables.  These are control inputs that are built into the datastream. For example: `table_index=N`, where N is an integer. This command "switches" the stream in such a way that subsequent entries are connected to another table with index N. This is true for both input and output tables.

Suppose our operation has 2 output tables at the output.
By default, to send data to a table with index 0 we must enter it in descriptor 1, and to be sent to table 1 it must be entered in descriptor 4.
Suppose the job has entered the following sequence of entries in descriptor 4:

```bash
{a=1};
<table_index=0>#;
{b=2};
```

In that case, the entry `a=1` goes into table 1, and entry `b=2` goes into table 0. Both descriptors will then write to table 0.

{% note warning "Attention!" %}

The order of strings when writing to one table through two descriptors is not established.

{% endnote %}

Switches work independently for each output descriptor.

Suppose descriptor N corresponds to table X. When you write 10 lines without switches to descriptor N, they will go into table X in the exact order in which they were written in the descriptor. There will be no other strings between them.

Suppose descriptors N and M correspond to table X. When you write 5 strings in descriptor N and 5 strings in descriptor M, all strings will go into table X, but their order will be unknown. Even if the user code writes only to descriptor N first, and only then to descriptor M, it cannot be guaranteed that the strings from N will appear in table X before the strings from M, or that they will not be shuffled.

### JSON { #json }

Works the same as [YSON](#yson).

Table switch:

```json
'{"$value": null, "$attributes": {"table_index": 1}}\n'
```

### DSV { #dsv }

Strictly speaking, the format and its derivatives do not support switches as a separate control input in the datastream. Each data entry must contain information about the table index.

{% note info "Note" %}

[DSV](./../../user-guide/storage/formats.md#dsv) does not support switching of input tables.

{% endnote %}

When the `enable_table_index` format option is enabled each line of an input table will be supplemented by a command window containing the table index. By default, the command field is called `@table_index`. The name of the field can be changed via the `table_index_column` format option.

All lines from the `//path/to/table` table, which was numbered N in the list of the operation's input tables, will have the same value in their `@table_index` field, equal to N.

### SCHEMAFUL_DSV { #schemaful_dsv }

When installing the `enable_table_index=true` option for tables in [SCHEMAFUL_DSV](./../../user-guide/storage/formats.md#schemaful_dsv) format, the table index will be recorded in all lines of the table with the first field, before the schema colmns.

## Examples

```python
# -*- coding: utf-8 -*-
import yt.wrapper as yt

from datetime import datetime

def parse_time(time_str):
    "2012-10-19T11:22:58.190448Z"
    return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")

def get_duration(rowA, rowB):
    if rowA["event_type"] == "job_completed":
        rowA, rowB = rowB, rowA
    delta = parse_time(rowB["timestamp"]) - parse_time(rowA["timestamp"])
    return delta.total_seconds() + (delta.microseconds / 1000000.0)

# By default the map-function accepts one entry for input.
# Any function run as an operation must be a generator.
def filter_event_type(row):
    if row.get("event_type") in ["job_started", "job_completed"]:
        yield row

# In the event of a reduce-function, a key is sent to the input (this is a map from the key columns to their value), as well as a list of entries.  
# The key columns of each entry are equal to that which lies in the key
def calculate_duration(key, rows):
    rows = list(rows)
    if len(rows) == 2:
        yield {"id": key["id"], "duration": get_duration(*rows)}
    else:
        assert len(rows) == 1
        row = rows[0]
        # You can indicate the output table number via the special field
        row["@table_index"] = 1
        yield row

if __name__ == "__main__":
    yt.config["tabular_data_format"] = yt.JsonFormat(control_attributes_mode="row_fields")
    yt.run_map_reduce(filter_event_type, calculate_duration,
                      "//tmp/forbeginners/event_log",
                      ["//tmp/forbeginners/durations", "//tmp/forbeginners/filtered"],
                      reduce_by="id")
```

An example of switching between output tables with a `table_index` with `control_attributes_mode="iterator"`:

```python
import yt.wrapper as yt

import random

@yt.aggregator
@yt.with_context
def mapper(rows, context):
    for row in rows:
        input_table_index = context.table_index
        sum = 0
        if input_table_index == 0:
            sum += int(row["value"])
        else:
            sum -= int(row["value"])

        output_table_index = random.randint(0, 1)
        # The function enables you to create an entry that is the switch to the table with the specified index.
        yield yt.create_table_switch(output_table_index)
        yield {"sum": sum}

if __name__ == "__main__":
    yt.run_map(mapper,
               ["//tmp/input1", "//tmp/input2", "//tmp/input3"],
               ["//tmp/output1", "//tmp/output2"],
               format=yt.YsonFormat(control_attributes_mode="iterator"))
```

