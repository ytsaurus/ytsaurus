# Examples

## Running a simple operation

For brevity, in the examples below, the ```--proxy <cluster_name>``` parameter is omitted or it is assumed that the ```YT_PROXY``` environment variable is set.

```bash
# Creating a directory in which output tables will be stored
yt create map_node //tmp/yt_cli_examples

# Running grep based on the data from logs. You must specify the format in which the data will come to the operation input.
yt map 'grep "domain.com" || exit 0' --src //home/logs/search/2017-Q3 --dst //tmp/yt_cli_examples/grep_result_table --format dsv

....

# Viewing the number of rows in the output table
yt get //tmp/yt_cli_examples/grep_result_table/@row_count

# Looking at the first 10 rows. The format must be specified.
yt read '//tmp/yt_cli_examples/grep_result_table[:#10]' --format dsv

# Reading rows 6 and 7 (numbering from zero) — in square brackets, the half-interval is set which includes the left boundary and does not include the right one
yt read '//tmp/yt_cli_examples/grep_result_table[#5:#7]' --format dsv

# Column-by-column reading:
yt read '//tmp/yt_cli_examples/grep_result_table{column1,column2}' --format dsv

# **Only for a sorted table**
# Viewing a range of keys in the table from "key_begin" to "key_end" (half-interval which includes the left boundary and does not include the right one).
yt read '//tmp/yt_cli_examples/grep_result_table["key_begin":"key_end"]' --format dsv
```

## The get, set, and remove commands

```bash
yt get //home/@count
36
yt set //home/@custom_attribute '{x=y}'
yt get //home/@custom_attribute
{
    "x" = "y"
}
yt remove //home/@custom_attribute
```

## The find and list commands

```bash
yt find / --name "dev"
//home/dev
//tmp/images/dev
//tmp/music/dev
...
yt list //home
abc_feedback
abt
abt-dev
abt-viewers
acid
...
yt list //home -l
map_node  abc_feedback 0.0B 2017-02-28 09:40 abc_feedback
map_node  abt          0.0B 2019-05-28 08:37 abt
map_node  abt-dev      0.0B 2019-04-28 20:23 abt-dev
map_node  abt-viewers  0.0B 2019-03-26 12:55 abt-viewers
map_node  dev          0.0B 2016-10-21 11:34 acid
...
```

## The read/write commands

```bash
echo "x=10" | yt write //tmp/yt_cli_examples/table --format dsv
yt read //tmp/yt_cli_examples/table --format dsv
x=10
```

```bash
echo -e "x=10\ty=20" | yt write //tmp/yt_cli_examples/table --format dsv
yt read //tmp/yt_cli_examples/table --format dsv
x=10   y=20
```

The `--format` option is mandatory and regulates the [format](../../../user-guide/storage/formats.md) in which data will be read.

Using `  --format "<format=text>yson" --control-attributes '{enable_row_index=%true}'`, you can find out the number of the first row in the ordered range.

## The upload/download commands

```bash
cat binary | yt upload //tmp/yt_cli_examples/binary --executable
yt download //tmp/yt_cli_examples/binary > binary_copy
diff binary binary_copy
```

## Running operations

```bash
yt map "./binary" --src //tmp/yt_cli_examples/table --dst //tmp/yt_cli_examples/output --format dsv --local-file binary
2013-03-28 16:23:35.797 ( 0 min): operation 1535429-8d795980-9f7f5a9f-44bec919 initializing
...

# Sorting by column x (in ascending order)
yt sort --src //tmp/yt_cli_examples/table --dst //tmp/yt_cli_examples/output --sort-by "x"
...

# Sorting by column x (in descending order, available only on clusters with masters version 21.2+)
yt sort --src //tmp/yt_cli_examples/table --dst //tmp/yt_cli_examples/output --sort-by "{name=x; sort_order=descending;}"
...

# Sorting by column x in descending order, and in the event of equality, by column y in ascending order (sorting in descending order is available only on clusters with masters version 21.2+)
yt sort --src //tmp/yt_cli_examples/table --dst //tmp/yt_cli_examples/output --sort-by "{name=x; sort_order=descending;}" --sort-by "y"
...

yt map-reduce --mapper cat --reducer cat --src //tmp/yt_cli_examples/table --dst //tmp/yt_cli_examples/output --format dsv --reduce-by "x"
...

yt map "cat" --spec "{pool=example_pool; weight=10.0}" --src //tmp/yt_cli_examples/table --dst //tmp/yt_cli_examples/output --format yson
...

yt vanilla --tasks '{task={job_count=10; command="sleep 60"; cpu_limit=2};}' --spec '{resource_limits={user_slots=2}}'
...
```

