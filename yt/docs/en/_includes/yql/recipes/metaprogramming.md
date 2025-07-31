# Metaprogramming practices in YQL

## Introduction
{{product-name}} doesn't support partitioned tables natively. That's why typical is the situation where a logically single table is stored as a set of physical tables split by date, hour, or the division remainder of some field hash. As a result, the queries run in YQL over {{product-name}} often access a huge number of tables (dozens, hundreds, or thousands).

Measured by the SQL standard, this is an exclusive situation and there are no generally adopted mechanisms to describe such queries in SQL terms. Rachel offers many extensions for the standard designed for such specific tasks. In the majority of regularly or periodically ran queries, they enable you to totally skip generation of query texts on the client. Each mechanism [is detailed in the main YQL syntax tutorial](../../../yql/syntax/index.md), and the purpose of this article is to demonstrate how you can combine these mechanisms to achieve practically useful results on near-to-real examples.{% if audience == "internal" %} These examples haven't been included in the [main tutorial]({{yql.link}}/Tutorial/yt_01_Select_all_columns) for a number of reasons, but will likely be added there in the future.{% endif %}

## Use cases

### Selecting tables by dates
Most logs are archived to a flat directory in {{product-name}} where an independent table is created for each date. If you want to make a calculation for certain dates, but don't want to add them manually to the query text, use the `FILTER` table function and select the dates with a Lambda function.

```yql
USE {{production-cluster}};

$log_type = "yql-log";
$base_folder = "logs/" || $log_type || "/1d";
$today = CurrentUtcDate();
$yesterday = $today - Interval("P1D");
$a_week_ago = $today - Interval("P1W");

$is_during_last_week = ($table_name) -> {
    $date = CAST($table_name AS Date);
    RETURN ($date BETWEEN $a_week_ago AND $yesterday) ?? false;
};

SELECT COUNT(*)
FROM FILTER($base_folder, $is_during_last_week);
```

Links to documentation:

* [CurrentUtcDate](../../../yql/builtins/basic.md#currentutcdate)
* [Lambda functions](../../../yql/syntax/expressions.md#lambda)
* [FILTER](../../../yql/syntax/select/index.md)

### Processing tables with a cached sliding window
Imagine that every day you need to calculate some aggregate of values for the previous week (like in the previous example). By their very nature, most log tables don't change beginning from the night of the next day. Hence, nearly 6/7 of computing resources every day are wasted to repeat calculations from the previous day. YQL offers a mechanism for [automatic caching of intermediate computing results](../../../yql/syntax/pragma/yt#querycachemode). However, as it wholly runs at the MapReduce level, the previous query would have zero cache hit: every day will produce a different set  of input tables for the first Map operation (and this set is used to calculate a key in the cache).

To give the automatic caching mechanism a chance to succeed (or even guarantee success, if you override [PRAGMA yt.TmpFolder](../../../yql/syntax/pragma/yt#tmpfolder) to your custom directory/quota instead of `//tmp`), you need to calculate an aggregate for each day separately and then combine the aggregates into a single resulting value (fortunately, most aggregate functions support this).

```yql
USE {{production-cluster}};

$log_type = "visit-log";
$base_folder = "logs/" || $log_type || "/1d/";
$today = CurrentUtcDate();
$range = ListFromRange(1, 9);

$shift_back_by_days = ($days_count) -> {
    RETURN Unwrap(
        $today - $days_count * Interval("P1D"),
        "Failed to shift today back by " || CAST($days_count AS String) || " days"
    );
};
$dates = ListMap($range, $shift_back_by_days);

DEFINE ACTION $max_income($date) AS
    $table_path = $base_folder || CAST($date AS String);
    INSERT INTO @daily_results
    SELECT MAX(CAST(Income AS Double)) AS MaxIncome
    FROM $table_path;
END DEFINE;

EVALUATE FOR $date IN $dates DO $max_income($date);

COMMIT;

SELECT MAX(MaxIncome) AS MaxIncome
FROM @daily_results;

DISCARD SELECT
    Ensure(
        NULL,
        COUNT(*) == 7,
        "Unexpected number of aggregates to be merged: " || CAST(COUNT(*) AS String) || " instead of 7"
    )
FROM @daily_results;
```

In contrast to the previous example, here we don't select table paths based on the directory contents. Instead, we generate them "out of thin air" based on the table naming conventions configured for this log. We first define an action to process each table separately (`DEFINE ACTION`) and then use a loop to apply this action to each calculated date (`EVALUATE FOR`).

This query also demonstrates several ways to terminate a query with a meaningful error if something strange occurs, instead of producing an invalid result:

* Going beyond the range of supported values is almost impossible for this use case. However, only in these situations, the date arithmetic can return NULL. In this context, it's quite safe to use Unwrap to convert an Optional value to a non-Optional one (exactly not NULL), producing a runtime error if the Optional type, nevertheless, included NULL.
* Using Ensure, you can check any conditions when running a query and return an error if the condition isn't met. You can add it at runtime, but if you combine it with `DISCARD` (calculate something and discard the result), you can make a separate side-check, for example, based on an aggregate of a temporary or summary table as here. In the case with `DISCARD`, the first argument is Ensure, which is usually a result of its call. It doesn't bear much sense, so you can write any constant to it, for example, NULL.

Links to documentation:

* [Lambda functions](../../../yql/syntax/expressions.md#lambda)
* [CurrentUtcDate](../../../yql/builtins/basic.md#currentutcdate)
* [ListFromRange](../../../yql/builtins/list.md#listfromrange) / [ListMap](../../../yql/builtins/list.md#listmap) / [ListLength](../../../yql/builtins/list.md#listlength)
* [Unwrap](../../../yql/builtins/basic.md#unwrap)
* [DEFINE ACTION](../../../yql/syntax/action.md)
* [EVALUATE FOR](../../../yql/syntax/action.md#evaluate-for)
* [@foo](../../../yql/syntax/select/temporary_table.md)
* [COMMIT](../../../yql/syntax/commit.md)
* [DISCARD](../../../yql/syntax/discard.md)
* [Ensure](../../../yql/builtins/basic.md#ensure)


### Use case with job processing
For example, you have an external process that writes tables to a directory. Basically, these tables are jobs to be processed. You need to run a computation on the contents of such table, write the result to another directory, and delete the processed tables: all this in the transactional mode.

{% cut "How can you emulate such an external process on YQL for testing purposes?" %}

```yql
USE {{production-cluster}};

$root_folder = "//tmp/tasks/";
$values = ListFromRange(1, 10);

DEFINE ACTION $create_task($i) AS
    $path = $root_folder || CAST($i AS String);
    INSERT INTO $path SELECT $i AS Task;
END DEFINE;

EVALUATE FOR $value IN $values DO $create_task($value);
```

{% endcut %}

First, start the processing
```yql
USE {{production-cluster}};

$tasks_folder = "tmp/tasks";
$results_folder = "tmp/results";

DEFINE ACTION $process_task($input_path) AS
    $output_path = String::ReplaceAll($input_path, $tasks_folder, $results_folder);

    INSERT INTO $output_path WITH TRUNCATE
    SELECT Task * Task AS Result
    FROM $input_path;
    COMMIT;
    DROP TABLE $input_path;
END DEFINE;

$tasks = (
    SELECT AGGREGATE_LIST(Path)
    FROM FOLDER($tasks_folder, "row_count")
    WHERE Type == "table" AND
        Yson::LookupInt64(Attributes, "row_count") == 1
);

EVALUATE FOR $task IN $tasks ?? ListCreate(TypeOf($tasks)) DO $process_task($task);
```
Here, you also use the mechanism of actions that loop through a list. However, the list of input tables isn't taken "out of thin air", but from the `FOLDER` table function that provides access to the contents of an arbitrary directory in {{product-name}}. Since `FOLDER` outputs not only tables but other types of nodes as well, meaning you can't be sure that the directory hosts only tables, we recommend adding a filter by Type or by arbitrary meta attributes of the table (we use row_count here as an example). For this, you'll need to query them in the second `FOLDER` argument (if you have several of them, use a semicolon) and then access the Attributes Yson-type column.

ListExtract takes a list of structures that include a single element and converts it to a list of strings with paths. However, it's also possible to retrieve Path from inside the action.

Links to documentation:

* [String UDF](../../../yql/udf/list/string.md) / [Yson UDF](../../../yql/udf/list/yson.md)
* [ListFromRange](../../../yql/builtins/list.md#listfromrange) / [ListExtract](../../../yql/builtins/list.md#listextract)
* [FOLDER](../../../yql/syntax/select/folder.md)
* [DROP TABLE](../../../yql/syntax/drop_table.md)
