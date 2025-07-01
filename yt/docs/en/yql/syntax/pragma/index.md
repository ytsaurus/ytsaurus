# PRAGMA

## Definition

Pragmas are special directives that override the query execution settings. For example, they can be used to select a strategy for joining tables, configure the error logging level, or specify which pool to perform query operations in. Pragmas can affect query execution speed, resource allocation, and semantics.

The pragmas apply only within the current query. For more information, see the [pragma scope](global.md#pragmascope).

For the full list of available settings, [see below](#pragmas).

## Syntax

`PRAGMA x.y = "z";` or `PRAGMA x.y("z", "z2", "z3");`:

* `x`: (optional) The category of the setting.
* `y`: The name of the setting.
* `z`: (optional for flags) The value of the setting. The following suffixes are acceptable:

  * `k`, `m`, `g`, `t`: For the data amounts.
  * `w`,`d`, `h`, `m`, `s`, `ms`, `us`, `ns` : For the time values.

For [dynamic yt pragmas](yt.md), you can revert the settings values to their default states using `PRAGMA my_pragma = default;`. Please note that you can't reset the settings for other pragmas.

## Examples

```yql
PRAGMA AutoCommit;
```

```yql
PRAGMA TablePathPrefix = "home/yql";
```

```yql
PRAGMA Warning("disable", "1101");
```


## Available pragmas {#pragmas}

Getting diagnostic information from YQL as an additional result of a query.
* [Global](global.md)
* [Yson](yson.md)
* [Working with files](file.md)
* [YT pragmas](yt.md)
* Debug and service &mdash; documentation will be added soon