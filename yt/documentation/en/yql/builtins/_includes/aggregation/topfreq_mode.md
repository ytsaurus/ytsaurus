
## TOPFREQ and MODE {#topfreq-mode}

**Signature**
```
TOPFREQ(T [, num:Uint32 [, bufSize:Uint32]])->List<Struct<Frequency:Uint64, Value:T>>
MODE(T [, num:Uint32 [, bufSize:Uint32]])->List<Struct<Frequency:Uint64, Value:T>>
```

Getting an approximate list of the most frequent column values with an estimate of their number. Return a list of structures with two fields:

* `Value`: The found frequent value.
* `Frequency`: Estimate of the number of mentions in the table.

Mandatory argument: the value itself.

Optional arguments:

1. For `TOPFREQ`: The desired number of items in the result. `MODE` is an alias to `TOPFREQ` with 1 in this argument. For `TOPFREQ`, the default value is also 1.
2. The number of items in the used buffer, which enables you to trade off memory consumption for accuracy. The default value is 100.

**Examples**
```yql
SELECT
    MODE(my_column),
    TOPFREQ(my_column, 5, 1000)
FROM my_table;
```
