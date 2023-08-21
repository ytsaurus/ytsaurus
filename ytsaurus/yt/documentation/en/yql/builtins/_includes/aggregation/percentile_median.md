
## PERCENTILE and MEDIAN {#percentile-median}

**Signature**
```
PERCENTILE(Double?, Double)->Double?
PERCENTILE(Interval?, Double)->Interval?

MEDIAN(Double? [, Double])->Double?
MEDIAN(Interval? [, Double])->Interval?
```

Calculating percentiles according to the amortized version of the [TDigest](https://github.com/tdunning/t-digest) algorithm. `MEDIAN`: Alias for `PERCENTILE(N, 0.5)`.

{% note info "Limitation" %}

The first argument (N) must be the name of the table column. If you need to bypass this limitation, you can use a subquery. The limitation is introduced to simplify computations, because several invocations with the same first argument (N) are merged into one pass in the implementation.

{% endnote %}

```yql
SELECT
    MEDIAN(numeric_column),
    PERCENTILE(numeric_column, 0.99)
FROM my_table;
```

