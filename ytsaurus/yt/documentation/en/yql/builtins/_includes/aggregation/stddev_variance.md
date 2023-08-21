
## STDDEV and VARIANCE {#stddev-variance}

**Signature**
```
STDDEV(Double?)->Double?
STDDEV_POPULATION(Double?)->Double?
POPULATION_STDDEV(Double?)->Double?
STDDEV_SAMPLE(Double?)->Double?
STDDEVSAMP(Double?)->Double?

VARIANCE(Double?)->Double?
VARIANCE_POPULATION(Double?)->Double?
POPULATION_VARIANCE(Double?)->Double?
VARPOP(Double?)->Double?
VARIANCE_SAMPLE(Double?)->Double?
```

Standard variance and dispersion by column. A [one-pass parallel algorithm](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm) is used the result of which may differ from that obtained by more common methods that require two passes over the data.

By default, sample dispersion and standard variance are calculated. Several write methods are available:

* With the `POPULATION` suffix/prefix, for example, `VARIANCE_POPULATION` and `POPULATION_VARIANCE`: Calculates dispersion/standard variance for the general population.
* With the `SAMPLE` suffix or without a suffix, for example, `VARIANCE_SAMPLE`, `SAMPLE_VARIANCE`, and `VARIANCE`: Calculates sample dispersion and standard variance.

There are also several abbreviated aliases, for example, `VARPOP` or `STDDEVSAMP`.

If all passed values are `NULL`, `NULL` is returned.

**Examples**
```yql
SELECT
  STDDEV(numeric_column),
  VARIANCE(numeric_column)
FROM my_table;
```
