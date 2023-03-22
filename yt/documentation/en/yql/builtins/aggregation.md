
{% include [x](_includes/aggregation/simple.md) %}

{% include [x](_includes/aggregation/count_distinct_estimate.md) %}

{% include [x](_includes/aggregation/agg_list.md) %}

If the number of items in the list is exceeded, the `Memory limit exceeded` error is returned.

<!--[Example in tutorial](https://cluster-name.yql/)-->

{% include [x](_includes/aggregation/max_min_by.md) %}

{% include [x](_includes/aggregation/top_bottom.md) %}

{% include [x](_includes/aggregation/topfreq_mode.md) %}

{% include [x](_includes/aggregation/stddev_variance.md) %}

{% include [x](_includes/aggregation/corr_covar.md) %}

{% include [x](_includes/aggregation/percentile_median.md) %}

{% include [x](_includes/aggregation/histogram.md) %}

{% if audience == internal %}

* [The used library with implementation in Arcadia](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/histogram/adaptive).
* [Distance, Weight, Ward](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/histogram/adaptive/common.cpp).

{% endif %}

{% include [x](_includes/aggregation/bool_bit.md) %}


{% include [x](_includes/aggregation/session_start.md) %}


{% include [x](_includes/aggregation/aggregate_by.md) %}

## UDAF

If the above aggregate functions were not enough for some reason, YQL has a mechanism for describing custom aggregate functions.