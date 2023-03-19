---
vcsPath: yql/docs_yfm/docs/ru/yql-product/syntax/select.md
sourcePath: yql-product/syntax/select.md
---
# SELECT syntax

{% include [x](_includes/select/calc.md) %}

<!-- This is all for RTMR, all content below is excluded -->

{% include [x](_includes/select/from.md) %}



{% include [x](_includes/select/with.md) %}


{% include [x](_includes/select/where.md) %}

{% include [x](_includes/select/order_by.md) %}

{% include [x](_includes/select/limit_offset.md) %}


{% include [x](_includes/select/assume_order_by.md) %}

{% include [x](_includes/select/sample.md) %}

<!--[Example in tutorial](https://cluster-name.yql/Tutorial/yt_14_Sampling)-->


{% include [x](_includes/select/distinct.md) %}

{% include [x](_includes/select/execution.md) %}

{% include [x](_includes/select/column_order.md) %}


{% note warning "Attention!" %}

Key columns always go before non-key columns in table schema {{product-name}}. The order of key columns is determined by the order of the composite key.
When `PRAGMA OrderedColumns;` is enabled, non-key columns preserve their output order.

{% endnote %}

When `PRAGMA OrderedColumns;` is used, the sequence of columns after [PROCESS](process.md)/[REDUCE](reduce.md) is not defined.

{% include [x](_includes/select/union_all.md) %}

{% include [x](_includes/select/commit.md) %}


{% include [x](_includes/select/functional_tables.md) %}

{% include [x](_includes/select/folder.md) %}


{% include [x](_includes/select/without.md) %}

{% include [x](_includes/select/from_select.md) %}


{% include [x](_includes/select/view.md) %}

<!--[Learn more about view creation](../misc/schema.md#view).-->

{% include [x](_includes/select/temporary_table.md) %}


{% include [x](_includes/select/from_as_table.md) %}
