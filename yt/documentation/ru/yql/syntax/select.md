---
vcsPath: yql/docs_yfm/docs/ru/yql-product/syntax/select.md
sourcePath: yql-product/syntax/select.md
---
# Синтаксис SELECT

{% include [x](_includes/select/calc.md) %}

 <!-- This is all for RTMR, all content below is excluded -->

{% include [x](_includes/select/from.md) %}



  {% include [x](_includes/select/with.md) %}


{% include [x](_includes/select/where.md) %}

{% include [x](_includes/select/order_by.md) %}

{% include [x](_includes/select/limit_offset.md) %}


  {% include [x](_includes/select/assume_order_by.md) %}

  {% include [x](_includes/select/sample.md) %}

<!--[Пример в tutorial](https://cluster-name.yql/Tutorial/yt_14_Sampling)-->


{% include [x](_includes/select/distinct.md) %}

{% include [x](_includes/select/execution.md) %}

{% include [x](_includes/select/column_order.md) %}


{% note warning "Внимание" %}

В схеме таблиц {{product-name}} ключевые колонки всегда идут перед неключевыми колонками. Порядок ключевых колонок определяется порядком составного ключа.
При включенной `PRAGMA OrderedColumns;` неключевые колонки будут сохранять выведенный порядок.

{% endnote %}

При использовании `PRAGMA OrderedColumns;` порядок колонок после [PROCESS](process.md)/[REDUCE](reduce.md) не определен.

{% include [x](_includes/select/union_all.md) %}

{% include [x](_includes/select/commit.md) %}


  {% include [x](_includes/select/functional_tables.md) %}

  {% include [x](_includes/select/folder.md) %}


{% include [x](_includes/select/without.md) %}

{% include [x](_includes/select/from_select.md) %}


  {% include [x](_includes/select/view.md) %}


  {% include [x](_includes/select/temporary_table.md) %}


{% include [x](_includes/select/from_as_table.md) %}
