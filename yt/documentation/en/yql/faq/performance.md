---
vcsPath: yql/docs_yfm/docs/ru/yql-product/faq/performance.md
sourcePath: yql-product/faq/performance.md
---
# Performance

## Why does it take so long to run my query with JOIN?

All the basic [JOIN](../syntax/join.md) types (except SEMI/ONLY) build a Cartesian product of rows by the matching key values. If one or more values of the key are singular, it might have many rows on each side and result in a huge Cartesian product. This is the most common reason behind slow JOINs, and such singular values don't have much practical value, so you can exclude them using the WHERE clause.

To find such frequent anomalies, you can execute two queries against each table with a GROUP BY the same keys as in JOIN, using the COUNT aggregate function and descending sorting by its result. You can also add LIMIT 100 to make the query even faster.

Sometimes, your query won't actually need a Cartesian product in JOIN because anyway you'll need distinct values as a result. In this case, eliminate duplicates in your subquery using [GROUP BY](../syntax/group_by.md) or [SELECT DISTINCT](../syntax/select.md#distinct) even before JOIN{% if audience == internal %} and tell about your use case by commenting in the ticket [Convolution by payload in JOIN](https://st.yandex-team.ru/YQL-668){% endif %}.
