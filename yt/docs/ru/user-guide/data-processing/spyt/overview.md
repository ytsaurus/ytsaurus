{% include [Обзор](../../../_includes/user-guide/data-processing/spyt/overview-p1.md) %}

## Что такое SPYT? { #what-is-spyt }

Spark over {{product-name}} (SPYT) позволяет запускать Spark-кластер на вычислительных мощностях {{product-name}}. Кластер запускается в [Vanilla-операции {{product-name}}](../../../user-guide/data-processing/operations/vanilla.md), затем забирает некоторое количество ресурсов из квоты и занимает их постоянно. Spark может читать как [статические](../../storage/static-tables.md), так и [динамические таблицы {{product-name}}](../../dynamic-tables/overview.md), делать на них расчеты и писать результат в статическую таблицу.
Текущая базовая версия Spark - 3.2.2.

{% include [Обзор](../../../_includes/user-guide/data-processing/spyt/overview-p2.md) %}
