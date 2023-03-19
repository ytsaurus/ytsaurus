---
vcsPath: yql/docs_yfm/docs/ru/yql-product/syntax/upsert_into.md
sourcePath: yql-product/syntax/upsert_into.md
---
{% include [x](_includes/upsert_into.md) %}


## UPSERT в {{product-name}}

{% note warning "Внимание" %}

В самом {{product-name}} операция не поддерживается, однако она может быть использована для публикации данных в Статфейс.

{% endnote %}

В этом случае в качестве таблицы выступает заданный скейл (существующего) отчета, набором ключевых колонок являются измерения отчета, а кластер (`stat_beta` или `stat`) определяет интерфейса Статфейса, который будет использован для публикации.

**Примеры:**

``` yql
UPSERT INTO stat.`Adhoc/My/Report/daily`
SELECT fielddate, hits FROM my_table_source;
```

``` yql
UPSERT INTO
  stat_beta.`Adhoc/My/Report/daily` (fielddate, hits)
VALUES
  ("2018-09-01", 1),
  ("2018-09-02", 2);
```

{% note warning "Внимание" %}

Во избежание утери данных при правках production расчетов рекомендуется предварительно опробовать правки на тестовом интерфейсе.

{% endnote %}

### Удаление данных из отчета перед заливкой

По умолчанию операция `UPSERT INTO` точечно обновляет записи в отчете в соответствии с набором измерений отчета.

Также поддерживается возможность удаления из отчета перед заливкой всех записей, значения некоторого набора измерений которых встречаются в заливаемых данных; для этого достаточно добавить модификатор: `UPSERT INTO ... ERASE BY (A, B, C)`, где `A, B, C` задают требуемый набор измерений.

**Примеры:**
``` yql
UPSERT INTO stat.`Adhoc/My/Report/daily` ERASE BY (fielddate)
SELECT fielddate, country, hits FROM my_table_source;
```

В этом примере из отчета будут удалены все данные за вновь посчитанные дни расчета (поле fielddate для скейла daily содержит дни), после чего будут залиты посчитанные данные.

``` yql
UPSERT INTO stat.`Adhoc/My/Report/daily` ERASE BY (fielddate, country)
SELECT fielddate, country, hits FROM my_table_source;
```

В этом случае задан полный набор измерений отчета, поэтому поведение семантически ничем не отличается от дефолтного поведения операции `UPSERT INTO`, модификатор можно не указывать.
