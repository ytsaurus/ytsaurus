# Spark SQL

С таблицами {{product-name}} возможно работать из [Spark SQL](https://spark.apache.org/docs/latest/sql-ref-syntax.html). Этот диалект SQL используется для запросов в [Query tracker](../../../../user-guide/query-tracker.md) с использованием SPYT.

При работе с {{product-name}} в качестве идентификатора базы данных используется `yt`, а в качестве файловой системы — `ytTable:/`. Второе можно опускать, поэтому приведенная пара запросов эквивалентна:

```sql
SELECT `key` FROM yt.`ytTable:///home/service/data`
```

```sql
SELECT `key` FROM yt.`//home/service/data`
```

Запросы в другие системы осуществляются так же, как в оригинальном Spark:

```sql
SELECT * FROM json.`s3a://bucket/file.json`
```

{% note info "Примечание" %}

Spark не поддерживает запуск множества последовательных команд в одном запросе. Все команды (`CREATE`, `INSERT` и т. д.) необходимо выполнять отдельными запросами.

{% endnote %}

## Работа с таблицами

Создание таблицы:

```sql
CREATE TABLE yt.`//tmp/users` (
    id INT,
    name STRING
) USING yt
```

```sql
CREATE TABLE yt.`//tmp/users_copy`
    USING yt AS
    SELECT * FROM yt.`//tmp/users`
```

Удаление таблицы:

```sql
DROP TABLE yt.`//tmp/users`
```

При исполнении запроса метаинформация о таблицах кешируется в память Spark-сессии для переиспользования при последующих обращениях. Если после запроса в таблице происходили изменения сторонними процессами, тогда информация в кеше становится неактуальной, а новые запросы будут выполняться с ошибками. В таком случае необходимо вручную сбрасывать кеш:

```sql
CREATE TABLE yt.`//tmp/users` (id INT, name STRING) USING yt
-- Любые изменения таблицы сторонними процессами
REFRESH TABLE yt.`//tmp/users` -- Сброс кеша
SELECT * FROM yt.`//tmp/users`
```

## Работа с данными

Чтение статических таблиц:

```sql
SELECT t1.value
    FROM yt.`//home/service/table1` t1
    JOIN yt.`//home/service/table2` t2
    ON t1.id == t2.id
```

Для чтения динамических таблиц необходимо указать временную метку среза данных:

```sql
SELECT * FROM yt.`//home/service/dynamic_data/@latest_version`
```

```sql
SELECT * FROM yt.`//home/service/dynamic_data/@timestamp_XXX`
```

Вставки в существующую таблицу:

```sql
INSERT INTO TABLE yt.`//home/service/copy`
    SELECT * FROM yt.`//home/service/origin`
```

```sql
INSERT OVERWRITE TABLE yt.`//home/service/copy`
    VALUES (-1, "Existed data is overwritten")
```

Запросы обновления в Apache Spark не поддерживаются.
