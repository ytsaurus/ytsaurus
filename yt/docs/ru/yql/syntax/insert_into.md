
# INSERT INTO

Добавляет строки в таблицу. Если целевая таблица уже существует и не является сортированной, операция `INSERT INTO` дописывает строки в конец таблицы. В случае сортированной таблицы YQL пытается сохранить сортированность путём запуска сортированного слияния.

Таблица ищется по имени в базе данных, заданной оператором [USE](use.md).

## Описание

`INSERT INTO` позволяет выполнять следующие операции:

* Добавлять константные значения с помощью [`VALUES`](values.md).

  ```yql
  INSERT INTO my_table (Column1, Column2, Column3, Column4)
  VALUES (345987,'ydb', 'Яблочный край', 1414);
  COMMIT;
  ```

  ```yql
  INSERT INTO my_table (Column1, Column2)
  VALUES ("foo", 1), ("bar", 2);
  ```

* Сохранять результаты выборки `SELECT`.

  ```yql
  INSERT INTO my_table
  SELECT SourceTableColumn1 AS MyTableColumn1, "Empty" AS MyTableColumn2, SourceTableColumn2 AS MyTableColumn3
  FROM source_table;
  ```

## Использование модификаторов

Запись может выполняться с одним или несколькими модификаторами. Модификатор указывается после ключевого слова `WITH` после имени таблицы: `INSERT INTO ... WITH SOME_HINT`. Например, чтобы перед записью очистить таблицу от имевшихся данных, достаточно добавить модификатор `WITH TRUNCATE`:

```yql
INSERT INTO my_table WITH TRUNCATE
SELECT key FROM my_table_source;
```

Действуют следующие правила:
- Если у модификатора есть значение, то оно указывается после знака `=`: `INSERT INTO ... WITH SOME_HINT=value`.
- Если необходимо указать несколько модификаторов, то они заключаются в круглые скобки: `INSERT INTO ... WITH (SOME_HINT1=value, SOME_HINT2, SOME_HINT3=value)`.

## Запись в динтаблицы

Запись возможна только в [сортированные динамические таблицы](../../user-guide/dynamic-tables/sorted-dynamic-tables.md), которые уже должны существовать перед запуском запроса.

Использование конструкции `INSERT INTO` возможно только с модификатором `WITH TRUNCATE`, то есть с семантикой полной перезаписи данных в таблице.{% if audience == "internal" %} Семантика вставки без очистки поддерживается через конструкцию [REPLACE INTO](replace_into.md).{% endif %}

### Полный список поддерживаемых модификаторов записи (для статических таблиц)
* `TRUNCATE` &mdash; очистить таблицу от имеющихся данных.
* `COMPRESSION_CODEC=codec_name` &mdash; записать данные с указанным compression codec. Имеет приоритет над прагмой `yt.PublishedCompressionCodec`. Допустимые значения смотрите в разделе [Сжатие]({{yt-docs-root}}/user-guide/storage/compression#compression_codecs).
* `ERASURE_CODEC=erasure_name` &mdash; записать данные с указанным erasure codec. Имеет приоритет над прагмой `yt.PublishedErasureCodec`. Допустимые значения смотрите в разделе [Репликация]({{yt-docs-root}}/user-guide/storage/replication#erasure).
* `EXPIRATION=value` &mdash; задать expiration период для создаваемой таблицы. Значение может задаваться как `duration` или как `timestamp` в формате [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). Имеет приоритет над прагмами `yt.ExpirationDeadline` / `yt.ExpirationInterval`. Данный модификатор может задаваться только совместно с `TRUNCATE`.
* `REPLICATION_FACTOR=factor` &mdash; задать replication factor для создаваемой таблицы. Значением должно быть число в диапазоне [1, 10].
* `USER_ATTRS=yson` &mdash; задать пользовательские атрибуты, которые будут добавлены к таблице. Атрибуты должны задаваться в виде yson-словаря.
* `MEDIA=yson` &mdash; указать медиумы. Имеет приоритет над прагмой `yt.PublishedMedia`. Допустимые значения смотрите в разделе [Медиумы]({{yt-docs-root}}/user-guide/storage/media)
* `PRIMARY_MEDIUM=medium_name` &mdash; задать первичный медиум. Имеет приоритет над прагмой `yt.PublishedPrimaryMedium`.
* `KEEP_META` &mdash; сохранить текущую схему, пользовательские атрибуты и атрибуты хранения (кодеки, репликации, медиа) целевой таблицы. Данный модификатор может задаваться только совместно с `TRUNCATE`.

{% note info %}

Использовать в `USER_ATTRS` системные атрибуты не рекомендуется, поскольку результат их применения для записываемых данных не определён.

{% endnote %}

### Полный список поддерживаемых модификаторов записи (для динамических таблиц)
* `TRUNCATE` &mdash; очистить таблицу от имеющихся данных, обязателен в случае `INSERT INTO`
* `USER_ATTRS=yson` &mdash; задать пользовательские атрибуты, которые будут добавлены к таблице. Атрибуты должны задаваться в виде yson-словаря.

#### Примеры

```yql
INSERT INTO my_table WITH (TRUNCATE, EXPIRATION="15m")
SELECT key FROM my_table_source;

INSERT INTO my_table WITH USER_ATTRS="{attr1=value1; attr2=value2;}"
SELECT key FROM my_table_source;
```
