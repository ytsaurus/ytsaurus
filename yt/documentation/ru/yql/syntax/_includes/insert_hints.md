Запись может выполняться с одним или несколькими модификаторами. Модификатор указывается после ключевого слова `WITH` после имени таблицы: `INSERT INTO ... WITH SOME_HINT`.
Если у модификатора есть значение, то оно указывается после знака `=`: `INSERT INTO ... WITH SOME_HINT=value`.
При необходимости указать несколько модификаторов они должны заключаться в круглые скобки: `INSERT INTO ... WITH (SOME_HINT1=value, SOME_HINT2, SOME_HINT3=value)`.

Чтобы перед записью очистить таблицу от имевшихся данных достаточно добавить модификатор: `INSERT INTO ... WITH TRUNCATE`.

**Примеры:**

``` yql
INSERT INTO my_table WITH TRUNCATE
SELECT key FROM my_table_source;
```

{% if audience == internal %}


Полный список поддерживаемых модификаторов записи:
* `TRUNCATE` - очистить таблицу от имеющихся данных.
* `COMPRESSION_CODEC=codec_name` - записать данные с указанным compression codec.  Имеет приоритет над прагмой `yt.PublishedCompressionCodec`.
<!--Допустимые значения смотрите в разделе [Сжатие](../../user-guide/storage/compression.md#compression_codecs).-->
* `ERASURE_CODEC=erasure_name` - записать данные с указанным erasure codec. Имеет приоритет над прагмой `yt.PublishedErasureCodec`.
<!--Допустимые значения смотрите в разделе [Репликация](../../user-guide/storage/replication.md#erasure).-->
* `EXPIRATION=value` - задать expiration период для создаваемой таблицы. Значение может задаваться как `duration` или как `timestamp` в формате [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). Имеет приоритет над прагмами `yt.ExpirationDeadline` / `yt.ExpirationInterval`. Данный модификатор может задаваться только совместно с `TRUNCATE`.
* `REPLICATION_FACTOR=factor` - задать replication factor для создаваемой таблицы. Значением должно быть число в диапазоне [1, 10].
* `USER_ATTRS=yson` - задать пользовательские атрибуты, которые будут добавлены к таблице. Атрибуты должны задаваться в виде yson-словаря.
* `MEDIA=yson` - указать медиумы. Имеет приоритет над прагмой `yt.PublishedMedia`.
<!--Допустимые значения смотрите в разделе [Медиумы](../../user-guide/storage/media.md)-->
* `PRIMARY_MEDIUM=medium_name` - задать первичный медиум. Имеет приоритет над прагмой `yt.PublishedPrimaryMedium`.
* `KEEP_META` - сохранить текущую схему, пользовательские атрибуты и атрибуты хранения (кодеки, репликации, медиа) целевой таблицы. Данный модификатор может задаваться только совместно с `TRUNCATE`.

{% note info %}

Использовать в USER_ATTRS системные атрибуты не рекомендуется, поскольку результат их применения для записываемых данных не определён.

{% endnote %}

{% endif %}


**Примеры:**


``` yql
INSERT INTO my_table WITH (TRUNCATE, EXPIRATION="15m")
SELECT key FROM my_table_source;

INSERT INTO my_table WITH USER_ATTRS="{attr1=value1; attr2=value2;}"
SELECT key FROM my_table_source;
```

