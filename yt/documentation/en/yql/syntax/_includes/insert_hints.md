---
vcsPath: yql/docs_yfm/docs/ru/yql-product/syntax/_includes/insert_hints.md
sourcePath: yql-product/syntax/_includes/insert_hints.md
---

Write operation can be performed with one or more modifiers. The modifier is specified after the `WITH` keyword after the table name: `INSERT INTO ... WITH SOME_HINT`.
If the modifier has a value, it's specified after the `=` sign: `INSERT INTO ... WITH SOME_HINT=value`.
If you need to specify multiple modifiers, they must be enclosed in parentheses: `INSERT INTO ... WITH (SOME_HINT1=value, SOME_HINT2, SOME_HINT3=value)`.

To clear existing data from the table before the write operation, just add a modifier: `INSERT INTO ... WITH TRUNCATE`.

**Examples:**

```yql
INSERT INTO my_table WITH TRUNCATE
SELECT key FROM my_table_source;
```



The full list of supported write modifiers:
* `TRUNCATE` to clear existing data from the table.
* `COMPRESSION_CODEC=codec_name` to write data with the specified compression codec. Takes priority over the `yt.PublishedCompressionCodec` pragma.
<!--For permitted values, see [documentation for {{product-name}}](../../user-guide/storage/compression.md). -->
* `ERASURE_CODEC=erasure_name` to write data with the specified erasure codec. Takes priority over the `yt.PublishedErasureCodec` pragma. <!--For permitted values, see [documentation for {{product-name}}](../../user-guide/storage/replication.md#erasure).-->

{% if audience == internal %}

* `EXPIRATION=value` to the set expiration period for the table being created. The value can be set as `duration` or as `timestamp` in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format. Takes priority over the `yt.ExpirationDeadline`/`yt.ExpirationInterval` pragmas. This modifier can only be set together with `TRUNCATE`.
* `REPLICATION_FACTOR=factor` to set the replication factor for the table being created. The value must be in the [1, 10] range.
* `USER_ATTRS=yson` to set the user attribute that will be added to the table. Attributes must be set as a yson dictionary.
* `MEDIA=yson` to set the media. Takes priority over the `yt.PublishedMedia` pragma. <!--For permitted values, see [documentation for {{product-name}}](../../user-guide/storage/media.md).-->
* `PRIMARY_MEDIUM=medium_name` to set the primary medium. Takes priority over the `yt.PublishedPrimaryMedium` pragma.
* `KEEP_META` to save the current schema, user attributes, and retention attributes (codecs, replications, media) of the target table. This modifier can only be set together with `TRUNCATE`.

{% note info %}

We do not recommend using system attributes in USER_ATTRS because the result of their application to the written data is not defined.

{% endnote %}

{% endif %}

**Examples:**


```yql
INSERT INTO my_table WITH (TRUNCATE, EXPIRATION="15m")
SELECT key FROM my_table_source;

INSERT INTO my_table WITH USER_ATTRS="{attr1=value1; attr2=value2;}"
SELECT key FROM my_table_source;
```

