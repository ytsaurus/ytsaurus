
# INSERT INTO

Adds rows to the table. If the target table already exists and is not sorted, the operation `INSERT INTO` adds rows at the end of the table. For a sorted table, YQL attempts to preserve sorting by starting a sorted merge.

Search for the table by name in the database set by the [USE](use.md) operator.

## Description

`INSERT INTO` lets you perform the following operations:

* Adding constant values using [`VALUES`](values.md).

  ```yql
  INSERT INTO my_table (Column1, Column2, Column3, Column4)
  VALUES (345987,'ydb', 'Apple region', 1414);
  COMMIT;
  ```

  ```yql
  INSERT INTO my_table (Column1, Column2)
  VALUES ("foo", 1), ("bar", 2);
  ```

* Saving the results of a `SELECT` statement.

  ```yql
  INSERT INTO my_table
  SELECT SourceTableColumn1 AS MyTableColumn1, "Empty" AS MyTableColumn2, SourceTableColumn2 AS MyTableColumn3
  FROM source_table;
  ```

## Using modifiers

Write operation can be performed with one or more modifiers. The modifier is specified after the `WITH` keyword after the table name: `INSERT INTO ... WITH SOME_HINT`. For example, to clear existing data from the table before executing a write operation, just add the `WITH TRUNCATE` modifier:

```yql
INSERT INTO my_table WITH TRUNCATE
SELECT key FROM my_table_source;
```

The following rules apply:
- If the modifier has a value, it's specified after the `=` sign: `INSERT INTO ... WITH SOME_HINT=value`.
- If you need to specify multiple modifiers, they must be enclosed in parentheses: `INSERT INTO ... WITH (SOME_HINT1=value, SOME_HINT2, SOME_HINT3=value)`.

## Writing to dynamic tables

You can only write data to sorted dynamic tables, and they must already exist before you run the query.
`INSERT INTO` statements can only be used with the `WITH TRUNCATE` modifier, which implies that any data in the table will be completely overwritten. To insert data without clearing the table, use [REPLACE INTO](replace_into.md) instead.

### Full list of supported write modifiers (for static tables)
* `TRUNCATE` — Clears existing data from the table.
* `COMPRESSION_CODEC=codec_name` — Writes data with the specified compression codec. This takes priority over the `yt.PublishedCompressionCodec` pragma. For the list of supported values, see [Compression]({{yt-docs-root}}/user-guide/storage/compression#compression_codecs).
* `ERASURE_CODEC=erasure_name` — Writes data with the specified erasure codec. This takes priority over the `yt.PublishedErasureCodec` pragma. For the list of supported values, see [Replication and erasure coding]({{yt-docs-root}}/user-guide/storage/replication#erasure).
* `EXPIRATION=value` — Sets the expiration period for the table being created. The value can be set as a `duration` or as a `timestamp` in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format. Takes priority over the `yt.ExpirationDeadline`/`yt.ExpirationInterval` pragmas. This modifier can only be set together with `TRUNCATE`.
* `REPLICATION_FACTOR=factor` — Sets the replication factor for the table being created. The value must be in the [1, 10] range.
* `USER_ATTRS=yson` — Sets user attributes that will be added to the table. Attributes must be set as a YSON dictionary.
* `MEDIA=yson` — Sets the media. This takes priority over the `yt.PublishedMedia` pragma. For the list of supported values, see [Media]({{yt-docs-root}}/user-guide/storage/media).
* `PRIMARY_MEDIUM=medium_name` — Sets the primary medium. This takes priority over the `yt.PublishedPrimaryMedium` pragma.
* `KEEP_META` — Saves the current schema, user attributes, and storage attributes (codecs, replications, media) of the target table. This modifier can only be set together with `TRUNCATE`.

{% note info %}

We do not recommend using system attributes in `USER_ATTRS`, as applying them to written data can lead to unpredictable or unreliable results.

{% endnote %}

### Full list of supported write modifiers (for dynamic tables)
* `TRUNCATE` — Clears existing data from the table; mandatory for `INSERT INTO`.
* `USER_ATTRS=yson` — Sets user attributes that will be added to the table. Attributes must be set as a YSON dictionary.

#### Examples

```yql
INSERT INTO my_table WITH (TRUNCATE, EXPIRATION="15m")
SELECT key FROM my_table_source;

INSERT INTO my_table WITH USER_ATTRS="{attr1=value1; attr2=value2;}"
SELECT key FROM my_table_source;
```
