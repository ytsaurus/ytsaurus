# PostgreSQL module

YQL supports PostgreSQL-compatible types, expressions, built-in functions, and syntax when processing data in {{product-name}}. PostgreSQL values use the `p` suffix, for example `'text'p` and `1p`.

## Calling PostgreSQL functions from YQL {#callpgfunction}

Use `PgCall(<function name>, <operands>)` to call a PostgreSQL function explicitly:

```yql
SELECT PgCall('lower', 'Test'p); -- 'test'p
```

For collation-sensitive functions, pass a named `Collation` argument:

```yql
SELECT PgCall('upper', 'straße'p, 'de-DE-x-icu' AS Collation); -- 'STRASSE'p
```

The value must be a collation name from `pg_catalog.pg_collation`: one of `default`, `C`, `POSIX`, `ucs_basic`, or `unicode`, or an ICU locale in the `<locale>-x-icu` form.

Use `PgRangeCall` for a function returning a set of `pgrecord` values:

```yql
SELECT * FROM AS_TABLE(PgRangeCall("json_each", pgjson('{"a":"foo", "b":"bar"}')));
```

## PostgreSQL syntax and warnings {#postgresql-syntax}

PostgreSQL syntax supports YT cluster-qualified table paths. Replace the placeholders with your cluster and table path:

```sql
SELECT * FROM <cluster-name>."//path/to/table";
```

Starting with version [2026.01](../../changelog/2026.01.md), `SET` can configure warning handling similarly to [PRAGMA Warning](../../syntax/pragma/global.md#warning):

```sql
SET Warning = "error", "4503";
```
