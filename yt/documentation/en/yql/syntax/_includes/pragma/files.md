## Working with files

### File

| Value type | By default | Static/<br/>dynamic |
| --- | --- | --- |
| Two or three string arguments — alias, URL, and optional token name | — | Static |

Attach a file to the query by URL. For attaching files you can use the built-in functions `FilePath` and `FileContent`. This `PRAGMA` is a universal alternative to attaching files using built-in mechanisms of web or console clients.

YQL reserves the right to cache files at the URL for an indefinite period, hence, if there is a significant change in the content behind it, we strongly recommended to modify the URL by adding/editing dummy parameters.

When specifying the token name, its value will be used to access the target system.

### Folder

| Value type | By default | Static/<br/>dynamic |
| --- | --- | --- |
| Two or three string arguments — prefix, URL, and optional token name | — | Static |

Attach a set of files to the query by URL. Functions similar to adding a set of files using [PRAGMA File](#file) via direct links to files with aliases obtained by joining a prefix with the file name via `/`.

When specifying the token name, its value will be used to access the target system.

### Library

| Value type | By default | Static/<br/>dynamic |
| --- | --- | --- |
| One or two arguments: the file name and an optional URL | — | Static |

Treat the specified attached file as a library from which you can do [IMPORT](../../export_import.md). The syntax type for the library is determined from the file extension:
* `.sql`: For the YQL dialect of SQL <span style="color: green;">(recommended)</span>.
* `.yql` for [s-expressions](/docs/s_expressions).

Example with a file attached to the query:

```yql
PRAGMA library("a.sql");
IMPORT a SYMBOLS $x;
SELECT $x;
```

If the URL is specified, the library is downloaded from the URL rather than from the pre-attached file as in the following example:

```yql
PRAGMA library("a.sql","https://paste.yandex-team.ru/5618566/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```

In this case, you can use text parameter value substitution in the URL:

```yql
DECLARE $_ver AS STRING; -- "5618566"
PRAGMA library("a.sql","https://paste.yandex-team.ru/{$_ver}/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```

