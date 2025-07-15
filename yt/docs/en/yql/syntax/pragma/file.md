# Working with files

## File

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Two or three string arguments — alias, URL, and optional token name | — | Static |

Attach a file to the query by URL. For attaching files you can use the built-in functions [FilePath and FileContent](../../builtins/basic.md#filecontent). This `PRAGMA` is a universal alternative to attaching files using built-in mechanisms of web or console clients.

YQL reserves the right to cache files at the URL for an indefinite period, hence, if there is a significant change in the content behind it, we strongly recommend to modify the URL by adding or modifying dummy parameters.

If the token name is specified, its value will be used to access the target system.

## FileOption

| Value type | Default value | Static/<br/>dynamic |
|-------------------------------------------------|--------------|--------------------------------|
| Three string arguments: alias, key, value | — | Static |

Set the option by the specified key for the specified file to the specified value. The file with this alias should already be declared through [PRAGMA File](#file) or attached to the query.

Currently supported options:

| Key | Value range | Description |
|-------------------------|-------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `bypass_artifact_cache` | `true`/`false` | Manages [caching]({{yt-docs-root}}/user-guide/data-processing/operations/operations-options#fajly) |

Example:

```yql
PRAGMA FileOption("<file-name>", "bypass_artifact_cache", "true");
```

{% if audience == "internal" %}
## Folder

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Two or three string arguments — prefix, URL, and optional token name | — | Static |

Attach a set of files to the query by URL. Functions similar to adding a set of files using [PRAGMA File](#file) via direct links to files with aliases obtained by joining a prefix with the file name via `/`.

If the token name if specified, its value will be used to access the target system.
{% endif %}

## Library

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| One or two arguments: file name and optional URL | — | Static |

Treat the specified attached file as a library from which you can do [IMPORT](../export_import.md). The syntax type for the library is determined from the file extension:
* `.sql`: For the YQL dialect of SQL <span style="color: green;">(recommended)</span>.
* `.yqls`: For {% if audience == "internal"  %}[s-expressions]({{yql.s-expressions-link}}){% else %}s-expressions{% endif %}.

Example with a file attached to the query:

```yql
PRAGMA library("a.sql");
IMPORT a SYMBOLS $x;
SELECT $x;
```

If the URL is specified, the library is downloaded from the URL rather than from the previously attached file as in the following example:
{% if audience == "internal"%}
```yql
PRAGMA library("a.sql","{{ corporate-paste }}/5618566/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```

In this case, you can use text parameter value substitution in the URL:

```yql
DECLARE $_ver AS STRING; -- "5618566"
PRAGMA library("a.sql","{{ corporate-paste }}/{$_ver}/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```
{% else %}

```yql
PRAGMA library("a.sql","https://raw.githubusercontent.com/ytsaurus/ytsaurus/refs/heads/main/yt/docs/code-examples/yql/pragma-library-example");
IMPORT a SYMBOLS $x;
SELECT $x;
```

In this case, you can use text parameter value substitution in the URL:

```yql
DECLARE $_ver AS STRING; -- "pragma-library-example"
PRAGMA library("a.sql","https://raw.githubusercontent.com/ytsaurus/ytsaurus/refs/heads/main/yt/docs/code-examples/yql/{$_ver}");
IMPORT a SYMBOLS $x;
SELECT $x;
```
{% endif %}

## Package

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| Two or three arguments: package name, URL, and optional token | — | Static |

Attach a hierarchical set of files to the query by URL, treating them as a package with the specified name, an interrelated set of libraries.

The package name is expected in ``project_name.package_name`` format; package libraries can then be used to make [IMPORT](../export_import.md) with a module name like ``pkg.project_name.package_name.maybe.nested.module.name``.

Example for a package with a flat hierarchy consisting of two libraries, foo.sql and bar.sql:

```yql
PRAGMA package({{yql.pages.syntax.pragma.package}});
IMPORT pkg.project.package.foo SYMBOLS $foo;
IMPORT pkg.project.package.bar SYMBOLS $bar;
SELECT $foo, $bar;
```

In this case, you can use text parameter value substitution in the URL:

```yql
DECLARE $_path AS STRING; -- "path"
PRAGMA package({{yql.pages.syntax.pragma.package-var}});
IMPORT pkg.project.package.foo SYMBOLS $foo;
IMPORT pkg.project.package.bar SYMBOLS $bar;
SELECT $foo, $bar;
```

## OverrideLibrary

| Value type | Default value | Static/<br/>dynamic |
| --- | --- | --- |
| One argument: file name | — | Static |

Interpret the specified attached file as a library and override one of the package libraries with it.

The file name should be in the format ``project_name/package_name/maybe/nested/module/name.EXTENSION``, [PRAGMA Library](#library) extensions are supported.

Example:

```yql
PRAGMA package({{yql.pages.syntax.pragma.package}});
PRAGMA override_library("project/package/maybe/nested/module/name.sql");

IMPORT pkg.project.package.foo SYMBOLS $foo;
SELECT $foo;
```

{% if audience == "internal" %}

  {% note warning %}

  For PRAGMA `Folder`, only links to {{yql.pages.syntax.pragma.folder-note}} with resources containing the directory are supported.

  {% endnote %}

{% endif %}

  {% note warning %}

  For PRAGMA `Package`, only links to directories on {{product-name}} clusters are supported.

  {% endnote %}

