---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/files.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/files.md
---
## FileContent and FilePath {#file-content-path}

You can add arbitrary named files to your query both in the console and web interfaces. With these functions, you can use the name of the attached file to get its contents or the path in the sandbox, and then use it as you like in the query.

**Signatures**
```
FilePath(String)->String
FileContent(String)->String
```

The `FileContent` and `FilePath` argument is a string with an alias.

**Examples**
```yql
SELECT "Content of "
  || FilePath("my_file.txt")
  || ":\n"
  || FileContent("my_file.txt");
```
## FolderPath {#folderpath}

Getting the path to the root of a directory with several "attached" files with the common prefix specified.

**Signature**
```
FolderPath(String)->String
```

The argument is a string with a prefix among aliases.

See also [PRAGMA File](../../../syntax/pragma.md#file) and [PRAGMA Folder](../../../syntax/pragma.md#folder).

**Examples**
```yql
PRAGMA File("foo/1.txt", "http://url/to/somewhere");
PRAGMA File("foo/2.txt", "http://url/to/somewhere/else");
PRAGMA File("bar/3.txt", "http://url/to/some/other/place");

SELECT FolderPath("foo"); -- The directory at the return path will
                          -- include the files 1.txt and 2.txt downloaded by the above links
```

## ParseFile

Get a list of values from the attached text file. You can use it together with [IN](../../../syntax/expressions.md#in) and file attachment using URL <span style="color:gray;">(instructions on how to attach files in the web interface and in the client </span>.

Only one file format is supported: one value per line. 
<!-- For something more sophisticated, you should write a small UDF in [Python](../../../udf/python.md) or [JavaScript](../../../udf/javascript.md). -->

**Signature**
```
ParseFile(String, String)->List<T>
```

Two required arguments:

1. List cell type: only strings and numeric types are supported.
2. The name of the attached file.

{% note info "Note" %}

The return value is a lazy list. For repeat use, wrap it in the function [ListCollect](../../list.md#listcollect)

{% endnote %}

**Examples:**
```yql
SELECT ListLength(ParseFile("String", "my_file.txt"));
```
```yql
SELECT * FROM my_table
WHERE int_column IN ParseFile("Int64", "my_file.txt"));
```
