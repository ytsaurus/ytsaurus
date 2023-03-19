---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/streaming.md
sourcePath: yql-product/udf/list/streaming.md
---
# Streaming UDF

Functions designed to launch an arbitrary script or executable in the streaming mode:
```yql
Streaming::Process(List<Struct<Data:String>>, String, List<String>) -> List<Struct<Data:String>>
Streaming::ProcessInline(List<Struct<Data:String>>, String, List<String>) -> List<Struct<Data:String>>
```
Argument description:

1. Input data (`List<Struct<Data:String>>`):
   * A child process is launched once in the beginning of list processing (or a part of it, see the next section), and each element is fed to it via stdin in separate lines.
   * If the input list is a large table, many identical processes are launched to process its parts in parallel. Processing of all table data in one process and the order of line processing are not guaranteed.
   * `Process` and `ProcessInline` expect each line in the input data list to be wrapped in a structure. It simplifies using `SELECT` because this is the type of `SELECT ... AS Data FROM ...`. Also, it helps discourage the user from choosing a very inefficient scenario in which a separate process is launched for each line of the source MapReduce table.
   * The `Data` name for the list item structure is fixed at the time of writing.
2. The second argument (`String`):
   * `Streaming::Process` is the program name from `$PATH` (you must ensure that it's installed on the target cluster) or the executable path that can be attached to the query and then receive the path via the built-in `FilePath("file alias")` function.
   * `Streaming::ProcessInline` is the line with the script that will be launched using the interpreter specified in [shebang](https://en.wikipedia.org/wiki/Shebang_(Unix)) (i.e. the first line of the script must read like `#!/usr/bin/env bash`, for example). Basic interpreters, such as sh, bash, awk, perl, or python, are usually already installed. If you need something unconventional, you can attach it to your query. The script can be defined inline using a multiline string literal (`@@`) or in a separate file (a tab in the web interface). In the second case, you can get a file's content in a line using the built-in `FileContent("file alias")` function.
3. The list of arguments to be passed to the script or executable when launching (`List<String>`, optional argument):
   * An easy way to create a list of arguments using literals is to use the built-in `AsList` function (accepts 1 or more arguments and creates a list of them).
   * If you don't need to pass arguments, skip them.
      If you're interested in a specific interaction with a child process that's different from the line-by-line approach described above, tell us about your use scenario in the [yql@ mailing list](http://ml.yandex-team.ru/lists/yql).

Notes on implementation:

* `Streaming::ProcessInline` usually uses the interpreter installed on the target cluster, which might lead to unexpected results in case of version conflict. To avoid this, you can attach your interpreter to the query (which isn't always straightforward) or ask the cluster administrator to install the required interpreter or its version (it's better if you contact them directly, but you can also ask us for help).
* stderr can be used for troubleshooting issues: some part of it is saved and returned to the client in case of a runtime error when running the query. The size of the saved part of stderr is configured on the cluster side and usually is several kilobytes.

<!--See a detailed example in the [tutorial](https://cluster-name.yql/Tutorial/yt_23_Embedded_streaming). Short examples are provided below.-->


**Examples**

```yql
$input = [
  <| "Data":"a" |>,
  <| "Data":"b" |>,
  <| "Data":"c" |>,
];

PROCESS AS_TABLE($input) USING Streaming::Process(TableRows(), "cat", ["-n"]);

/*
[(Data: ' 1 a'),
(Data: ' 2 b'),
(Data: ' 3 c')]
*/
```

```yql
$ips = (SELECT COALESCE(ip, "") AS Data FROM hahn.`home/yql/tutorial/users`);

$script = @@#!/usr/bin/awk -f
BEGIN { FS="."; }
$2 ~ /170/ { print $0" has 170 in second octet!"; }
@@;

PROCESS $ips
USING Streaming::ProcessInline(
  TableRows(),
  $script
);
/*
"93.170.111.29 has 170 in second octet!"
"93.170.111.28 has 170 in second octet!"
*/
```
