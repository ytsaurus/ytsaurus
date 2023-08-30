## Python API

{% note info "Note" %}

Before you start, install the Python client from the pip repository using the command:

```bash
pip install ytsaurus-client
```

{% endnote %}

What becomes available after installing the package:

- The Python yt library.
- The binary yt todo file.
- The binary yt-fuse file for connecting [Cypress](../../../user-guide/storage/cypress.md) as a file system locally.

### Installation { #install }

#### YSON libraries

To use the YSON format to work with tables, you need C++ bindings installed as a separate package. Installing [YSON bindings](../../../api/python/userdoc.md#yson_bindings):

```bash
pip install ytsaurus-yson
```

{% note warning "Attention!" %}

It is currently impossible to install YSON bindings on Windows.

{% endnote %}

{% note info "For Apple M1 platform users" %}

There are currently no YSON bindings built for the Apple platform. You can use [Rosetta 2](https://en.wikipedia.org/wiki/Rosetta_(software)) as a temporary solution and install the Python version for the x86_64 architecture.

Learn more [here](https://stackoverflow.com/questions/71691598/how-to-run-python-as-x86-with-rosetta2-on-arm-macos-machine).


{% endnote %}

To learn more about YSON, see [Formats](../../../api/python/userdoc.md#formats).

To find out the version of the installed Python wrapper, print the `yt.VERSION` variable or call the `yt --version` command.

If you encounter a problem, check the [FAQ](#faq) section. If the problem persists, write to the [chat](https://t.me/ytsaurus_ru).

[Library source code](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/yt/wrapper).

{% note warning "Attention!" %}

We do not recommend installing the library and its dependent packages in different ways at the same time. This can lead to problems that are difficult to diagnose.

{% endnote %}

### User documentation { #userdoc }
* [General](../../../api/python/userdoc.md#common)
   - [Agreements used in the code](../../../api/python/userdoc.md#agreements)
   - [Client](../../../api/python/userdoc.md#client)
      - [Thread safety](../../../api/python/userdoc.md#threadsafety)
      - [Asynchronous client based on gevent](../../../api/python/userdoc.md#gevent)
   - [Configuration](../../../api/python/userdoc.md#configuration)
      - [Shared config](../../../api/python/userdoc.md#configuration_common)
      - [Logging setup](../../../api/python/userdoc.md#configuration_logging)
      - [Token setup](../../../api/python/userdoc.md#configuration_token)
      - [Setting up retries](../../../api/python/userdoc.md#configuration_retries)
   - [Errors](../../../api/python/userdoc.md#errors)
   - [Formats](../../../api/python/userdoc.md#formats)
   - [YPath](../../../api/python/userdoc.md#ypath)
* [Teams](../../../api/python/userdoc.md#commands)
   - [Working with Cypress](../../../api/python/userdoc.md#cypress_commands)
   - [Working with files](../../../api/python/userdoc.md#file_commands)
   - [Working with tables](../../../api/python/userdoc.md#table_commands)
      - [Data classes](../../../api/python/userdoc.md#dataclass)
      - [Schemas] { ../../../api/python/userdoc.md#table_schema }
      - [TablePath](../../../api/python/userdoc.md#tablepath_class)
      - [Teams](../../../api/python/userdoc.md#table_commands)
      - [Parallel table reading](../../../api/python/userdoc.md#parallel_read)
      - [Parallel table writing](../../../api/python/userdoc.md#parallel_write)
   - [Working with transactions and locks](../../../api/python/userdoc.md#transaction_commands)
   - [Running operations](../../../api/python/userdoc.md#run_operation_commands)
      - [SpecBuilder](../../../api/python/userdoc.md#spec_builder)
   - [Working with operations and jobs](../../../api/python/userdoc.md#operation_and_job_commands)
      - [Operation](../../../api/python/userdoc.md#operation_class)
      - [OperationsTracker](../../../api/python/userdoc.md#operations_tracker_class)
      - [OperationsTrackerPool](../../../api/python/userdoc.md#operations_tracker_pool_class)
   - [Working with access permissions](../../../api/python/userdoc.md#acl_commands)
   - [Working with dynamic tables](../../../api/python/userdoc.md#dyntables_commands)
   - [Other commands](../../../api/python/userdoc.md#etc_commands)
* [Python objects as operations](../../../api/python/userdoc.md#python_operations)
   - [General information](../../../api/python/userdoc.md#python_operations_intro)
   - [Preparing an operation from a job](../../../api/python/userdoc.md#prepare_operation)
   - [Decorators](../../../api/python/userdoc.md#python_decorators)
   - [Pickling functions and environments](../../../api/python/userdoc.md#pickling)
      - [General structure](../../../api/python/userdoc.md#pickling_description)
      - [Link to a post with tips](../../../api/python/userdoc.md#pickling_advises)
   - [Porto layers](../../../api/python/userdoc.md#porto_layers)
   - [tmpfs in jobs](../../../api/python/userdoc.md#tmpfs_in_jobs)
   - [Statistics in jobs](../../../api/python/userdoc.md#python_jobs_statistics)
* [Untyped Python operations](../../../api/python/userdoc.md#python_operations_untyped)
   - [Decorators](../../../api/python/userdoc.md#python_decorators_untyped)
   - [Formats](../../../api/python/userdoc.md#python_formats)
      - [Structured data representation](../../../api/python/userdoc.md#structured_data)
      - [Control attributes](../../../api/python/userdoc.md#control_attributes)
      - [Other formats](../../../api/python/userdoc.md#other_formats)
* [Other](../../../api/python/userdoc.md#other)
   - [gRPC](../../../api/python/userdoc.md#grpc)
   - [YSON bindings](../../../api/python/userdoc.md#yson_bindings)
* [Deprecated](../../../api/python/userdoc.md#legacy)
   - [Python3 and byte strings](../../../api/python/userdoc.md#python3_strings)

### Help { #pydoc }
The most up-to-date help on specific functions and their parameters is in the code.

To view a description of functions and classes in the interpreter, proceed as follows:

```bash
python
>>> import yt.wrapper as yt
>>> help(yt.run_sort)
```

### Examples { #examples }

* [About compiling Python programs in Arcadia](../../../api/python/examples.md#arcadia)
* [Basic level](../../../api/python/examples.md#base)
   - [Reading and writing tables](../../../api/python/examples.md#read_write)
   - [Table schemas](../../../api/python/examples.md#table_schema)
   - [Simple map](../../../api/python/examples.md#simple_map)
   - [Sorting a table and a simple reduce operation](../../../api/python/examples.md#sort_and_reduce)
   - [Reduce with multiple input tables](../../../api/python/examples.md#reduce_multiple_output)
   - [Reduce with multiple input and output tables](../../../api/python/examples.md#reduce_multiple_input_output)
   - [mapreduce](../../../api/python/examples.md#map_reduce)
   - [MapReduce with multiple intermediate tables](../../../api/python/examples.md#map_reduce_multiple_intermediate_streams)
   - [Decorators for job classes](../../../api/python/examples.md#job_decorators)
   - [Working with files on the client and in operations](../../../api/python/examples.md#files)
   - [Grep](../../../api/python/examples.md#grep)
* [Advanced level](../../../api/python/examples.md#advanced)
   - [Batch queries](../../../api/python/examples.md#batch_queries)
   - [RPC](../../../api/python/examples.md#rpc)
* [Miscellaneous](../../../api/python/examples.md#misc)
   - [Data classes](../../../api/python/examples.md#dataclass)
   - [Context and managing writes to output tables](../../../api/python/examples.md#table_switches)
   - [Spec builders](../../../api/python/examples.md#spec_builder)
   - [Using gevent](../../../api/python/examples.md#gevent)
* [Untyped API](../../../api/python/examples.md#untyped_tutorial)
   - [Reading and writing tables](../../../api/python/examples.md#read_write_untyped)
   - [Simple map](../../../api/python/examples.md#simple_map_untyped)
   - [Sorting a table and a simple reduce operation](../../../api/python/examples.md#sort_and_reduce_untyped)
   - [Reduce with multiple input tables](../../../api/python/examples.md#reduce_multiple_output_untyped)
   - [Reduce with multiple input and output tables](../../../api/python/examples.md#reduce_multiple_input_output_untyped)
   - [MapReduce operation](../../../api/python/examples.md#map_reduce_untyped)
   - [Decorators for job classes and functions](../../../api/python/examples.md#job_decorators_untyped)
   - [Table switches and context](../../../api/python/examples.md#table_switches_untyped)
   - [Working with strings in Python3](../../../api/python/examples.md#yson_string_proxy)

<!-- ### Для разработчика { #fordeveloper }

  * [Контрибы](for_developer.md#contribs)
  * [Разбиение библиотеки на части в Аркадии](for_developer.md#peerdirs)
  * [Устройство и запуск тестов](for_developer.md#tests)
  * [Политика обновления библиотеки](for_developer.md#update_policy) -->

### FAQ { #faq }

This section contains answers to a number of frequently asked questions about the Python API. Answers to other frequently asked questions are in the [FAQ](../../../faq/faq.md) section.

**Q: I installed the package via pypi, but I get the `yt: command not found` error.**
A: Try running the
`pip install ytsaurus-client --force-reinstall` command
, the log will most likely display a warning like `The script yt is installed in '...' which isn't on your PATH`. To solve the problem, you need to add the specified path to the PATH environment variable. To do this, run the following command:

```
echo 'export PATH="$PATH:<specified path>"' >> ~/.bashrc
source ~/.bashrc
```
Depending on the shell, the file may have a different name. The most common name on Mac is `~/.zshrc`.

**Q: Reading with retry ends with an error because of timeout.**
A: Most likely there are too many chunks in the table, you need to enlarge them. Use `yt merge --src table --dst table --spec "{combine_chunks=true}"`

**Q: The operation ends with a YSON error (for example: `YsonError: Premature end of stream`) and the web interface displays a YSON parsing error.**
A: The operation most likely writes to `stdout`. This is prohibited from being done explicitly in Python via `print, sys.stdout.write()` if the operation is not marked as `raw_io`, but it can be done by a third-party program, such as an archiver.

**Q: The Python library writes too much to stderr, how do I increase the level of logging?**
A: You can increase the level by setting the `YT_LOG_LEVEL="ERROR"` environment variable or by setting up the {{product-name}} logger: `logging.getLogger("Yt").setLevel(logging.ERROR)`.

**Q:  I start an operation on Mac OS X, but jobs end with errors like `ImportError: ./tmpfs/modules/_ctypes.so: invalid ELF header`.**
A: Since the Python wrapper takes all Python operation dependencies with it to the cluster, binary .so and .pyc files arrive there too, which then cannot be loaded. Use a porto layer with your local environment and enable filtering of these files so that they do not end up on the cluster. For more information, see the [section](../../../api/python/userdoc.md#porto_layers).

**Q: Jobs end with the `Invalid table index N: expected integer in range [A,B]` error.**
A: The message means that you output a table index in the records and there is no corresponding table. This most often means that you have several input tables and one output table. The `@table_index` fields appear in the input records by default. To disable them, you can change the format: `yt.config["tabular_data_format"] = yt.YsonFormat(process_table_index=None)`. To learn more about the format, see the [section](../../../api/python/userdoc.md#python_formats). As an alternative, explicitly indicate in the specification (example for a map operation): `{"mapper": {"enable_input_table_index": False}}`.

**Q: The (ReadTimeout, HTTPConnectionPool(....): Read timed out.) error appears after the operation is completed.**
The message means that the operation stderr could not be downloaded due to network problems and even repeated queries didn't help. In that case, you should use the `ignore_stderr_if_download_failed` option which enables you to ignore stderr if you can't download it. We recommend using this option when writing production processes.

**Q: I get the `Yson bindings required` error.**
This means that YSON was selected as the input (output) format and bindings could not be imported in the job. To learn more about YSON and bindings, see the [section](../../../api/python/userdoc.md#yson). You need to install the bindings package and check that YSON bindings are not filtered out using `module_filter`. This is a dynamic yson_lib.so library that can easily be accidentally filtered out when filtering out all .so files. In addition, so that `yt_yson_bindings` that came in modules are not deleted, write `config["pickling"]["ignore_yson_bindings_for_incompatible_platforms"] = False` in the configuration file.
