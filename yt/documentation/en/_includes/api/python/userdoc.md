## General information { #common }

### Conventions { #agreements }

Options of system commands have the default value of `None`. This means that the option value won't be transmitted at the command execution, and the system will use the default value.

For the `format` option, the default value of `None` usually has another meaning. The command must return a parsed Python structure or expect a Python structure as input. If the format option is not specified, the command must return an unparsed result or expect unparsed input. If the `format` option is not specified, the command must return an unparsed result or expect unparsed input.

All timeouts or time periods are accepted in milliseconds by default. In some cases, `datetime.timedelta` can be used as time interchangeably with milliseconds.

All the functions have the `client` object as the last option. This behavior is implementation-specific. We do not recommend using this feature on your own.

For brevity, in the below examples, we assume that `import yt.wrapper as yt` has already been performed.

### Client and global client { #client }

The functions and clauses are available from the yt.wrapper global environment of the module library and can change its global status. For example, they save the current transaction there. By changing yt.config, you're also changing the global configuration. If you want to have an option to work from several independent (that is, differently configured) clients, use the [YtClient](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient) class. This class provides almost all the functions from the [yt.wrapper](https://pydoc.ytsaurus.tech/yt.wrapper.html) module: you can call `client.run_map`, `client.read_table_structured`, `with client.Transaction()`, and so on. Note that the `YT_PROXY`, `YT_TOKEN` and other environment variables only set the configuration of the global client. That is, they affect only [yt.wrapper.config](https://pydoc.ytsaurus.tech/yt.wrapper.html#module-yt.wrapper.config) rather than the configuration of explicitly created `YtClient` instances.
??
```python
from yt.wrapper import YtClient
client = YtClient(proxy=<cluster-name>, config={"backend": "rpc"})
print(client.list("/"))
```

{% note warning "Attention!" %}

Note that the library is not thread-safe: in order to work from different threads, you need to create a client instance on each thread.

{% endnote %}

#### Command parameters { #commandparams }

You can transmit a specified set of parameters to all the requests set up through the client (for example, `trace_id`). To do this, you can use the special [create_client_with_command_params](https://pydoc.ytsaurus.tech/yt.wrapper.html?highlight=create_client_with#yt.wrapper.client.create_client_with_command_params) method that enables you to specify an arbitrary set of options to be transmitted to all the API calls.

Here is a todo example of how to use this feature for specifying prerequisites.

#### Thread safety { #threadsafety }

This tip remains valid if you are using multiprocessing, even if you aren't using threading. One of the reasons is that if you have already set up a request for the cluster in your main process, a connection pool to this cluster has already been initiated within the global client. So, when the processes fork, they're going to use the same sockets when communicating with the cluster. This might result in various issues.

{% note warning "Attention!" %}

When using a native driver, use fork + exec (via `Popen`), avoiding even `multiprocessing`, because the driver service threads aren't preserved at fork, and the driver becomes incapable of executing requests. You should also avoid overusing clients after forks, because they might have a non-trivial state that poorly survives forks.

{% endnote %}

#### Asynchronous client based on gevent { #gevent }

If `gevent` is present in your code, you can use `yt.wrapper` in the asynchronous mode. This functionality hasn't been covered by tests yet, but should be operable in simple scenarios. Key points:

- Before getting started, run monkey patching to replace blocking network calls.
- Create an independent [YtClient](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient) inside each [greenlet](https://pypi.org/project/greenlet/). This is important. If you neglect this point, you will get errors.

You can find an example in this [section](../../../api/python/examples.md#gevent).


### Configuring { #configuration }

The library supports rich configuration options determining its behavior in different scenarios. For example, you can change the path in Cypress where your temporary tables will be created by default: `yt.config["remote_temp_tables_directory"] = "//home/my_home_dir/tmp"`.
To learn about options and their detailed descriptions, see the [code](http://pydoc.ytsaurus.tech/_modules/yt/wrapper/default_config.html)??.

The library is configured via one of the following methods:
- Updating the `yt.config` object: `yt.config["proxy"]["url"] = "<cluster_name>"`.
- Calling the function `yt.update_config`: `yt.update_config({"proxy": {"url": "<cluster_name>"}})`.
- Setting an environment variable: `export YT_CONFIG_PATCHES='{url="<cluster_name>"}'`;
- Using the file whose path is stored in the `YT_CONFIG_PATH` environment variable, the default value is `~/.yt/config`. The file should be in the [YSON](../../../user-guide/storage/formats.md#yson) format  (using the  `YT_CONFIG_FORMAT` environment variable, you can change this behavior; the YSON and JSON  formats are supported). Sample file contents: `{proxy={url="<cluster_name>"}}`.

You can change some configuration options using environment variables. Such variables are: `YT_TOKEN`, `YT_PROXY`, `YT_PREFIX`. And also options for setting up the logging options (that are separate from the config): `YT_LOG_LEVEL` and `YT_LOG_PATTERN`.

When using CLI todo, you can pass the configuration patch in the `--config` option.

Be aware that the library configuration doesn't affect the client configuration, and when you are creating a client, a configuration using default values is created by default. To pass the configuration based on environment variables to the client: `client = yt.YtClient(..., config=yt.get_config_from_env())`. You can also update the current configuration by values from the environment variables using the `update_config_from_env(config)` function.

Keep an eye on the priority order. When you import the library, the configurations transmitted through `YT_CONFIG_PATCHES` are applied. This environment variable expects list_fragment: you can pass multiple configurations separated by a semicolon. These patches are applied last-to-first. Then, the values of the options specified by specific environment variables are applied. For example, this can be done using `YT_PROXY`. Only after that, the configurations explicitly specified in the code (or passed in the `--config` option) are applied.

When the config is applied, all the nodes that are dicts are merged rather than overwritten.

Setting up a global configuration to work with a cluster in your home directory.

```python
yt.config["prefix"] = "//home/username/"
yt.config["proxy"]["url"] = "cluster-name"
```

Creating a client that will create more and more new tables with the `brotli_3` codec.

```python
client = yt.YtClient(proxy=<cluster-name>, config={"create_table_attributes": {"compression_codec": "brotli_3"}})
```

Setting up a global client to collect the archives of dependencies in Python operations using a local directory that is different from `/tmp`. Then creating a client with an additional max_row_weight setting of 128 MB.

```python
my_config = yt.get_config_from_env()
my_config["local_temp_directory"] = "/home/ignat/tmp"
my_config["table_writer"] = {"max_row_weight": 128 * 1024 * 1024}
client = yt.YtClient(config=my_config)
```

#### Shared config { #configuration_common }

#### Logging setup { #configuration_logging }

This is how logging works in ytsaurus-client and all the tools that use this library. There is a special logger implemented in the `yt.logger` module both as a global `LOGGER` variable and as aliases that enable logging at the module level.

To change the logging settings, update the `LOGGER`.

The initial logger setup (when loading the module) is regulated by the `YT_LOG_LEVEL` and `YT_LOG_PATTERN` environment variables. The `YT_LOG_LEVEL` variable that regulates the logging level accepts one of the values `DEBUG`, `INFO`, `WARNING`, `ERROR`, while the `YT_LOG_PATTERN` variable, which regulates the formatting of log messages, accepts the logger's format string. For more information, see the Python [documentation](https://docs.python.org/3/library/logging.html#logging.Formatter).

By default, the logging level is INFO, and log messages are written to stderr.


#### Token setup { #configuration_token }

You can retrieve the token from the following places (listed in priority order).

1. From the `config["token"]`option that gets to the global configuration from the `YT_TOKEN` environment variable.
2. From the file specified in `config["token_path"]`; the default value of this option is `~/.yt/token`. This option can also be overridden using the `YT_TOKEN_PATH` environment variable (the environment variable only applies to the global client configuration; for details, see the [ section](#configuration)).

#### Setting up configuration retries { #configuration_retries }

Commands in {{product-name}} are classified into light and heavy (to get the system's view on the list of commands, see the section) todo.

Light commands are commands like `create, remove, set, list, get, ping`, and other similar ones. Before each request, the client also accesses a special proxy to get a list of commands that it supports. We recommend retrying such commands when errors or timeouts occur. You can set up the retry parameters in the `proxy/retries` configuration section.

There are two categories of heavy requests.

- Reading from and writing to tables or files (that is, read_table, write_table, read_blob_table, and other requests). The retries for such commands are set up in the `read_retries` and `write_retries` sections (for read requests and write requests, respectively).
- Reading from and writing to dynamic tables (that is, with heavy queries, such a select_rows, lookup_rows, insert_rows, and others). For such commands, the retries are set up individually in the `dynamic_table_retries` section (the regular options read_retries and write_retries won't work because when reading static tables, 60-second intervals between retries are considered normal, but that's inappropriate for dynamic tables where low latency is critical).

The above-mentioned retries affect the processes of data uploads or exports from the cluster in the case of network faults, chunk unavailability, and other issues. Below is a description of actions that are initiated inside the cluster (running a MapReduce operation or a batch request).

1. The `start_operation_retries` section regulates the retries of the operation start command, that is, instead of dealing with network issues, here we handle the errors like "concurrent operation limit exceeded," when many operations are running and the scheduler won't start new ones. In case of such errors, the client makes retries with big sleep intervals to allow enough time for some operations to complete.
2. The `concatenate_retries` section regulates the retries of the concatenate command (see the page with the API) todo. This is not a light command because it can access different master cells and spend a long time there. That's why you can't use the retry settings for light commands in this case.
3. The `batch_requests_retries` section regulates the retries made from inside a batch request (see the description of the `execute_batch`) command. The client retries the requests that failed with such errors as the "request rate limit exceeded". That is, the client sends a batch of requests, of which some have completed and some have failed with the "request queue size per user exceeded" errors. In this case, the requests are delivered again with a new batch. This section regulates the policy of such retries.


### Errors { #errors }

[YtError](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtError): The parent class of all the errors in the library. It has the following fields:

- `code` (int type): HTTP error code  (see the [section](../../../api/error-codes.md)). If omitted, it's equal to 1.
- `inner_errors` (the list type): Errors that preceded the given error when executing the request (the proxy accessed the driver, an error occurred inside the driver: this error will be added to `inner_errors`).
- `attributes` (the dict type): Attributes of a generic error (for example, the request date).

The following errors describe more specific issues.

[YtOperationFailedError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtOperationFailedError): The operation has failed. This error stores the following information in the `attributes` field.

- `id`: Operation ID.
- `url`: Operation URL.
- `stderrs`: List of dicts with details about the jobs that failed or jobs with stderr. This dict has the `host` field; it can also have the `stderr` and `error` fields, depending on whether the job had a stderr or had failed.
- `state`: Operation status (for example, `failed`).

To print the stderr output when an operation fails, you need to process the exception and explicitly print error messages. Otherwise, you will see them truncated in backtrace. There is a non-public decorator (you can use it, but it might be renamed) [add_failed_operation_stderrs_to_error_message](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.add_failed_operation_stderrs_to_error_message)?? that intercepts the [YtOperationFailedError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtOperationFailedError)  exception and enriches its message about the stderr error.

[YtResponseError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtResponseError): The command (that is, the request to {{product-name}}) has failed. This class has the `error` field that stores the structured response describing the error cause.
It provides the following useful methods:

- [is_resolve_error](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_resolve_error) (for example, it returns`True` on an attempt to commit a non-existing transaction or request a non-existing node).

- [is_access_denied](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_access_denied);

- [is_concurrent_transaction_lock_conflict](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_concurrent_transaction_lock_conflict);

- [is_request_queue_size_limit_exceeded](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_request_queue_size_limit_exceeded);

- [is_chunk_unavailable](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_chunk_unavailable);

- [is_request_timed_out](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_request_timed_out);

- [is_concurrent_operations_limit_reached](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_concurrent_operations_limit_reached).

Other subclasses of `YtError`:
- [YtHttpResponseError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtHttpResponseError): Subclass of [YtResponseError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtResponseError), it includes the `headers` field.

- [YtProxyUnavailable](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtProxyUnavailable): The proxy is currently unavailable - for example, it's overwhelmed with requests.
- [YtTokenError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtTokenError): An invalid token has been passed.
- [YtFormatError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtFormatError): The format is invalid.
- [YtIncorrectResponse](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtIncorrectResponse): An invalid response has been received from the proxy (for example, JSON is invalid).
- [YtTimeoutError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtTimeoutError): The operation has been terminated by a timeout.

You can look up a full list of subclasses in the code.

### Formats { #formats }

To learn more about the formats, see the [section](../../../user-guide/storage/formats.md).

There is a separate class for each format:
- [YsonFormat](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.YsonFormat);
- [JsonFormat](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.JsonFormat);
- [DsvFormat](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.DsvFormat);
- [SchemafulDsvFormat](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.SchemafulDsvFormat);
- [YamrFormat](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.YamrFormat).


In the constructor, these classes accept the parameters that are specific for the given formats. If the format option isn't explicitly available in the constructor, you can pass it in the `attributes` option that accepts a `dict`.

Moreover, each class has a set of methods for serialization or deserialization of records from the stream:

- [load_row](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.Format.load_row): Reads a single row from the stream. Some formats (for example, Yson) do not support loading a single record. That's why, when you use `load_rows` on them, they will throw an exception.
- [load_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.Format.load_rows): Reads all the records from the stream, processes the table switches, and returns a record iterator. If `raw==True`, the method returns rows without parsing them. This method is a generator.
- [dump_row](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.Format.dumps_row): Writes a single record to the stream.
- [dump_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.Format.dumps_rows): Writes a set of records to the stream. It adds table switches to the output stream.

There is also the [create_format](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.create_format) function that creates a format based on the specified name and a given set of attributes. The format name can also include attributes in YSON format: you can create a format as follows: `create_format("<columns=[columnA;columnB]>schemaful_dsv")`.

You can use the [create_table_switch](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.create_table_switch) function to create a record that functions as a switch to the table with the specified index.

The functions `read_table, write_table, select_rows, lookup_rows, insert_rows, delete_rows` have the `raw` parameter that enables you to get/send records in an unparsed format (as strings). If you set `raw` to `False`, this means that the records will be deserialized to `dict` at reading and serialized from `dict` at writing.

Example:

```python
yt.write_table("//home/table", ["x=value\n"], format=yt.DsvFormat(), raw=True)
assert list(yt.read_table("//home/table", raw=False)) == [{"x": "value"}]  # Ok
```

A similar option exists for operations. By default, this operation deserializes records from the format specified at operation startup. If you want the operation to accept unparsed strings as input, use the `raw` decorator and specify the format when running the operation:

```python
@yt.raw
def mapper(rec):
    ...

yt.run_map(...., format=yt.JsonFormat())
```

{% note warning "Attention!" %}

To use YSON format to work with tables (operations, table read/write), additionally install the package with YSON C++ bindings (see [yson bindings](#yson_bindings)). Otherwise, you will see the "Yson bindings required" exception.

{% endnote %}

Specifics of using JSON format: the JSON module is written in Python, that's why it's very slow. The library tries to use different modules that support bindings written in faster languages such as ujson. To enable it, use the [enable_ujson](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.JsonFormat) parameter: `JsonFormat(..., enable_ujson=True)`. The ujson package is disabled by default because in certain cases its default behavior is faulty:

```python
import ujson
s = '{"a": 48538100000000000000.0}'
ujson.loads(s)
{u'a': 1.1644611852580897e+19}
```

If you have read the ujson documentation, and you are sure you that your input data will be deserialized correctly, then parse using this module.


### YPath { #ypath }

Paths in {{product-name}} are represented by [YPath](../../../user-guide/storage/ypath.md). For paths in the code, you can use the [YPath](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.YPath)?? class and its subclasses, [TablePath](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.TablePath)?? and [FilePath](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.FilePath)??. Among the constructors of the last two classes, you can specify relevant YPath attributes, for example, `schema`, `start_index`, `end_index` (for `TablePath`) and `append` and `executable` (for `FilePath`). To learn more about `TablePath`, see the [section](#tablepath_class).
Use these classes for working with paths in your code instead of formatting YPath literals manually.

There are other useful functions in [YPath](https://pydoc.ytsaurus.tech/yt.wrapper.html#module-yt.wrapper.ypath) modules, here are some of them:
- [ypath_join](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.ypath_join) joins several paths into one path (a counterpart of `os.path.join`).
- [ypath_split](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.ypath_split): Returns the `(head, tail)` pair, where `tail` is the trailing path component and`head` is the remaining path (a counterpart of `os.path.split`).
- [ypath_dirname](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.ypath_dirname): Returns the path without the trailing component (a counterpart of `os.path.dirname`).
- [escape_ypath_literal](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.escape_ypath_literal): Escapes the string so that it can be used as a YPath component.


### Python3 and byte strings { #python3_strings }

In {{product-name}}, all the strings are byte strings. However, in Python, regular strings are Unicode-based. That's why the following feature for structured data (dicts) has been introduced in Python 3. When writing to and reading from tables in `raw` mode, the library uses binary strings both for input and output.
In non-`raw` mode, the following logic is enabled.

When read, strings are automatically utf8-encoded, including dict keys (to select a different encoding, use the `encoding` parameter in the format, see below). If a byte string is read, a special [YsonStringProxy](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.yson_types.YsonStringProxy) object is returned. If you try to treat such an object as a string, you will get a typical error: [NotUnicodeError](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.yson_types.NotUnicodeError). You can call two functions on such an object: [yt.yson.is_unicode](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.yson_types.is_unicode) and [yt.yson.get_bytes](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.yson_types.get_bytes). The first function will return `False` for `YsonStringProxy`, and the second function will return raw bytes that couldn't be decoded. You can call the same functions on regular strings, and for them `is_unicode` will return `True` as expected. When calling `get_bytes` on regular strings, as the second argument, you can specify the encoding (`utf-8` by default) and get `s.encode(encoding)`.

Here's the assumed scenario for mixed, regular, and byte structures when reading tables or running operations:
```python
## if you simply need bytes
b = yt.yson.get_bytes(s)

## if you need to do respond differently to byte strings and regular strings
if yt.yson.is_unicode(s):
    # Process a regular string
else:
    b = yt.yson.get_bytes(s)
    # Process a byte string
```
When writing data, you can both leave the `YsonStringProxy`  object as it is (it will be automatically converted to a byte string) or return a byte string or Unicode string. Unicode strings will be encoded in UTF-8 (or other encoding).

**Note that** you can't mix `bytes` and `str` in dict keys. When`encoding != None`, the only way to specify a byte key is to use the [yt.yson.make_byte_key](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.yson_types.make_byte_key)?? function. The reason is that in Python 3, the strings `"a"` and `b"a"` are not equal. It is unacceptable for a dict in the format `{"a": 1, b"a": 1}` to be sent to the system implicitly, converted to a string with two identical `a` keys.

If needed, you can disable the decoding logic or select another encoding. Use the `encoding` parameter when creating a format in this case. If the `encoding` parameter is specified and is `None`, the library works with the records where all the rows are expected to be binary rows (both keys and values, both on write and on read). If you attempt to serialize a dict that includes Unicode strings with `encoding=None`, in most cases, you'll see an error.

If the `encoding` parameter is specified and is not `None`, the library handles Unicode strings, but expects the specified encoding from all data (instead of UTF-8 used by default).

An example of use with comments is [in a separate section](../../../api/python/examples.md#yson_string_proxy).

### Batch requests

Low-level API:

- [execute_batch](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.execute_batch): Accepts a set of request descriptions in the list format and returns a set of results. A simple wrapper for the API command (todo).

High level API:

- [create_batch_client](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.batch_helpers.create_batch_client): Creates a batch client. This function is available both on the client side and globally. The batch client has as its methods all the API functions supported in the batch requests.
- [batch_apply](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.batch_apply): This method enables you to apply a function to a dataset, in batch mode. Returns a list of results.

Example:

```python
client = yt.YtClient("cluster-name")

batch_client = client.create_batch_client()
list_rsp = batch_client.list("/")
exists_rsp = batch_client.exists("/")
batch_client.commit_batch()

print(list_rsp.get_result())
print(exists_rsp.get_result())
```

For more information about batch requests, see the [tutorial](../../../user-guide/storage/batch-requests.md).

Specifics of working with the batch client:

- Both the [create_batch_client](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.create_batch_client)?? method and the client configuration has the `max_batch_size` parameter with a default value of 100. When the [commit_batch](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.batch_client.BatchClient.commit_batch) method is called on the client side, the requests are broken down into parts, `max_batch_size` each, and are executed in a step-by-step manner. This is because of natural restrictions on the request size.

- All the requests are sent with the client transaction from which the batch-client has been constructed. If the client is used inside a transaction, all its requests are executed in the context of this transaction. The batch client behaves similarly.

- By default, you can handle all the errors that arise in requests by looking at [BatchResponse](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.batch_response.BatchResponse)?? returned by the batch client methods.

   Example:

   ```python
   client = yt.YtClient("cluster-name")
   batch_client = client.create_batch_client()
   list_rsp = batch_client.list("//some/node")
   if list_rsp.is_ok():
       # ...
   else:
       error = yt.YtResponseError(list_rsp.get_error())
       if error.is_resolve_error():
           # Handle the situation when the node for which the list has been created doesn't exist
           # ...
   ```

The [create_batch_client](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.create_batch_client) method has the `raise_errors` parameter that you can set to `True`. Then, if at least one request fails, the [YtBatchRequestFailedError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.batch_execution.YtBatchRequestFailedError) exception will be thrown with all the errors. Example:

```python
client = yt.YtClient("cluster-name")
batch_client = client.create_batch_client(raise_errors=True)
list_rsp = batch_client.list("//some/node")
try:
    batch_client.commit_batch()
except yt.YtBatchRequestFailedError as err:
   # Print the error message
   print err.inner_errors[0]["message"]  # "Error resolving path //some/node"
```


## Commands { #commands }

The yt library makes the todo systems available in the Python API. The public part of the library includes only the methods that are in `yt/wrapper/__init__.py` and `yt/yson/__init__.py`.

Some command options are shared by command classes. For more information, see the [section](../../../api/commands.md).

### Working with Cypress { #cypress_commands }

- [get](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.get): Get the value of the Cypress node. [Read more](../../../user-guide/storage/cypress-example.md#get).

- [set](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.set): Write a value to the Cypress node. [Read more](../../../user-guide/storage/cypress-example.md#set).

- [create](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.create): Create an empty Cypress node of the `type` and with `attributes`. [Read more](../../../user-guide/storage/cypress-example.md#create).

- [exists](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.exists): Check whether the Cypress node exists. [Read more](../../../user-guide/storage/cypress-example.md#exists).

- [remove](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.remove): Delete the Cypress node. [Read more](../../../user-guide/storage/cypress-example.md#remove).

- [list](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.list): Get a list of children for the `path` node. The `absolute` option enables the output of absolute paths instead of relative paths. [Read more](../../../user-guide/storage/cypress-example.md#list).

Examples:

```python
yt.create("table", "//home/table", attributes={"mykey": "myvalue"}) # Output: <id of the created object>
yt.get("//home/table/@mykey")  # Output: "myvalue"
yt.create("map_node", "//home/dir") # Output: <id of the created object>
yt.exists("//home/dir")  # Output: True

yt.set("//home/string_node", "abcde")
yt.get("//home/string_node")  # Output: "abcde"
yt.get("//home/string_node", format="json")  # Output: '"abcde"'

yt.set("//home/string_node/@mykey", "value")  # Set the attribute
yt.get("//home/string_node/@mykey")  # Output: "value"

## Create a list and append to it the number 7 and a string
yt.create("list_node", "//home/lst")
yt.set("//home/lst/end", 7)
yt.set("//home/lst/end", "cabbage")
yt.get("//home/lst")  # Output: [7L, "cabbage"]
```

- [copy](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.copy)?? and [move](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.move): Copy/move the Cypress node. To learn more about the option value, see [section](../../../user-guide/storage/cypress-example.md#copy_move).

- [link](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.link): create a symlink to a Cypress node. [Read more](../../../user-guide/storage/cypress-example.md#link).
   To learn where the symlink points, read the value from the `@path` attribute. To access a `link` object, add `&` at the end of the path.

Examples:

```python
yt.create("table", "//home/table")
yt.copy("//home/table", "//tmp/test/path/table")
## error
yt.copy("//home/table", "//tmp/test/path/table", recursive=True)
yt.get("//home/table/@account")
## Output: sys
yt.get("//tmp/test/path/table/@account")
## Output: tmp

yt.move("//home/table", "//tmp/test/path/table")
## error
yt.move("//home/table", "//tmp/test/path/table", force=True)
yt.get("//tmp/test/path/table/@account")
## Output: sys

yt.link("//tmp/test/path/table", "//home/table")
yt.get("//home/table/@path")
##Output: "/tmp/test/path/table"
```

An alias function for creating a directory.

- [mkdir](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.mkdir)?? : Creates a directory, that is, a node of the `map_node` type.

{% cut "Alias functions for working with attributes" %}

- [get_attribute](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.get_attribute)??
- [has_attribute](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.has_attribute)??
- [set_attribute](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.set_attribute)??
- [remove_attribute](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.remove_attribute)??

Note that these functions do not support access to nested attributes by design.
To access nested attributes, use regular Cypress verbs and navigation using [YPath](../../../user-guide/storage/ypath.md).

{% endcut %}

Merging files/tables:

- [concatenate](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.concatenate)?? : merges chunks from tables or files.

Other commands:

- [find_free_subpath](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.find_free_subpath)?? : Searches a free node whose path begins with `path`.

- [search](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.search)?? : recursively traverses a subtree growing from the root node. By default, it outputs the absolute paths of all the nodes of the subtree. There's also a number of filters that allow you to select specific records. The `attributes` option specifies a list of attributes that must be retrieved with each node. The retrieved attributes are available in the `.attributes` field on the returned paths.

   Example:

   ```python
   for table in yt.search(
       "//home",
       node_type=["table"],
       object_filter=lambda obj: obj.attributes.get("account") == "dev",
       attributes=["account"],
   ):
       print(table)
   ```


### Working with files { #file_commands }

For more information about files in Cypress, see [section](../../../user-guide/storage/files.md).

- [read_file](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.read_file)??

   Read a file from Cypress to a local machine. Returns the [ResponseStream](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.response_stream.ResponseStream) object, which is a line iterator that has the following additional methods:

   - [read](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.response_stream.ResponseStream.read): Read `length` bytes from the stream. If `length==None`, read the data to the end of the stream.
   - [readline](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.response_stream.ResponseStream.readline): Read a line (including "\n").
   - [chunk_iter](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.response_stream.ResponseStream.chunk_iter): An iterator by response chunks.

   The command supports retries (enabled by default). To enable/disable or increase the number of retries, use the `read_retries` configuration option (see `read_table`). To enable read retries, use the `YT_RETRY_READ=1` variable.

- [write_file](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.write_file)
   Write a file to Cypress. The command accepts a stream from which it reads data. The command supports retries. To set up retries, use the `write_retries` configuration option (more in [write_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.write_table)). There is the `is_stream_compressed` parameter that says that the stream data has already been compressed, and you can transmit it without compressing.

- Files can be transmitted as arguments of an operation. In that case, they are written to the root of the directory where your jobs will be run. For more information, see the [section](#run_operation_commands) and the [example](../../../api/python/examples.md#files).

- [get_file_from_cache](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.get_file_from_cache)??
   Returns a path to the cached file based on the specified md5 sum

- [put_file_to_cache](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.put_file_to_cache)??
   Upload the file existing at the given Cypress path to the cache. Note that the file should be uploaded to Cypress with a special option that enables md5 calculation.

### Working with tables { #table_commands }

For more information about tables, see [Static tables](../../../user-guide/storage/static-tables.md).

#### Data classes { #dataclass }
The main method used to represent table rows is to use classes with fields annotated by types (similar to [dataclasses](https://docs.python.org/3/library/dataclasses.html)). With this approach, you can effectively (de)serialize data, avoid errors, and work with complex types (structures, lists, and others) more conveniently. To define a data class, use the `yt_dataclass` decorator. For example:

```python
@yt.yt_dataclass
class Row:
    id: int
    name: str
    robot: bool = False
```

The field type comes after the colon. This might be a regular Python type or a type from the [typing](https://docs.python.org/3/library/typing.html) module or a special type such as `OtherColumns`. For more information, see the [Data classes](dataclass.md#types) section. Just as in the standard `dataclasses` module, you can create objects in the usual way: `row = Row(id=123, name="foo")`. In that case, for all the fields without default values (as for `robot: bool = False`), you need to pass relevant fields to the constructor. Otherwise, an exception will arise.

The data classes support inheritance. For more information, see the [Data classes](dataclass.md) section todo. See also the [example](examples.md#dataclass).

#### Schemas { #table_schema }

Each table in {{product-name}} has a [schema](../../../user-guide/storage/static-schema.md). The Python  API has a dedicated [TableSchema](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.schema.table_schema.TableSchema) class. Most often, a schema is created from a [data class](#dataclass): `schema = TableSchema.from_row_type(Row)`, for example, a schema can be created automatically when writing a table.  Sometimes, you need to build a schema manually. To do this easily, use a builder interface such as:

```python
import yandex.type_info.typing as ti

schema = yt.TableSchema() \
    .add_column("id", ti.Int64, sort_order="ascending") \
    .add_column("name", ti.Utf8) \
    .add_column("clicks", ti.List[
        ti.Struct[
            "url": ti.String,
            "ts": ti.Optional[ti.Timestamp],
        ]
    ])
```

The column type should be a type from the [type_info](https://github.com/ytsaurus/ytsaurus/tree/main/library/python/type_info) library.

Composite types (`Optional`, `List`, `Struct`, `Tuple`, etc.) are set up using square brackets.

You can specify a schema when creating or writing to (an empty) table (in the `schema` attribute of the [TablePath](#tablepath_class) class). To get a table schema, use:

```python
schema = TableSchema.from_yson_type(yt.get("//path/to/table/@schema"))
```

#### TablePath { #tablepath_class }

All the commands used with tables (including operations), are not only accepting string as the input and output tables, but also the [TablePath](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.TablePath) class (where appropriate). This class represents a path to a table with certain modifiers (a wrapper on [YPath](../../../user-guide/storage/ypath.md) for tables). Its constructor accepts:

- `name`: Path to a table in Cypress.
- `append`: Append records to the table instead of overwriting it.
- `sorted_by`: Set of columns by which the table should be sorted when written to.
- `columns`: List of selected columns.
- `lower_key, upper_key, exact_key`: Lower/upper/exact read boundary defined by a key. Used with sorted tables only.
- `start_index, end_index, exact_index`: Lower/upper/exact read boundary defined by row indexes.
- `ranges`: Specify an arbitrary set of ranges to be read.
- `schema`: A [table schema](#table_schema); it makes sense when creating a table or writing to an empty or non-existing table.
- `attributes`: Set any additional attributes.

Ranges are semi-intervals (that is, they do not include the upper boundary). Note that some modifiers make sense only when you read data from a table (all the attributes related to ranges or columns), and some modifiers can only be used when writing to tables (append, sorted_by). As `name`, you can pass a string with ypath modifiers and ypath attributes, they will be read correctly and put to the `attributes` field. In the [TablePath](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.TablePath)?? object, the `attributes` field is both readable and writable.

Example:

```python
@yt.yt_dataclass
class Row:
    x: str

table = "//tmp/some-table"
yt.write_table_structured(table, Row, [Row(x="a"), Row(x="c"), Row(x="b")])
yt.run_sort(table, sort_by=["x"])
ranged_path = yt.TablePath(table, lower_key="b")
list(yt.read_table_structured(ranged_path, Row))
## Output: [Row(x='b'), Row(x='c')]
```

#### Commands { #table_commands }

- [create_temp_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.create_temp_table)??

   Creates a temporary table in the `path` directory with the `prefix`. If `path` is omitted, the directory will be taken from the config: `config["remote_temp_tables_directory"]`. For convenience, there's a wrapper that supports with_statement and accepts the same parameters as it.
   Example:

   ```python
   with yt.TempTable("//home/user") as table1:
       with yt.TempTable("//home/user", "my") as table2:
           yt.write_table_structured(table1, Row, [Row(x=1)])
           yt.run_map(..., table1, table2, ...)
   ```

- [write_table_structured](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.write_table_structured)??

   It writes the rows of `row_type` (it must be a [`yt_dataclass`](#dataclass)) from `input_stream` to the `table`.
   If the table is missing, first it is created together with a schema.     The command supports retries. You can set up retries using the `write_retries` config option.

   {% note warning "Attention!" %}

   Writing with retries consumes more memory than regular writing because the write operation buffers the rows written into chunks before writing
   (if a chunk fails to be written, a retry occurs). The default size of each chunk is 520 MB (see the [configuration option](https://github.com/ytsaurus/ytsaurus/blob/49f99ef8659f108f94d2d086f5f1dacfddb6b553/yt/python/yt/wrapper/default_config.py#L535)).

   {% endnote %}

   With the `table_writer` options, you can specify a number of system [write parameters](../../../user-guide/storage/io-configuration.md). To write raw or compressed data, use the `write_table` function.

   Example:
   ```python
   @yt.yt_dataclass
   class Row:
       id: str
       ts: int

   yt.write_table_structured("//path/to/table", Row, [Row(id="a", ts=10)])
   ```

   When writing to an empty or non-existing table, the schema is created automatically.
   In more complex cases, you might need to build the schema manually. For more information, see the [section](#table_schema) and the [example](../../../api/python/examples.md#table_schema).

- [read_table_structured](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.read_table_structured)??

   Read the table as a sequence of rows of `row_type` (it must belong to [`yt_dataclass`](#dataclass)).
   The command supports retries (enabled by default). You can set up retries using the `read_retries` configuration option.
   The `table_reader` (dict) option enables you to specify a number of system [read parameters](../../../user-guide/storage/io-configuration.md#table_reader).
   The `unordered` (bool) option enables you to request unordered reading. In that case, the data might be read faster, but the read order isn't guaranteed.
   The `response_parameters` (dict) option enables you to send a dict to it. This dict will be appended by special read command parameters (currently, there are two such parameters: `start_row_index` and `approximate_row_count`)??.

   The iterator returned supports the `.with_context()` method that returns an iterator on the `(row, ctx)` pairs. The second item enables you to get the indexes of the current row and range using the `ctx.get_row_index()` and `ctx.get_range_index()` methods (a similar iterator inside the job  also enables you to get the table index: `ctx.get_table_index()`). See examples in the tutorial showing the context in [regular reading](../../../api/python/examples.md#read_write) and [inside operations](../../../api/python/examples.md#table_switches).

   A few more words about reading with retries: in the event of retries, a transaction is created in the current context and a snapshot lock is taken on the table. The lock holds until the whole data stream is read or `.close()` is called on the stream or iterator. This behavior can result in different errors. For example, the following code **won't** work: because of the nested read transaction you won't be able to commit the transaction that has been explicitly created within the code (the nested read transaction hasn't been completed because `read_table_structured` created an iterator that hasn't been used).

   ```python
   with Transaction():
       write_table_structured(table, Row, rows)
       read_table_structured(table, Row)
   ```

Examples:

```python
@yt.yt_dataclass
class Row:
    id: str
    ts: int

yt.write_table_structured("//path/to/table", Row, [Row(id="a", ts=1)])
assert list(yt.read_table_structured("//path/to/table", Row) == [Row(id="a", ts=1)])

ranges = [
    {"lower_limit": {"key": ["a"]}, "upper_limit": {"key": ["b"]}},
    {"lower_limit": {"key": ["x"]}, "upper_limit": {"key": ["y"]}},
]
path = yt.TablePath("//other/table", columns=["time", "value"], ranges=ranges)
rows = yt.read_table_structured(path, Row)
for row, ctx in rows.with_context():
    # ctx.get_row_index() – index of row in table.
    # ctx.get_range_index() – index of range from requested ranges.
    # ...
```

Alias functions for working with tables:

- [row_count](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.row_count)?? : Returns the number of records in the table
- [is_empty](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.is_empty)?? : Checks whether the table is empty
- [is_sorted](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.is_sorted)?? : Checks whether the table is sorted

Examples:

```python
yt.write_table_structured("//home/table", Row, [Row(id="a", ts=2), Row(id="b", ts=3)])

sum = 0
for row in yt.read_table_structured("//home/table", Row):
    sum += row.ts
print(sum)  # Output: 5

yt.is_empty("//home/table")  # Output: False
yt.row_count("//home/table")  # Output: 2
yt.is_sorted("//home/table") # Output: False
```

- [write_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.write_table)??
   A non-typed analog of `write_table_structured`, **should be avoided**.

   {% cut "Read more" %}

   Writes rows from `input_stream` to the `table`.
   If the table is missing, it is created first. You can set up retries using the `write_retries` config option.
   See the [example](../../../api/python/examples.md#read_write_untyped) in the dedicated section.

   {% note warning "Attention!" %}

   Writing with retries consumes more memory than regular writing because the write operation buffers the rows written into chunks before writing
   (if a chunk fails to be written, then a retry is made). The default size of each chunk is 520 MB (see the [configuration option](https://github.com/ytsaurus/ytsaurus/blob/49f99ef8659f108f94d2d086f5f1dacfddb6b553/yt/python/yt/wrapper/default_config.py#L535)).

   {% endnote %}

   With the `table_writer` options, you can specify a number of system [write parameters](../../../user-guide/storage/io-configuration.md). There is also the `is_stream_compressed` parameter that says that the stream data has already been compressed, and you can transmit them without prior compression. Keep in mind that when transmitting compressed data, you need to specify `Content-Encoding` using the configuration: `config["proxy"]["content_encoding"] = "gzip"`, as well as set the `raw=True` option. The `raw` option regulates in which format the data is expected. If `raw=False`, an iterator by Python structures is expected. If `raw=True`, then a string is expected (or a string iterator) or a stream with the data in the `format`.

   Examples:

   ```python
   yt.write_table("//path/to/table", ({"a": i, "b": i * i} for i in xrange(100)))
   yt.write_table("//path/to/table", open("my_file.json"), format="json", raw=True)
   yt.write_table("//path/to/table", "a=1\na=2\n", format="dsv", raw=True)
   ```
   To write a table with a schema, you need to create it separately first.
   ```python
     schema = [
         {"name": "id", "type": "string"},
         {"name": "timestamp", "type": "int64"},
         {"name": "some_json_info", "type": "any"},
     ]
     yt.create("table", "//path/to/table", attributes={"schema": schema})
     yt.write_table("//path/to/table", rows)
   ```

   {% endcut %}


- [read_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.read_table)
   A non-typed analog of `read_table_structured`, **should be avoided**.

   {% cut "Details" %}

   Reads the table using the specified format. The value returned depends on the `raw` option. If `raw=False` (default value), an iterator by a list of records is returned. One record is a `dict` or `Record` (in the case of the `yamr` format). If `raw=True`, a stream-like object is returned from which you can read data in the `format`.
   The command supports retries (enabled by default). You can set up retries using the `read_retries` configuration option. When retries are enabled, reading is slower because of the need to parse the stream as you need to count the number of previously read rows.
   The `table_reader` (dict) option enables you to specify a number of system [read parameters](../../../user-guide/storage/io-configuration.md#table_reader).
   With the `control_attributes` (dict) option, you can request a number of [control attributes](../../../user-guide/storage/io-configuration.md#control_attributes)when reading data
   The `unordered` (bool) option enables you to request unordered reading. In that case, the data might be read faster, but the read order isn't guaranteed.
   The `response_parameters` (dict) option enables you to send a dict to it. This dict will be appended by special read command parameters (in the current implementation, the two parameters are: start_row_index and approximate_row_count)??.

   See the [example](../../../user-guide/storage/examples.md#read_write_untyped) in the dedicated section.

   A few more words about reading with retries: in this case, a transaction is created in the current context and a snapshot lock is taken on the table. This lock holds until you've read the entire data stream or call `.close()` on the stream or the iterator returned. Such a behavior can result in errors. For example, the following code **won't** work: because of the nested read transaction, you won't be able to commit the transaction explicitly created within the code (the nested read transaction hasn't been completed because [read_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.read_table) created an iterator that hasn't been used).

   ```python
   with Transaction():
       write_table(table, rows)
       read_table(table, format="yson")
   ```

   {% note warning "Attention!" %}

   Reading with retries usually works slower than without them because you need to parse a stream of records.

   {% endnote %}

   {% endcut %}


#### Parallel reading of tables and files { #parallel_read }

The table is broken down into smaller ranges, assuming that the data is evenly distributed across rows. Each range is considered a separate stream. When you enable retries, the entire range, rather than individual table rows, will be retried. This approach enables you to skip data parsing and streamline the reading process.
The files are simply split into chunks of the specified size and read in parallel.

The configuration of parallel reading are stored in the `read_parallel` configuration section that includes the following keys:
- `enable`: Enable parallel reading.
- `max_thread_count`: Maximum number of threads.
- `data_size_per_thread`: Amount of data loaded by each data thread.

Specifics of parallel reading of data:

- Requesting control attributes is not supported.
- Table rows with limits set by keys aren't supported.
- Parallel reading might be ineffective on tables that substantially vary in row sizes.

Acceleration example:

```bash
time yt read-table //sys/scheduler/event_log.2[:#1000000] --proxy cluster-name --format yson > /dev/null

real    1m46.608s
user    1m39.228s
sys     0m4.216s

time yt read-table //sys/scheduler/event_log.2[:#1000000] --proxy cluster-name --format yson --config "{read_parallel={enable=%true;max_thread_count=50;}}" > /dev/null

real    0m14.463s
user    0m12.312s
sys     0m4.304s
```

{% cut "Measuring the speed" %}

```bash
export YT_PROXY=cluster-name
yt read //sys/scheduler/event_log.2[:#20000000] --format json --config "{read_parallel={enable=%true;max_thread_count=50;}}" > scheduler_log_json
yt read //sys/scheduler/event_log.2[:#20000000] --format yson --config "{read_parallel={enable=%true;max_thread_count=50;}}" > scheduler_log_yson

ls -lah
total 153G
drwxrwxr-x 2 user group 4.0K Sep 29 21:23 .
drwxrwxr-x 7 user group 4.0K Sep 28 15:20 ..
-rw-r--r-- 1 user group  51G Sep 29 21:25 scheduler_log_json
-rw-r--r-- 1 user group  51G Sep 29 21:22 scheduler_log_yson
-rw-r--r-- 1 user group  51G Sep 29 21:20 test_file

time cat scheduler_log_yson | yt write //tmp/test_yson --format yson
real    36m51.653s
user    31m34.416s
sys     1m5.256s

time cat scheduler_log_json | yt write //tmp/test_json --format json
real    88m38.745s
user    21m7.188s
sys     1m1.400s

time cat test_file | yt upload //tmp/test_file
real    35m50.723s
user    17m31.232s
sys     1m39.132s

time cat scheduler_log_yson | yt write //tmp/test_yson --format yson --config "{write_parallel={enable=%true;max_thread_count=30;}}"
real    13m37.545s
user    37m20.516s
sys     4m16.436s

time cat scheduler_log_json | yt write //tmp/test_json --format json --config "{write_parallel={enable=%true;max_thread_count=30;}}"
real    3m53.308s
user    23m21.152s
sys     2m57.400s

time cat test_file | yt upload //tmp/test_file --config "{write_parallel={enable=%true;max_thread_count=30;}}"
real    1m49.368s
user    18m30.904s
sys     1m40.660s
```

{% endcut %}

#### Parallel writing of tables and files { #parallel_write}

To use this option, do the following:

1. Set `zlib_fork_safe` if you run python2.
2. Enable the following option in the config file: `config["write_parallel"]["enable"] = True`.

After that, the standard commands [write_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.write_table)?? and [write_file](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.write_file)?? will work in multithreading mode

Now, the client configuration includes a new `write_parallel` section with the following keys:

- `enable` (False by default) enables the option to write to tables and files in {{product-name}} in parallel.
- `max_thread_count` (10 by default): The maximum number of threads the data is written to in parallel.
- `unordered` (False by default): Enables you to write rows to a table in an arbitrary order, streamlining the writing process.
- `concatenate_size` (100 by default): a limit on the number of tables or files that the concatenate command receives as input.

#### How it works

The entire process of parallel writing looks like this:

1. The input thread is split into rows, and the rows are grouped into chunks whose size is regulated by the `/write_retries/chunk_size` configuration option (the default value is 512 MB).
2. The chunks are delivered to ThreadPool.
3. The threads receive a group of rows to be written as input, compress this data, and upload it to to a temporary table or file on the server.
4. The main thread accumulates temporary tables and files that have already been written to.
5. As soon as the amount of written tables or files becomes equal to `/write_parallel/concatenate_size`, the data is merged.

#### Limitations

- Parallel writing is unavailable when a thread of compressed data is delivered as input.
- If the path to the output table has the `sorted_by` attribute, parallel writing will also be unavailable.
- Parallel writing won't be effective in Python2 unless you install `zlib_fork_safe` (this is because of GIL).
4. Parallel writing won't be effective if the parser used for your format is slow (for example, in the event of [schemaful_dsv](../../../user-guide/storage/formats.md#schemaful_dsv)).
5. Because the input thread is split into chunks for parallel uploading to the cluster, the custom script will consume the memory proportionally to `max_thread_count * chunk_size` (in practice, the multiplier is about 2).

**Q: Why is multithreaded writing many times faster for JSON vs. YSON, but is the other way around for singlethreaded writing?**
**A:** There are two reasons:
You need to break down the input thread into rows. In JSON, you can do this easily by splitting by `\n`. Doing this in YSON requires much more effort. As this operation is single-threaded, it becomes a weak spot and locks the entire writing process.

### Working with transactions and locks { #transaction_commands }

- [start_transaction](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.start_transaction): Create a new transaction with the specified timeout.

- [abort_transaction](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.abort_transaction): Abort the transaction with the specified ID.

- [commit_transaction](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.commit_transaction): Commit the transaction with the specified ID.

- [ping_transaction](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.ping_transaction): Ping the transaction to extend its TTL.


These functions produce the [YtResponseError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtResponseError) exception with `.is_resolve_error() == True` when the transaction is not found.

- [lock](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.lock): Takes a lock on the specified node within the current transaction written to `TRANSACTION_ID`. In the event of a waitable lock and the specified `wait_for`, it waits for the lock to be taken within `wait_for` milliseconds. If the lock isn't taken within this time, it returns an exception.

- [unlock](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.unlock): Removes all the [explicit](../../../user-guide/storage/transactions.md#implicit_locks) locks taken by the current transaction (written to `TRANSACTION_ID`) — both already taken locks and enqueued locks. If there are no locks, it has no effect. When the unlocking is impossible (because the locked node version includes changes compared to the original version), it returns an exception.

   {% note info "Note" %}

   When the transaction is completed (either successfully or not), all the locks taken by it are released. This means that you only have to `unlock` when you need to release a node without completing the transaction.

   {% endnote %}

- [Transaction](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.transaction.Transaction)?? : **Single-thread** wrapper class for creating, committing, or aborting transactions. It supports the syntax of context manager (the `with` statement), that is, if the transaction exits the scope successfully, the transaction is committed; otherwise, it is aborted. All the commands in the scope run within the specified transaction. You can create nested scopes. The `ping` parameter (the default value is `True`) in the builder is responsible for running a pinging thread. If there is no ping, the operation will be forcibly aborted on timeout expiry.

Examples:

```python
with yt.Transaction():
    yt.write_table_structured(table, [Row(x=5)])  # the data will be written within the transaction
    # Once it exits the with, the transaction is committed.

with yt.Transaction():
    yt.write_table_structured(table, [Row(x=6)])
    raise RuntimeError("Something went wrong")
    # The exception will result in transaction abort and no one will see the changes

## Nested transactions
with yt.Transaction():
    yt.lock("//home/table", waitable=True)
    with yt.Transaction():
        yt.set("//home/table/@attr", "value")
```

If the pinging thread fails when trying to ping the transaction, it calls `thread.interrupt_main()`. You can change this behavior using the option `config["ping_failed_mode"]`.
Available options:
1. `pass`: Do nothing
2. `call_function`: Calls the function specified in the `ping_failed_function` field of the config
3. `interrupt_main`: Throw the `KeyboardInterrupt` exception in the main thread
4. `send_signal`: Send the `SIGUSR1` signal to the process.
5. `terminate_process`: Terminate the process.

### Running operations { #run_operation_commands }

To learn about the operations available for table data, see the [section](../../../user-guide/data-processing/operations/overview.md).

All the functions that run operations start with the `run_` prefix. Such functions often have numerous parameters. Next, we will describe the parameters that are shared by all the operation run functions, as well as the parameter selection logic. For a full list of parameters for each specific function, see [pydoc](http://pydoc.ytsaurus.tech/yt.wrapper.html).

Operation run commands

- [run_erase](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_erase): [The operation that deletes data from a table](../../../user-guide/data-processing/operations/erase.md)
- [run_merge](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_merge): [The operation to merge/process data in the table without running the custom code](../../../user-guide/data-processing/operations/merge.md)
- [run_sort](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_sort): [Data sorting operation](../../../user-guide/data-processing/operations/sort.md). If the destination table is omitted, inplace sorting of the table is performed.
- [run_map_reduce](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_map_reduce): [MapReduce operation](../../../user-guide/data-processing/operations/mapreduce.md).
- [run_map](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_map): [Map operation](../../../user-guide/data-processing/operations/map.md).
- [run_reduce](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_reduce): [Reduce operation](../../../user-guide/data-processing/operations/reduce.md).
- [run_join_reduce](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_join_reduce): [JoinReduce operation](../../../user-guide/data-processing/operations/reduce.md#foreign_tables).
- [run_operation](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_operation): Runs the operation.
   By default, the command starts the operation as it is. The `enable_optimizations` option enables the `treat_unexisting_as_empty`, `run_map_reduce_if_source_is_not_sorted`, `run_merge_instead_of_sort_if_input_tables_are_sorted` optimizations if they are enabled in the config
- [run_remote_copy](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_remote_copy): [RemoteCopy operation](../../../user-guide/data-processing/operations/remote-copy.md)
   Coping the table from one {{product-name}} cluster to another.


#### SpecBuilder { #spec_builder }

We recommend using [spec builders](#spec_builder) to specify the operation's runtime parameters. 

The following classes are provided for populating the operation specifications:
- [MapSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MapSpecBuilder);
- [ReduceSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.ReduceSpecBuilder);
- [MapReduceSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MapReduceSpecBuilder);
- [JoinReduceSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.JoinReduceSpecBuilder);
- [MergeSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MergeSpecBuilder);
- [SortSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.SortSpecBuilder);
- [RemoteCopySpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.RemoteCopySpecBuilder);
- [EraseSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.EraseSpecBuilder);
- [VanillaSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.VanillaSpecBuilder).

The names of the methods correspond to option names in the specification listed [here](../../../user-guide/data-processing/operations/operations-options.md).

The builders used to populate the specification for an operation with custom jobs provide the `begin_mapper`, `begin_reducer`,  and `begin_reduce_combiner` methods. Similarly, there are the `begin_job_io`, `begin_map_job_io`, `begin_sort_job_io`, `begin_reduce_job_io`, `begin_partition_job_io`, `begin_merge_job_io` methods in the appropriate spec builders.

Example:

```python
import yt.wrapper as yt

if __name__ == "__main__":
    spec_builder = yt.spec_builders.MapSpecBuilder() \
        .input_table_paths("//tmp/input_table") \
        .output_table_paths("//tmp/output_table") \
        .begin_mapper() \
            .command("cat") \
        .end_mapper()

    yt.run_operation(spec_builder)
```

Another two types of spec builders are provided for convenience:

1. Spec builders for filling out [I/O parameters](../../../user-guide/storage/io-configuration.md):
   - [PartitionJobIOSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.PartitionJobIOSpecBuilder);
   - [SortJobIOSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.SortJobIOSpecBuilder);
   - [MergeJobIOSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MergeJobIOSpecBuilder);
   - [ReduceJobIOSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.ReduceJobIOSpecBuilder);
   - [MapJobIOSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MapJobIOSpecBuilder).
2. Spec builders for filling out the specification of a custom job:
   - [MapperSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MapperSpecBuilder);
   - [ReducerSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.ReducerSpecBuilder);
   - [ReduceCombinerSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.ReduceCombinerSpecBuilder).

see the [example in the tutorial](../../../api/python/examples.md#spec_builder).

#### Other parameters of the operation run commands.

The `sync` parameter is shared by all the operation run functions (`True` by default). If `sync=True`, when the function is called, the operation completion will be awaited synchronously. If `sync=False`, the [Operation](#operation_class) object will be returned to the client.

If the relevant parameter is missing in the appropriate spec builder, you can use the `spec` parameter of the `dict` type that enables you to explicitly set arbitrary options for the operation specification (the specified parameter has the highest priority when generating a specification). For example, you can specify the desired amount of data per job: `yt.run_map(my_mapper, "//tmp/in", "//tmp/out", spec={"data_size_per_job": 1024 * 1024})` (but in this particular case, it's more appropriate to use a [builder's method](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MapSpecBuilder.data_size_per_job)).

All the required specification parameters exist as individual options. Such parameters include the input and output table paths and the command (for operations with the custom code). For the sort, reduce, map_reduce, join_reduce operations, the `sort_by`, `reduce_by`, and `join_by` parameters are specified as individual options.

The `source_table` and `destination_table` parameters can accept as their input both a table path and a list of paths to tables, and correctly handle both cases, depending on whether the operation expects a table path or a set of table paths at the given point. Both the strings and [TablePath](#tablepath_class) objects are accepted as table paths.

The parameters include numerous settings that are changed fairly regularly. These settings include `job_io` and its `table_writer` part. These parameters will be added to the appropriate parts of the specification. If the operation includes multiple steps, then the parameters specified in `job_io` will propagate to all the steps. For all the operations that have the `job_count` parameter in the specification, this parameter is defined as a separate option.

For all the operations that include the custom code, a separate option is created for `memory_limit`, as well as the `format`, `input_format`, and `output_format` parameters that enable you to specify the [input and output data format](../../../user-guide/storage/formats.md). There are the `yt_files` and `local_files` parameters (and the `files` alias) enabling you to set the file paths in Cypress and the paths to local files (respectively) that will be passed to the job environment.

For the map_reduce operation, all the parameters of the custom script described in the previous paragraph are copied separately for mapper, reducer, and reduce-combiner.

As the `binary`, you can pass both the [run command](../../../user-guide/data-processing/operations/overview.md) (as a string) and a callable Python object. The operations that access callable objects as `binary` are referred to as [Python operations](#python_operations). If a run command is passed as `binary`, the [format](#python_formats) parameter is mandatory. However, the format is optional in the case of a callable object.

Any operation writing data to destination_table has the option that skips deleting the existing table but appends data to it. For this purpose, as destination_table, you can specify a [TablePath object](#tablepath_class) with `append=True`.

When running a `map_reduce` operation with multiple input tables, keep in mind that the {{product-name}}client can add table switches to input records. As a result, the switches might end up in the output stream (if you pass this data to the output stream as it is, such as  a mapper from some `yield row`). This might result in the error of writing the record to a non-existing table. To avoid this situation, you should either disable the table index using the `enable_input_table_index` option or manually delete the table index before writing data to the output stream: `del row["@table_index"]`.

For the [run_map_reduce](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_map_reduce) function, all the options specific to a given job exist in three copies: for the mapper, reducer, and reduce_combiner. That is, the function has the options `map_local_files`, `reduce_local_files`, `reduce_combiner_local_files`, etc. For a full list of options, see [pydoc](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_map_reduce).

### Working with operations and jobs { #operation_and_job_commands }

- [get_operation_state](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.get_operation_state): Returns the state of the current operation. The object returned is a class that has a single  `name` field and the `is_finished, is_running, is_unsuccessfully_finished` methods.
- [abort_operation](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.abort_operation): Abort the operation without saving its result.
- [suspend_operation](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.suspend_operation): Suspend the operation.
- [resume_operation](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.resume_operation): Resume the suspended operation.
- [complete_operation](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.complete_operation): Complete the operation with the current result.

When running an operation with the `sync=False` flag, it's more convenient to use the `abort, suspend, resume, complete` methods from the `Operation` class than the above methods (see the [Operation](#operation_class) section).


- [run_job_shell](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_job_shell): Run a job-shell for the job. For this function, it's more convenient to use its CLI (../cli/cli.md) counterpart  todo

#### Getting information about jobs and operations { #operation_and_job_info_commands }
This operation has a fairly non-trivial life cycle, and at certain points in time the information about the operation can be obtained from various sources:

1. Cypress (contains information about the running operations that haven't been archived yet)
2. The Orchid of the controller agent (includes all current information about the running operations)
3. Operation archive (includes information about completed operations)

Because you can get information about an operation from different sources (and not only the list of sources can change, but also the structure of each source), there exist the following methods that can collect information about an operation from the listed sources anytime.

- [get_operation](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.get_operation): Get information about the operation based on its ID. A `dict` is returned with fields similar to the fields in the get_operation (../commands.md#get_operation) response todo
- [list_operations](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.list_operations): Get information about a set of operations based on filters. The meaning of the fields is similar to `get_operation`. For a list of filters, see the section (../commands.md#list_operations) todo
- [iterate_operations](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.iterate_operations): Get an iterator for a set of operations. This function is similar to `list_operations`, but it doesn't set restrictions on the number of requested operations.

An example is to output the types of the three latest operations run by `username`:
```python
from datetime import datetime
import yt.wrapper as yt
result = yt.list_operations(
    user="username",
    cursor_time=datetime.utcnow(),
    cursor_direction="past",
    limit=3,
)
print([op["type"] for op in result["operations"]])
## Output: ['sort', 'sort', 'sort']
```

Another example is to find all the executing operations run in the user ephemeral pools:
```python
for op in client.iterate_operations(state="running"):
    if "$" in op.get("pool"):
        print(op["id"], op["start_time"])
```

The information about the operation's job is available in the scheduler and in the archive. The following methods enable you to get information about jobs.
- [get_job](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.get_job): Get information about the job. A `dict` is returned with a field similar to the field in the get_job (../commands.md#get_job) response todo
- [list_jobs](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.list_jobs): Get information about the set of jobs for the operation. The meaning of fields in the response is similar to that in `get_job`. For a list of filters, see the section (../commands.md#list_jobs) todo
- [get_job_stderr](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.get_job_stderr): Get the stderr of the job.
- [get_job_input](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.get_job_input): Get the job's full input
- [get_job_input_paths](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.get_job_input_paths): Get the list of input tables (with row ranges) for the job
- [get_job_spec](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.get_job_spec): Get the specification of the job.


{% note warning "Attention!" %}

When you call functions described in this section from inside themselves, they can access the master servers, the scheduler, and the cluster nodes (note that accesses to the master servers and to the scheduler aren't scalable), so you shouldn't use them too often.

{% endnote %}

For debugging of failed jobs, it is convenient to use the job tool (../../problems/jobtool.md) todo. This utility enables you to prepare the environment similar to the job environment and run it with the same input data.

#### Operation { #operation_class }

The object of this class is returned by the operation run commands (`run_*`). It provides a [small API](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.Operation) to work with the already running or completed operations.

#### OperationsTracker { #operations_tracker_class }

To handle several operations CONVENIENTLY, you can use [OperationsTracker](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTracker). You can also use it for working with operations running on different clusters. OperationsTracker has the following interface:

- [add](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTrackerBase.add): Add an `Operation` object to the set of tracked objects

- [add_by_id](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTracker.add_by_id): Add the operation to the set of tracked operations using its ID

- [wait_all](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTrackerBase.wait_all): Wait until all the operations are completed

- [abort_all](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTrackerBase.abort_all): Abort all the tracked operations

Example:

```python
with yt.OperationsTracker() as tracker:
    op1 = yt.run_map("sleep 10; cat", "//tmp/table1", "//tmp/table1_out", sync=False)
    op2 = yt.run_map("sleep 10; cat", "//tmp/table2", "//tmp/table2_out", sync=False)
    tracker.add(op1)
    tracker.add(op2)
    tracker.abort_all()
    tracker.add(yt.run_map("true", table, TEST_DIR + "/out", sync=False))
```
When exiting the `with` block, the tracker will wait until all the operations are completed. If an exception occurs, all the running operations are aborted.

{% note warning "Attention!" %}

Using the tracker without `with` is deprecated and we don't recommend it.

{% endnote %}


#### OperationsTrackerPool { #operations_tracker_pool_class }

The [OperationsTrackerPool](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTrackerPool) class is similar to [OperationsTracker](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTracker), but it additionally guarantees that there won't be more than `pool_size` operations running in parallel. As the input, the methods of the class accept one or more spec builders (see the appropriate [section](#spec_builder)).
This class creates a background thread that gradually runs all the operations from the queue, maintaining the guarantees for the number of concurrently running operations.
Interface:

- [add](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTrackerPool.add): Accepts a spec builder as the input and adds it to the run queue
- [map](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTrackerPool.map): Accepts a list of all the spec builders as its input. The other parameters have the same meaning as the add method.

### Working with access rights { #acl_commands }

- [check_permission](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.check_permission)
- [add_member](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.add_member)
- [remove_member](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.remove_member)

Examples:

```python
yt.create("user", attributes={"name": "tester"})
yt.check_permission("tester", "read", "//sys")
## Output: {"action": "allow", ...}
yt.create("group", attributes={"name": "test_group"})
yt.add_member("tester", "test_group")
yt.get_attribute("//sys/groups/testers", "members")
## Output: ["tester"]
yt.remove_member("tester", "test_group")
yt.get_attribute("//sys/groups/testers", "members")
## Output: []
```


### Working with dynamic tables { #dyntables_commands }

[Dynamic tables](../../../user-guide/dynamic-tables/overview.md) implement an interface both for point reads and for key-based data writes with transaction support and a native SQL dialect.

The list of commands to access the content of dynamic tables.
- [insert_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.insert_rows): [Insert](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#insert_rows) rows to a dynamic table
- [lookup_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.lookup_rows): [Select](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#chtenie-stroki) the rows with the specified keys
- [delete_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.delete_rows): [Delete](../../../user-guide/dynamic-tables//sorted-dynamic-tables.md#udalenie-stroki) the rows with the specified keys.
- [select_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.select_rows): [Select](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#vypolnenie-zaprosa) the rows that match the query written in the [SQL dialect](../../../user-guide/dynamic-tables/dyn-query-language.md)
- [lock_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.lock_rows): [Lock](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#blokirovka-stroki) the rows with the given keys
- [trim_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.trim_rows): [Delete](../../../user-guide/dynamic-tables/ordered-dynamic-tables#trim) the specified number of rows from the other table

Commands related to [mounting](../../../user-guide/dynamic-tables/overview.md#mount_table) of tables.
- [mount_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.mount_table): Mount a dynamic table
- [unmount_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.unmount_table): Unmount a dynamic table
- [remount_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.remount_table): Remount a dynamic table

Commands related to [sharding](../../../user-guide/dynamic-tables/resharding.md).
- [reshard_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.reshard_table): Reshard the table using the specified pivot keys.
- [reshard_table_automatic](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.reshard_table_automatic): Reshard the mounted tablets based on the tablet balancer configuration.

Other commands.
- [freeze_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.freeze_table): [Freeze](../../../user-guide/dynamic-tables/overview.md#zamorozka) the table
- [unfreeze_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.unfreeze_table): [Unfreeze](../../../user-guide/dynamic-tables/overview.md#zamorozka) the table
- [balance_tablet_cells](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.balance_tablet_cells): Spread the tablets evenly across [tablet cells](../../../user-guide/dynamic-tables/overview.md#tablet_cells)
- [get_tablet_infos](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.get_tablet_infos): Get the [tablet](../../../user-guide/dynamic-tables/overview.md#tablets)'s attributes
- [get_tablet_errors](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.get_tablet_errors): Get the errors that occurred on the [tablet](../../../user-guide/dynamic-tables/overview.md#tablets)
- [alter_table_replica](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.alter_table_replica): [Change attributes](../../../user-guide/dynamic-tables/replicated-dynamic-tables.md#nastrojki-replik) of a replica of the [replicated table](../../../user-guide/dynamic-tables/replicated-dynamic-tables.md)

### Other commands { #etc_commands }

#### Converting tables

**Transform.** Occasionally, you might need to transform your table to a new compression codec, erasure codec, or a [new chunk format](../../../user-guide/storage/chunks.md). Such tasks are solved by the [transform](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.transform) function.

This function starts the `merge` operation on the input table, compressing the data based on the specified parameters. If the `check_codecs` option is set to `True`, the function checks whether the data has already been compressed, and if yes, then the operation is not run. If the `optimize_for` option is specified, the operation always runs (the `check_codecs` option is ignored). This is because there is no option to check which format table chunks have.

**Shuffle.** To randomly shuffle the table rows, use the [shuffle_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.shuffle_table) function.
The function runs the `map_reduce` operation that sorts the table by the added column with a random number.

## Python objects as operations { #python_operations }

### Overview { #python_operations_intro }

To run the operation, you need to define a special `yt.wrapper.TypedJob` subclass and pass the object of this class to the operation run [function](#run_operation_commands) (or specify it in the applicable field of the [SpecBuilder](#spec_builders)).

Make sure to define, in the job class, the `__call__(self, row)` method (for the mapper) or `__call__(self, rows)` method (for the reducer). As input, this method accepts table rows (in the case of a reducer, a single call (`__call__`) corresponds to a set of rows with the same key). It has to return (**by using `yield`**) the rows that need to be written to the output table. If you have multiple output tables, use the wrapper class `yt.wrapper.OutputRow`, whose constructor accepts the row written and the `table_index` as a named parameter (see the [example](examples.md#table_switches) in the tutorial).

In addition, you can define the methods `start(self)` (it will be called only once before processing the job records) and `finish(self)` (it will be called once after processing the job records) which, just as `__call__`, can generate records (using `yield`). This allows you, for example, to easily run aggregating operations. As well as the [`.prepare_operation(self, context, preparer)`](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.TypedJob.prepare_operation) method. It is used to specify row types for input and output table tabs, as well as to modify the operation specification. For more information, see [below](#prepare_operation) and the examples in the tutorial: [one](examples.md#prepare_operation) and [two](#examples.md#grep).

### Preparing an operation from a job { #prepare_operation }

To specify the input and output string types in the job class, you can use the hint type: [one](examples.md#simple_map), [two](examples.md#multiple_input_reduce), [three](examples.md#multiple_input_multiple_output_reduce), and [four](examples.md#map_reduce_multiple_intermediate_streams)) or override the method [`.prepare_operation(self, context, preparer)`](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.TypedJob.prepare_operation). The types are specified using the methods of the `preparer` object of the [`OperationPreparer`](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.OperationPreparer) type. Useful methods:
1. [`inputs`](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.OperationPreparer.inputs): Enables you to specify the input row type for multiple input tables (it must be a class with the decorator [`@yt.wrapper.yt_dataclass`](#dataclass)), a list of names for the columns that the job needs, as well as the renamings for the columns.
2. [`outputs`](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.OperationPreparer.outputs): Enables you to specify the output row type for multiple output tables (it must be a class with the decorator [`@yt.wrapper.yt_dataclass`](#dataclass)) and the schema that you want to output for these tables (by default, the schema is output from the data class).
3. `input` and `output` are the counterparts of corresponding methods that accept a single index.

The [`context`](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.OperationPreparationContext) object enables you to get information about the input and output streams: their number, schemas, and paths to tables.

See the examples in the tutorial: [one](examples.md#prepare_operation) and [two](#examples.md#grep).

If you run MapReduce with multiple intermediate streams, you also need to override the [.get_intermediate_stream_count(self)](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.TypedJob.get_intermediate_stream_count) method, returning the number of intermediate streams from it. See [example](examples.md#map_reduce_multiple_intermediate_streams).

### Decorators { #python_decorators }

You can mark functions or job classes with special decorators that change the expected interface of interaction with jobs.

- [aggregator](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.aggregator) is a decorator that allows you to indicate that a given mapper is an aggregator, that is, it accepts a row iterator as input rather than a single row.
- [raw](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.raw) is a decorator that allows you to specify that the function accepts a stream of raw data as input rather than parsed records.
- [raw_io](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.raw_io) is a decorator that allows you to indicate that the function will take records (rows) from `stdin` and write them to `stdout`.
- [reduce_aggregator](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.reduce_aggregator) is a decorator that enables you to specify that the reducer is an aggregator that accepts a generator of pairs where each pair is (a key, and records with this key) as input, rather than an iterator of records with a single key.
- [with_context](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.with_context) is a decorator that enables you to request a context for the function. This context will include control attributes requested when running the operation.

Keep in mind that the decorator is implemented by setting an attribute on a function. That's why, for example, you cannot declare a function with the decorator and then make `functools.partial` on it. If you want to pass certain parameters to a function directly at function call, it makes sense to create a class with a decorator (see the last example below).

You can find examples in the [tutorial](examples.md#job_decorators).


### Pickling functions and environments { #pickling }
#### General structure { #pickling_description }

Here is a sequence of actions that occur when you run an operation that is a Python function:

1. The library uses the [dill](https://github.com/uqfoundation/dill) module to transform the executed object (class or function) into a stream of bytes
2. The local runtime environment for the function is set up. This environment is transmitted into a job on the cluster and then used to start up your function properly.

After that, a special code from the `_py_runner.py` module is executed on the cluster. Locally, it unpacks all the dependencies in each job, transforms the function from a set of bytes to a Python object, and starts it up properly, reading data from stdin.

When you are **building a program in Arcadia**, it is assumed that all the proper dependencies of your program are pre-compiled into your static binary file. Therefore, just a binary file is transmitted to the server in this case. In this case, the code in `_py_runner.py` is run using the `Y_PYTHON_ENTRY_POINT` environment variable that specifies a special entry point for the `__main__` substitution and calls the code from `_py_runner.py`.

When using the **vanilla Python**, the following steps are performed: the system iterates through all the modules from `sys.modules`, then, for every such module that has a file or a library actually existing in the system, such a file/library is taken, along with the module, to the job.

#### Link to the post with tips { #pickling_advises }

{% cut "Examples of module filtering " %}
Here's an example of how to filter out transmission of .so and .pyc files but continue to transmit the yt module (by the way, the same thing enables you to run scripts on Mac OS X):

```python
yt.config["pickling"]["module_filter"] = lambda module: hasattr(module, "__file__") and \
    not module.__file__.endswith(".so")
yt.config["pickling"]["force_using_py_instead_of_pyc"] = True

## When working in the client, you can specify it in the config:
client = yt.wrapper.client.Yt(config={"pickling": {"module_filter": lambda ...}})
```

You can also write your dependency resolution function based on the [default implementation from here](http://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.create_modules_archive_default):

```python
def my_create_modules_archive():
    ...

yt.config["pickling"]["create_modules_archive_function"] = my_create_modules_archive
```

If you are using binary modules that are dynamically linked with other libraries that are missing in the cluster, it makes sense to add them to the cluster manually (be sure to set the `LD_LIBRARY_PATH`) environment variable or build dynamic libraries for your Python modules automatically. At each build, an `ldd` is invoked for each `.so` and the found dependencies are packed.
Here's how it's enabled:

```python
yt.config["pickling"]["dynamic_libraries"]["enable_auto_collection"] = True
```

You can also specify a filter if you do not need to add any libraries Example:

```python
yt.config["pickling"]["dynamic_libraries"]["library_filter"] = lambda lib: not lib.startswith("/lib")
```

By default, we recommend filtering out the libraries that are in the `/lib`, `/lib64` directories because these directories host various system libraries (for example, `libcgroup`) and using them on the cluster might result in some strange errors. To filter the libraries, you can use the function from the example above.

Boilerplate filters for different cases:

- If you see the error: `AttributeError: 'module' object has no attribute 'openssl_md_meth_names'`, filter out hashlib:`yt.config["pickling"]["module_filter"] = lambda module: "hashlib" not in getattr(module, "__name__", "") `

- If you run Anaconda, you have to filter out hashlib (see the filter example above), as well as the .so libraries. To filter out .so libraries, you can use the following filter:

   ```python
   yt.config["pickling"]["module_filter"] = (
       lambda module: hasattr(module, "__file__") and
       not module.__file__.endswith(".so")
   )
   ```

   {% note warning "Attention!" %}

   This filter also filters out YSON [bindings](#yson). If you use YSON, it makes sense to add the library to exceptions:

   ```python
   yt.config["pickling"]["module_filter"] = (
       lambda module: hasattr(module, "__file__") and
       (not module.__file__.endswith(".so") or module.__file__.endswith("yson_lib.so")
   )
   ```

   {% endnote %}

- You execute your program using updated python2.7 and get the following error: `ImportError: No module named urllib3` or `ImportError: cannot import name _compare_digest`, or you can't import the `hmac` module

   To solve this problem, you need to filter out hmac from the modules that you take with you (it imports from the module the method missing from python2.7.3 installed on the cluster).

   ```python
   yt.config["pickling"]["module_filter"] = ... and getattr(module, "__name__", "") != "hmac"
   ```

Automatic filtering of the .pyc and .so files is also supported in cases when the Python version and OS version differ between the cluster and the client. The option should be enabled in the config first:

```python
yt.config["pickling"]["enable_modules_compatibility_filter"] = True
```

When running a Python  function in a job, all the modules present in the dependencies are unpacked and imported, including those of your main module. That's why all the business logic needs to be hidden within `__main__`; a proper implementation should look like this:

```python
class Mapper(yt.TypedJob):
    ...

if __name__ == "__main__":
    yt.run_map(mapper, ...)
```

{% endcut %}

### Porto layers { #porto_layers }

When running an operation, you can specify which [porto layers](../../../user-guide/data-processing/porto/layer-paths.md) need to be prepared before running your jobs.
There is a certain set of [ready-to-use layers](../../../user-guide/data-processing/porto/layer-paths.md#gotovye-sloi-v-kiparise) available at `//porto_layers`.

One way you can specify a path to the proper layer using the parameter `layer_paths` in the job spec is:
```python
spec_builder = ReduceSpecBuilder() \
    .begin_reducer() \
        .command(reducer) \
        .layer_paths(["//porto_layers/ubuntu-precise-base.tar.xz"]) \
    .end_reducer() \
    ...
yt.run_operation(spec_builder)
```

### tmpfs in jobs { #tmpfs_in_jobs }

Supporting tmpfs in jobs includes two parts:

1. For Python operations, tmpfs is enabled by default: it is mounted to a special tmpfs directory and the module archive is unpacked into it. Additional memory needed for tmpfs is added to the limit specified by the user. The behavior is regulated by the options: `pickling/enable_tmpfs_archive` and `pickling/add_tmpfs_archive_size_to_memory_limit`.
2. There is an option to automatically enable tmpfs for all the job's files: this option is called `mount_sandbox_in_tmpfs/enable` and is disabled by default. If you enable this option, the specs for your operations will specify `tmpfs_path="."` and also set `tmpfs_size` equal to the total file size. tmpfs_size will also be added to `memory_limit`. Node that if you're using table files, the system can't find out the size of the disk after formatting, so you need to specify the size in the `disk_size` attribute of the path. You can also request additional tmpfs space if your job generates some files at runtime. For this, specify the appropriate number of bytes in the `mount_sandbox_in_tmpfs/additional_tmpfs_size` option.

### Statistics in jobs { #python_jobs_statistics }

As the job is running, the user can export their own statistics (for example, measure the execution time for certain steps in the job). The library provides the following functions:

- [write_statistics](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.user_statistics.write_statistics): Writes a dict with the collected statistics to an appropriate file descriptor. The function must be called from within the job.

The `Operation` class also has the [get_job_statistics](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.Operation.get_job_statistics) method for quick access to the operation statistics.

Example:

```python
class WriteStatistics(yt.wrapper.TypedJob):
    def __call__(self, row: Row) -> Row:
        yt.write_statistics({"row_count": 1})
        yield row

yt.write_table_structured(table, [Row(x=1)])
op = yt.run_map(WriteStatistics(), table, table, sync=False)
op.wait()
print(op.get_job_statistics()["custom"])
## Output: {"row_count": {"$": {"completed": {"map": {"count": 1, "max": 1, "sum": 1, "min": 1}}}}}
```


## Untyped Python operations { #python_operations_untyped }

Non-typed jobs are also supported aside from typed jobs.
Instead of the `TypedJob` class, a Python function or any callable object is used as a custom job. The function accepts a record (in case of the mapper) and the key plus the record iterator (in case of the reducer). In the latter case, the key is a `dict`-like object where only key columns are populated. The function must be a generator that uses `yield` to generate records that will be written to the output table.

Example of a filtering mapper:

```python
def mapper(row):
    if row.get("type") == "job_started":
        yield row
```

Example of a reducer summing across the value column:

```python
def reducer(key, rows):
    row = dict(key.iteritems())
    row["sum"] = sum((row["value"] for row in rows))
    yield row
```

### Decorators { #python_decorators_untyped }

In the non-typed API, the meaning of some decorators slightly changes compared to the [typed](#python_decorators) API.

- [reduce_aggregator](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.reduce_aggregator): Instead of accepting a single pair (key, and records), the reducer will accept a pair iterator where each pair is (key, and records with this key).

Keep in mind that the decorator is implemented by setting an attribute on a function. That's why, for example, you cannot declare a function with the decorator and then make `functools.partial` on it. If you want to pass certain parameters to a function directly at function call, it makes sense to create a class with a decorator (see the last example below).

You can find examples in the [tutorial](examples.md#job_decorators_untyped).


### Formats { #python_formats }

By default, you do not have to specify formats when running Python operations.

{{product-name}} stores structured data, and there's no predefined textual representation for this data: all the jobs run in streaming mode, and the user needs to explicitly specify the format in which its script expects to see the data in the input stream. By contrast, when accessing data from a Python function, you also get the data in the structured format. You can build a mapping between the structured data format in {{product-name}} and in Python, and the intermediate format used to transmit data to the job isn't so critical.

Keep in mind that there is no perfect one-to-one correspondence between the structured data in {{product-name}} and dicts in Python. That's why, in this case, there are some peculiarities (see below).

#### Structured data representation { #structured_data }

The following key peculiarities exist in representing structured data in Python:

1. Objects in table records in {{product-name}} can have [attributes](../../../user-guide/storage/attributes.md) (this only applies to columns of the type any). To represent them, the library provides [special types](http://pydoc.ytsaurus.tech/yt.yson.html#module-yt.yson.yson_types) that are inherited from standard Python  types, but additionally they also have the `attributes` field. Because creating custom objects is an expensive procedure, by default, such an object is created only if it has attributes. To regulate this behavior, there is the option: `always_create_attributes`. YSON types are compared as follows: first the values of the elementary types, then the attributes. If the attributes aren't equal (for example, if one object has attributes, and another object doesn't), the objects are considered non-equal. This should be kept in mind when comparing with elementary types in Python: to avoid dependence on the presence of object attributes, you should explicitly convert the object to the elementary type.

   Explanatory example:

   ```python
   import yt.yson as yson
   s = yson.YsonString("a")
   s.attributes["b"] = "c"
   s == "a" # False

   str(s) == "a" # True

   other_s = yson.YsonString("a")
   other_s == s # False

   other_s.attributes["b"] = "c"
   other_s == s # True
   ```

2. The YSON format uses two integer types: [int64 and uint64](../../../user-guide/storage/yson.md) . On the other hand, from the data model viewpoint, Python has a single data type with no limits imposed. For this reason, when reading data, a non-signed type is represented as YsonUint64, while a signed type is represented as a regular int type. When writing an int, the behavior is automatic: numbers in the range [-2^63, 2^63) are represented as signed, while numbers in the range [2^63, 2^64) are represented as non-signed. However, you can always specify the type explicitly by creating an explicit Yson object.

3. Unicode strings. As all the strings are byte strings in {{product-name}}, the Python's  Unicode strings are UTF-8-encoded as byte strings. When reading data in Python 3, an attempt is made to decode byte strings using a utf-8 decoder. If this doesn't work, a special [YsonStringProxy](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.yson_types.YsonStringProxy) object is returned. For more information, see the appropriate[section](#python3_strings).

When writing data, YsonFormat correctly distinguishes between Python types and YSON types.

#### Control attributes { #control_attributes }

Apart from a stream of records, when reading data from a table (or from within a job), you can request various [control attributes](../../../user-guide/storage/io-configuration.md#control_attributes). Working with control attributes is format-dependent: for example, most control attributes can't be represented in formats different from Yamr, JSON, or YSON.

Yamr format in Python API correctly supports parsing of the row_index and table_index attributes that will be represented as the tableIndex and recordIndex fields and Record-type objects.

In JSON format, the control attributes aren't processed in any special way. When requesting them, you have to process the control records within the stream.

YSON format has several modes for automated processing of control attributes. Selection of the mode is controlled by a `control_attributes_mode` option that can take on the following values **(important: for historical reasons, for the option to run correctly, you also need to set process_table_index=None when creating a format. By default, process_table_index is True, which enforces control_attributes_mode=row_fields)**:

- `iterator`(default): When parsing the stream, the format will output a record iterator. To get the control attributes from a job, you need to use the context.
- `row_fields`: The requested control attributes will be added as fields to each record. For example, if you request row _index, then each record will have the `@row_index` field with the ID of this record.
- `none`: No special processing will be done for the control attributes: the client will get a stream of records where the entity-type records will have control attributes.

Examples of switching between output tables with `table_index` at `control_attributes_mode` equal to `iterator` and `row_fields`. [Here](examples.md#table_switches_untyped) you can find an example of how to get an index of the current table in the reducer using `context`.

#### Other formats { #other_formats }

By default, the library serializes data into [YSON format](../../../user-guide/storage/formats.md#yson) because this format provides a proper and unambiguous representation for any data stored in {{product-name}} tables. To handle YSON format efficiently in Python, you need a [native library](#yson_bindings). If you don't add this library to the cluster, you will get a runtime error in your job (we didn't add a fallback to the Python's YSON library because it's very slow and inefficient). In this situation, you can switch over to the [JSON format](../../../user-guide/storage/formats.md#json).

Most other formats are text-based (that is, numbers have the same format as strings), so you will lose data typing.

If you use the formats different from YSON and Yamr, you will always get a `dict` of records. For example, there will be no automatic record conversion from JSON to YSON.




## Other { #other }

### gRPC { #grpc }

RPC proxy with gRPC transport isn't supported. If you can't install a binary package with RPC bindings, you can use gRPC on your own.

<!-- Подробное описание и пример есть на [отдельной странице](../../description/proxy/grpc.md). -->

### YSON { #yson }

Together with the library to work with {{product-name}} clusters,  we also supply a library for [YSON format](../../../user-guide/storage/yson.md). The library is available in the `yt.yson` module, implementing the standard `load`, `loads`, `dump`, and `dumps` functions. In addition, it provides access to [Yson types](#structured_data). The library also implements the following generally useful functions:

- [to_yson_type](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.convert.to_yson_type): Creating a YSON type from a Python object.
- [json_to_yson](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.convert.json_to_yson): Recursively converts the Python object from JSON to YSON (read about the peculiarities of {{product-name}} structured data representation in [JSON format](../../../user-guide/storage/formats.md#json).
- [yson_to_json](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.convert.yson_to_json): Recursively converts a Python object from YSON to JSON.

#### YSON bindings { #yson_bindings }

The yson library has two implementations: written in pure Python, and written as C++ bindings.
The YSON's native parser and writer written in Python are very slow and can only be used with small amounts of data.
**Important**: For example, you won't be able to run operations or read tables in YSON format

C++ bindings are delivered as Debian and pip packages.

The packages are built as a universal .so library with libcxx compiled into it: that's why they should work in any Debian-based system.
