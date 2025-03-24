# {{product-name}} Airflow Provider

[Apache Airflow](https://airflow.apache.org/) is a platform for creating, scheduling, and monitoring workflows. {{product-name}} Airflow Provider allows you to integrate Airflow with {{product-name}} and efficiently manage data processing tasks.

This section provides instructions on how to connect to a {{product-name}} cluster in Apache Airflow and how to use operators to interact with Cypress, work with static tables, and execute queries in [Query Tracker](../../user-guide/query-tracker/about.md).

## Installation

{% note info %}

The current package version requires:
- Python 3.8+
- Airflow 2.9.0+

{% endnote %}


1. Install the Python client from the PyPi repository:
    ```bash
    pip install ytsaurus-airflow-provider
    ```

    {% cut "If you encounter an error 'Building wheel for xmlsec (pyproject.toml) did not run successfully.'" %}

    Install the packages required to build `xmlsec`:
    ```bash
    sudo apt install libxmlsec1-dev pkg-config build-essential python3-dev
    ```

    {% endcut %}

2. Verify that the provider installation was successful:

    ```bash
    $ python3 -c "import ytsaurus_airflow_provider; print(ytsaurus_airflow_provider.__version__)"
    0.1.1
    ```

As a result, the following will be available:

- Python library for writing DAGs;
- provider for [Apache Airflow](https://airflow.apache.org);
- tools `ytsaurus-client` and `ytsaurus-yson`.


## Getting Started { #howtostart }

In the Apache Airflow web interface, create a connection to the {{product-name}} cluster:
1. **Admin** -> **Connections** -> **+**
2. **Connection Id**: `ytsaurus_cluster_default`
3. **Connection Type**: {{product-name}} Cluster
4. **Cluster Proxy**: Cluster proxy
5. **Cluster Token**: Cluster connection token
6. **Extra**: Optionally, you can specify [Client Config](../../../api/python/userdoc#configuration):
    ```
    {
        "client_config": {
            "create_table_attributes": {
                "compression_codec": "brotli_3"
            }
        }
    }
    ```

If necessary, you can create a connection to object storage:
1. **Admin** -> **Connections** -> **+**
2. **Connection Id**: `aws_default`
3. **Connection Type**: Amazon Web Service
4. **AWS Access Key ID** and **AWS Secret Access Key**: Object storage keys

#### How to specify a connection { #selectconn }

Each operator accepts an optional parameter `ytsaurus_conn_id`, which by default is set to `ytsaurus_cluster_default`. If you need to use a different connection, you should specify it in the `ytsaurus_conn_id` parameter.


## Operators { #operators }

All operators, except for the read operator with specified ObjectStorage, write data to XCom. It is not recommended to pass large volumes of data through XCom. Below is a description of which operators write data to which XComs.

{% note info "Note" %}

All operators use the Python SDK. If errors occur during operator execution, refer to the [Python SDK documentation](../../../api/python/userdoc.md).

{% endnote %}

### Working with Cypress { #operators_cypress }

#### GetOperator

Operator for retrieving the contents of a Cypress node. Implements the functionality of [get](../../../api/commands.md#get) from the Python SDK.

**Parameters:**
- `path: str | YPath`
- `max_size: None | int = None`
- `attributes: None | dict[str, Any] = None`
- `read_from: None | str = None`
- `ytsaurus_conn_id: str = "ytsaurus_cluster_default"`

**XComs:**
- `path`
- `return_value`

#### SetOperator

Operator for writing new content to a Cypress node. Implements the functionality of [set](../../../api/commands.md#set) from the Python SDK.

**Parameters:**
- `path: str | YPath`
- `value: Any`
- `set_format: None | str | Format = None`
- `recursive: bool = False`
- `force: None | bool = None`
- `ytsaurus_conn_id: str = "ytsaurus_cluster_default"`

**XComs:**
- `path`

#### RemoveOperator

Operator for removing a Cypress node. Implements the functionality of [remove](../../../api/commands.md#remove) from the Python SDK.

**Parameters:**
- `path: str | YPath`
- `recursive: bool = False`
- `force: bool = False`
- `ytsaurus_conn_id: str = "ytsaurus_cluster_default"`

**XComs:**
- `path`

#### CreateOperator

Operator for creating an empty Cypress node of type `node_type` with attributes `attributes`. Implements the functionality of [create](../../../api/commands.md#create) from the Python SDK.

**Parameters:**
- `node_type: Literal["table", "file", "map_node", "document", "string_node", "int64_node", "uint64_node", "double_node", "list_node", "boolean_node", "link"]`
- `path: str | YPath`
- `recursive: bool = False`
- `ignore_existing: bool = False`
- `lock_existing: None | bool = None`
- `force: None | bool = None`
- `attributes: None | dict[str, Any] = None`
- `ignore_type_mismatch: bool = False`
- `ytsaurus_conn_id: str = "ytsaurus_cluster_default"`

**XComs:**
- `object_id`
- `path`

#### ListOperator

Operator for getting a list of descendants of the `path` node. The `absolute` option enables output of absolute paths instead of relative ones. Implements the functionality of [list](../../../api/commands.md#list) from the Python SDK.

**Parameters:**
- `path: str | YPath`
- `max_size: None | int = None`
- `absolute: None | bool = None`
- `attributes: None | dict[str, Any] = None`
- `sort: bool = True`
- `read_from: None | str = None`
- `ytsaurus_conn_id: str = "ytsaurus_cluster_default"`

**XComs:**
- `path`
- `return_value`

### Working with tables { #operators_tables }

#### WriteTableOperator

Operator for writing data to a static table. Implements the functionality of [write_table](../../../api/commands.md#write_table) from the Python SDK.

Input data can be passed through the `input_data` parameter or through the `object_storage_path` parameter, which is an object of type [ObjectStorage](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html). It is not possible to pass both `input_data` and `object_storage_path` simultaneously. When writing data from object storage, you need to specify the corresponding `object_storage_format` (see [Table data representation formats](../../../user-guide/storage/formats#table_formats)).

**Parameters:**
- `path: str | YPath`
- `input_data: Any | None = None`
- `object_storage_path: None | UPath = None`
- `object_storage_format: None | str | Format = None`
- `table_writer: dict[str, Any] | None = None`
- `max_row_buffer_size: int | None = None`
- `force_create: None | bool = None`
- `ytsaurus_conn_id: str = "ytsaurus_cluster_default"`

**XComs:**
- `path`
- `object_storage_path` (if writing through `object_storage_path`)

#### ReadTableOperator

Operator for reading data from a static table. Implements the functionality of [read_table](../../../api/commands.md#read_table) from the Python SDK.

It can write data to object storage if the `object_storage_path` parameter is provided, which is an object of type [ObjectStorage](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html). Otherwise, the table will be written to XCom. When writing data to object storage, you need to specify the desired `object_storage_format` (see [Table data representation formats](../../../user-guide/storage/formats#table_formats)).

**Parameters:**
- `path: str | YPath`
- `object_storage_path: None | UPath = None`
- `object_storage_format: None | str | Format = None`
- `table_reader: None | dict[str, Any] = None`
- `control_attributes: None | dict[str, Any] = None`
- `unordered: None | bool = None`
- `response_parameters: None | dict[str, Any] = None`
- `enable_read_parallel: None | bool = None`
- `omit_inaccessible_columns: None | bool = None`
- `ytsaurus_conn_id: str = "ytsaurus_cluster_default"`

**XComs:**
- `path`
- `object_storage_path` (if writing to `object_storage_path`)
- `return_value` (if writing to XCom)

### Query Tracker { #operators_query_tracker }

#### RunQueryOperator

Operator for executing queries in Query Tracker with different syntax. You can read more about Query Tracker [here](../../../user-guide/query-tracker/about.md). Implements the functionality of [run_query](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_query) from the Python SDK.

It can be run in asynchronous mode, in which case the operator will not wait for the query to complete.

Query execution results can be written to XCom and object storage. If the query returns 3 results, and `[None, ObjectStorage("s3://bucket/path"), None]` is passed as `object_storage_paths`, then results #1 and #3 will be written to XCom, and result #2 will be written to object storage.

**Parameters:**
- `engine: Literal["ql", "yql", "chyt", "spyt"]`
- `query: str`
- `settings: None | dict[str, Any] | yt.yson.yson_types.YsonType = None`
- `files: None | list[dict[str, Any]] | list[yt.yson.yson_types.YsonType] = None`
- `stage: None | str = None`
- `annotations: None | dict[str, Any] | yt.yson.yson_types.YsonType = None`
- `access_control_objects: None | list[str] = None`
- `sync: bool = True`
- `object_storage_paths: list[None | UPath] | None = None`
- `ytsaurus_conn_id: str = "ytsaurus_cluster_default"`

**XComs:**
- `meta`
- `query_id`
- `result_{i}`, where i is the number of the returned result, counted from 0. If `object_storage_paths[i]` is passed for the i-th result, the result will not be written to `result_{i}`.

## Examples { #examples }

Examples of working with operators are presented in ready-made DAGs [example_dags](https://github.com/ytsaurus/ytsaurus-airflow-provider/tree/main/ytsaurus_airflow_provider/example_dags).
