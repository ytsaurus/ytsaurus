# {{product-name}} Airflow Provider

[Apache Airflow](https://airflow.apache.org/) — это платформа для создания, планирования и мониторинга рабочих процессов. {{product-name}} Airflow Provider позволяет интегрировать Airflow с {{product-name}} и эффективно управлять задачами, связанными с обработкой данных.

В данном разделе приведены инструкции — как подключиться к {{product-name}} кластеру в Apache Airflow и как использовать операторы для взаимодействия с Кипарисом, для работы со статическими таблицами и для выполнения запросов в [Query Tracker](../../user-guide/query-tracker/about.md).

## Установка

{% note info %}

Текущая версия пакета требует:
- Python 3.8+
- Airflow 2.9.0+

{% endnote %}


1. Установите Python-клиент из PyPi-репозитория:
    ```bash
    pip install ytsaurus-airflow-provider
    ```

    {% cut "Если возникает ошибка 'Building wheel for xmlsec (pyproject.toml) did not run successfully.'" %}

    Установите пакеты, необходимые для сборки `xmlsec`:
    ```bash
    sudo apt install libxmlsec1-dev pkg-config build-essential python3-dev
    ```

    {% endcut %}

2. Проверьте, что установка провайдера прошла успешно:

    ```bash
    $ python3 -c "import ytsaurus_airflow_provider; print(ytsaurus_airflow_provider.__version__)"
    0.1.1
    ```

В результате будут доступны:

- Python-библиотека для написания DAG-ов;
- провайдер для [Apache Airflow](https://airflow.apache.org);
- инструменты `ytsaurus-client` и `ytsaurus-yson`.


## Начало работы { #howtostart }

В веб-интерфейсе Apache Airflow создайте подключение к кластеру {{product-name}}:
1. **Admin** -> **Connections** -> **+**
2. **Connection Id**: `ytsaurus_cluster_default`
3. **Connection Type**: {{product-name}} Cluster
4. **Cluster Proxy**: Прокси кластера
5. **Cluster Token**: Токен подключения к кластеру
6. **Extra**: По желанию, можно указать [Client Config](../../../api/python/userdoc#configuration):
    ```
    {
        "client_config": {
            "create_table_attributes": {
                "compression_codec": "brotli_3"
            }
        }
    }
    ```

При необходимости можно создать подключение к объектному хранилищу:
1. **Admin** -> **Connections** -> **+**
2. **Connection Id**: `aws_default`
3. **Connection Type**: Amazon Web Service
4. **AWS Access Key ID** и **AWS Secret Access Key**: Ключи доступа к объектному хранилищу

#### Как задать подключение { #selectconn }

Каждый оператор принимает на вход опциональный параметр `ytsaurus_conn_id`, который по стандарту указан как `ytsaurus_cluster_default`. Если необходимо использовать другое подключение, то его следует указать в параметре `ytsaurus_conn_id`.


## Операторы { #operators }

Все операторы, кроме оператора чтения при указанном ObjectStorage, пишут данные в XCom. Не рекомендуется передавать большие объёмы данных через XCom. Ниже описано, какие операторы в какие XCom пишут данные.

{% note info "Примечание" %}

Все операторы используют Python SDK. При возникновении ошибок во время выполнения операторов обратитесь к [документации Python SDK](../../../api/python/userdoc.md).

{% endnote %}

### Работа с Кипарисом { #operators_cypress }

#### GetOperator

Оператор для получения содержимого узла Кипариса. Реализует функционал [get](../../../api/commands.md#get) из Python SDK.

**Параметры:**
- `path: str | YPath`
- `max_size: None | int = None`
- `attributes: None | dict[str, Any] = None`
- `read_from: None | str = None`
- `ytsaurus_conn_id: str = "ytsaurus_cluster_default"`

**XComs:**
- `path`
- `return_value`

#### SetOperator

Оператор для записи нового содержимого в узел Кипариса. Реализует функционал [set](../../../api/commands.md#set) из Python SDK.

**Параметры:**
- `path: str | YPath`
- `value: Any`
- `set_format: None | str | Format = None`
- `recursive: bool = False`
- `force: None | bool = None`
- `ytsaurus_conn_id: str = "ytsaurus_cluster_default"`

**XComs:**
- `path`

#### RemoveOperator

Оператор для удаления узла Кипариса. Реализует функционал [remove](../../../api/commands.md#remove) из Python SDK.

**Параметры:**
- `path: str | YPath`
- `recursive: bool = False`
- `force: bool = False`
- `ytsaurus_conn_id: str = "ytsaurus_cluster_default"`

**XComs:**
- `path`

#### CreateOperator

Оператор для создания пустого узла Кипариса типа `node_type` с атрибутами `attributes`. Реализует функционал [create](../../../api/commands.md#create) из Python SDK.

**Параметры:**
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

Оператор для получения списка потомков узла `path`. Опция `absolute` включает вывод абсолютных путей вместо относительных. Реализует функционал [list](../../../api/commands.md#list) из Python SDK.

**Параметры:**
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

### Работа с таблицами { #operators_tables }

#### WriteTableOperator

Оператор для записи данных в статическую таблицу. Реализует функционал [write_table](../../../api/commands.md#write_table) из Python SDK.

Входные данные могут передаваться через параметр `input_data` или через параметр `object_storage_path`, представляющий из себя объект типа [ObjectStorage](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html). Одновременно передавать `input_data` и `object_storage_path` нельзя. При записи данных из объектного хранилища требуется указать соответствующий `object_storage_format` (см. [Форматы представления табличных данных](../../../user-guide/storage/formats#table_formats)).

**Параметры:**
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
- `object_storage_path` (если запись через `object_storage_path`)

#### ReadTableOperator

Оператор для чтения данных из статической таблицы. Реализует функционал [read_table](../../../api/commands.md#read_table) из Python SDK.

Может писать данные в объектное хранилище, если на вход принимается параметр `object_storage_path`, представляющий из себя объект типа [ObjectStorage](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html). В ином случае таблица будет записываться в XCom. При записи данных в объектное хранилище требуется указать желаемый `object_storage_format` (см. [Форматы представления табличных данных](../../../user-guide/storage/formats#table_formats)).

**Параметры:**
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
- `object_storage_path` (если запись в `object_storage_path`)
- `return_value` (если запись в XCom)

### Query Tracker { #operators_query_tracker }

#### RunQueryOperator

Оператор для выполнения запросов в Query Tracker с разным синтаксисом. Подробнее про Query Tracker можно почитать [тут](../../../user-guide/query-tracker/about.md). Реализует функционал [run_query](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_query) из Python SDK.

Может запускаться в асинхронном режиме, тогда оператор не будет дожидаться выполнения запроса.

Результаты выполнения запроса могут записываться в XCom и объектное хранилище. Если в результате запроса возвращается 3 результата, а в качестве `object_storage_paths` переданы `[None, ObjectStorage("s3://bucket/path"), None]`, то в XCom будут записаны результаты №1 и №3, а результат №2 будет записан в объектное хранилище.

**Параметры:**
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
- `result_{i}`, где i — номер возвращаемого результата, отсчитываемого от 0. Если для i-го результата передан `object_storage_paths[i]`, то результат не будет записан в `result_{i}`.

## Примеры работы { #examples }

Примеры работы с операторами представлены в готовых DAG'ах [example_dags](https://github.com/ytsaurus/ytsaurus-airflow-provider/tree/main/ytsaurus_airflow_provider/example_dags).
