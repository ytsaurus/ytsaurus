## Общая информация { #common }

### Соглашения { #agreements }

Опции системных команд имеют значение по умолчанию `None`. Это означает, что значение данной опции не будет передано при выполнении команды и система будет использовать значение по умолчанию.

Для опции `format` значение по умолчанию `None` обычно имеет другой смысл. Результат команды должен быть возвращен в виде python-структуры (в parsed виде), либо вход команды ожидается в виде python-структуры. Если опция `format` указана, результат выполнения команды будет возвращен в unparsed виде, либо вход команды ожидается в unparsed виде.

Все таймауты/периоды времени по умолчанию принимаются в миллисекундах. В ряде случаев поддержано использование `datetime.timedelta` в качестве времени наравне с указанием в миллисекундах.

Все функции имеют в качестве последней опции объект `client`. Такое поведение является особенностью реализации. Не рекомендуется самостоятельно использовать данную возможность.

В примерах ниже для краткости подразумевается, что выполнен импорт вида `import yt.wrapper as yt`.

### Клиент и глобальный клиент { #client }

Функции и классы доступны из глобального окружения библиотеки модуля yt.wrapper и могут менять его глобальное состояние. Например, сохраняют туда текущую транзакцию. Также, меняя yt.config, вы меняете глобальную конфигурацию. Если вы хотите иметь возможность работать из нескольких независимых (по-разному сконфигурированных) клиентов, то используйте класс [YtClient](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient). У данного класса доступны практически все функции из модуля [yt.wrapper](https://pydoc.ytsaurus.tech/yt.wrapper.html), то есть вы можете вызывать `client.run_map`, `client.read_table_structured`, `with client.Transaction()` и так далее. Обратите внимание, что переменные окружения `YT_PROXY`, `YT_TOKEN` и остальные задают только конфигурацию глобального клиента, то есть влияют только на [yt.wrapper.config](https://pydoc.ytsaurus.tech/yt.wrapper.html#module-yt.wrapper.config), но не на конфигурацию явно созданных экземпляров `YtClient`.

```python
from yt.wrapper import YtClient
client = YtClient(proxy=<cluster-name>, config={"backend": "rpc"})
print(client.list("/"))
```

{% note warning "Внимание" %}

Обратите внимание, что библиотека не является потокобезопасной, поэтому для работы из разных потоков в каждом необходимо создавать свой экземпляр клиента.

{% endnote %}

#### Параметры команд { #commandparams }

Существует возможность передавать заданный набор параметров во все запросы, задаваемые через клиент (например, `trace_id`). Для этого есть специальный метод [create_client_with_command_params](https://pydoc.ytsaurus.tech/yt.wrapper.html?highlight=create_client_with#yt.wrapper.client.create_client_with_command_params), который позволяет указать произвольный набор опций, которые будут передавать во все вызовы API.

<!--Пример todo использования данной функциональности для указания предварительных условий.-->

#### Потокобезопасность { #threadsafety }

Даже если вы пользуетесь не потоками (threading), а процессами (multiprocessing), то совет также остается в силе. Одна из причин: если в главном процессе вы уже задавали запрос к кластеру, то был инициализирован connection pool до этого кластера в рамках глобального клиента. Поэтому после fork процессы будут использовать одни и те же сокеты при общении с кластером, что приведет к разнообразным проблемам.

{% note warning "Внимание" %}

При использовании нативного драйвера стоит пользоваться fork + exec (через `Popen`) и не пользоваться даже `multiprocessing`, так как fork не сохраняет служебные потоки драйвера, и драйвер становится неспособным выполнять запросы. Также не стоит переиспользовать клиенты после fork-ов, у них может быть нетривиальное состояние, которое плохо переживает fork-и.

{% endnote %}

#### Асинхронный клиент на основе gevent { #gevent }

Если ваш код использует `gevent`, то можно использовать `yt.wrapper` в асинхронном режиме. Данная функциональность в данный момент не покрыта тестами, но в простых сценариях должна работать. Важные моменты:

- Перед началом работы выполните monkey patching, чтобы заменить блокирующие сетевые вызовы.
- Внутри каждого [greenlet](https://pypi.org/project/greenlet/) создавайте свой отдельный [YtClient](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient). Это важно, невыполнение этого пункта приведет к ошибкам.

Пример можно найти в [разделе](../../../api/python/examples.md#gevent).


### Конфигурация { #configuration }

Библиотека поддерживает богатые возможности по конфигурации своего поведения в разных местах. Например, вы можете изменить путь в Кипарисе, где будут по умолчанию создаваться временные таблицы: `yt.config["remote_temp_tables_directory"] = "//home/my_home_dir/tmp"`.
Опции и их подробные описания проще всего посмотреть [в коде](http://pydoc.ytsaurus.tech/_modules/yt/wrapper/default_config.html).

Конфигурация библиотеки выполняется одним из следующих способов:
  - Изменение объекта `yt.config`: `yt.config["proxy"]["url"] = "<cluster_name>"`;
  - Вызов функции `yt.update_config`: `yt.update_config({"proxy": {"url": "<cluster_name>"}})`;
  - Установка переменной окружения: `export YT_CONFIG_PATCHES='{url="<cluster_name>"}'`;
  - Через файл, путь к которому указан в переменной окружения `YT_CONFIG_PATH`, по умолчанию `~/.yt/config`. Файл должен быть в формате [YSON](../../../user-guide/storage/formats.md#yson) (с помощью переменной окружения `YT_CONFIG_FORMAT` можно изменить это поведение; поддерживаются форматы YSON и JSON). Пример содержания файла: `{proxy={url="<cluster_name>"}}`.

Часть опций конфигурации можно изменять через переменные окружения. К таким относятся `YT_TOKEN`, `YT_PROXY`, `YT_PREFIX`. А также опции для конфигурации логирования (которые находятся отдельно от конфига): `YT_LOG_LEVEL` и `YT_LOG_PATTERN`.

При использовании [CLI](../../../api/cli/cli.md) патч к конфигурации можно передать с помощью опции `--config`.

Обратите внимание, что конфигурация библиотеки не влияет на конфигурацию клиента и по умолчанию при создании клиента используется конфиг с дефолтными значениями. Вы можете передать в клиент конфиг, построенный на основе переменных окружения следующим образом `client = yt.YtClient(..., config=yt.default_config.get_config_from_env())`, также вы можете обновить имеющийся у вас конфиг значениями из переменных окружения с помощью функции `update_config_from_env(config)`.

Также обратите внимание на порядок приоритетов. При импорте библиотеки применяются конфигурации, переданные через `YT_CONFIG_PATCHES`. В данной переменной окружения ожидается list_fragment, то есть может быть передано несколько конфигураций, разделенных точкой с запятой. Указанные патчи накладываются от последнего к первому. Дальше накладываются значения опций, указанные через конкретные переменные окружения, например через `YT_PROXY`. И лишь затем накладываются конфигурации, указанные явно в коде, или переданные через опцию `--config` при использовании CLI.

При накладывании конфига все узлы, являющиеся dict-ами, мержатся, а не перезаписываются.

Настройка глобальной конфигурации для работы с кластером в своей домашней директории.

```python
yt.config["prefix"] = "//home/username/"
yt.config["proxy"]["url"] = "cluster-name"
```

Создание клиента, который будет создавать все новые таблицы с кодеком `brotli_3`.

```python
client = yt.YtClient(proxy=<cluster-name>, config={"create_table_attributes": {"compression_codec": "brotli_3"}})
```

Настройка глобального клиента для сбора архивов зависимостей в python-операциях с использованием локальной директории отличной от `/tmp`. Затем создание клиента с дополнительной настройкой max_row_weight в 128 МБ:

```python
my_config = yt.default_config.get_config_from_env()
my_config["local_temp_directory"] = "/home/ignat/tmp"
my_config["table_writer"] = {"max_row_weight": 128 * 1024 * 1024}
client = yt.YtClient(config=my_config)
```

#### Общий конфиг { #configuration_common }

#### Настройка логирования { #configuration_logging }

Логирование в ytsaurus-client и всех инструментах, использующих данную библиотеку, устроено следующим образом. Заведен специальный логгер, который находится в модуле `yt.logger` в виде глобальной переменной `LOGGER` и алиасов для логирования на уровне модуля.

Для изменения настроек логирования необходимо изменять `LOGGER`.

Первичная настройка логгера (при загрузке модуля) регулируется переменными окружения `YT_LOG_LEVEL` и `YT_LOG_PATTERN`. Переменная `YT_LOG_LEVEL`, регулирующая уровень логирования, принимает одно из значений `DEBUG`, `INFO`, `WARNING`, `ERROR`, переменная `YT_LOG_PATTERN`, регулирующая форматирование лог-сообщений принимает строку форматирования логгера, подробнее можно прочитать в[документации](https://docs.python.org/3/library/logging.html#logging.Formatter) по Python.

По умолчанию уровень логирования равен INFO, и логирование происходит в stderr.


#### Настройка токена { #configuration_token }

Токен может браться из следующих мест (перечислены в порядке приоритета):

1. Из опции `config["token"]`, которая попадает в глобальную конфигурацию из переменной окружения `YT_TOKEN`.
2. Из файла, указанного в `config["token_path"]`; по умолчанию эта опция равна `~/.yt/token`, также опция может быть переопределена с помощью переменной окружения `YT_TOKEN_PATH` (переменная окружения действует только на конфигурацию глобального клиента, детальней написано в [разделе](#configuration)).

#### Настройка повторных запросов (retry) { #configuration_retries }

Команды в {{product-name}} делятся на легкие и тяжелые (посмотреть на список команд с точки зрения системы можно в разделе [Команды](../../../api/commands.md)).

Легкие команды — это команды вида `create, remove, set, list, get, ping` и так далее. Также перед всеми запросами клиент обращается к специальной вызову прокси для выяснения поддерживаемых ею команд. Такие команды могут выполняться повторно в случае ошибки или таймаута. Настроить параметры повторных запросов можно в секции `proxy/retries` конфигурации.

Тяжелые запросы делятся на две категории.

- Чтения и запись таблиц, файлов (то есть read_table, write_table, read_blob_table и другие). Повторные запросы таких команд настраиваются в секциях `read_retries` и `write_retries` (в зависимости от того, запрос на чтение или на запись).
- Чтения и запись в динамические таблицы (то есть тяжелые запросы select_rows, lookup_rows, insert_rows и другие). Повторные запросы этих команд настраиваются отдельно (обычные read_retries и write_retries не подойдут, так как при чтении статических таблиц считается нормальным интервал в 60 секунд между повторными запросами, что неприменимо к динамическим таблицам, где латентность должна быть низкой) в секции `dynamic_table_retries`.

Повторные запросы, описанные выше, влияют на процессы загрузки, выгрузки данных с кластера в случае сетевых проблем, недоступности чанков и других проблем. Ниже описаны повторные запросы действий, которые порождаются внутри кластера (запуск mapreduce операции, выполнение батч-запроса).

1. Секция `start_operation_retries` отвечает за повторные запросы команды запуска операции, то есть это не сетевые проблемы, а ошибки вида «concurrent operation limit exceeded», когда одновременно бегущих операций много, и планировщик отказывается запускать новые операции. Клиент повторно выполняет такие ошибки с большими sleep-ами, чтобы какие-то операции успели завершиться.
2. Секция `concatenate_retries` отвечает за повторные запросы команды concatenate (см. [API](../../../api/python/start.md)) , которую нельзя назвать легкой, так как она может ходить к разным мастер-селлам, долго висеть, поэтому повторные запросы для легких команд ей не подходят.
3. Секция `batch_requests_retries` отвечает за повторные запросы запросов внутри batch запроса (см. описание команды `execute_batch`). Клиент повторно выполняет запросы, завершившиеся с ошибками вида «request rate limit exceeded» и так далее. То есть клиент отправляет батчем пачку запросов, какие-то из них завершились, а какие-то упали с ошибками вида «превышен размер очереди запросов на пользователя» и тогда такие запросы снова отправляются новым батчем. Секция регламентирует политику таких повторных запросов.


### Ошибки { #errors }

[YtError](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtError) — базовый класс всех ошибок библиотеки. Имеет следующие поля:

- `code` (тип int) — HTTP код ошибки (смотрите [раздел](../../../api/error-codes.md)). Если не проставлен, то равен 1.
- `inner_errors` (тип list) — ошибки, которые предшествовали данной ошибке при исполнении запроса (прокси обратилась к драйверу, внутри драйвера произошла ошибка, она будет в `inner_errors`).
- `attributes` (тип dict) —  атрибуты ошибки общего назначения (например, дата запроса).

Следующие ошибки описывают более специфичные проблемы:

[YtOperationFailedError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtOperationFailedError) —  операция завершилась неуспешно. У этой ошибки в поле `attributes` есть следующая информация:

- `id` — ID операции.
- `url` — URL операции.
- `stderrs` —  список словарей с информацией про завершившиеся с ошибкой джобы или джобы с stderr. У словаря есть поле `host`, а также могут быть поля `stderr` и `error` в зависимости от того, был ли у джоба stderr и завершился ли джоб с ошибкой.
- `state` — статус операции (например, `failed`).

Чтобы при падении операции распечатать stderr, необходимо обработать исключение и явно напечатать сообщения об ошибках, иначе вы увидите их в урезанном виде в backtrace. Существует непубличный декоратор (вы можете его использовать, но он может быть переименован) [add_failed_operation_stderrs_to_error_message](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.add_failed_operation_stderrs_to_error_message), который перехватывает исключение [YtOperationFailedError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtOperationFailedError) и обогащает его сообщение об ошибке stderr.

[YtResponseError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtResponseError) — команда (запрос к системе {{product-name}}) выполнилась неуспешно. У данного класса есть поле `error`, в котором хранится структурированный ответ, описывающий причину ошибки.
Имеет следующие полезные методы:

- [is_resolve_error](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_resolve_error) (например, возвращает `True` если была попытка закоммитить несуществующую транзакцию или запросить несуществующий узел);

- [is_access_denied](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_access_denied);

- [is_concurrent_transaction_lock_conflict](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_concurrent_transaction_lock_conflict);

- [is_request_queue_size_limit_exceeded](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_request_queue_size_limit_exceeded);

- [is_chunk_unavailable](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_chunk_unavailable);

- [is_request_timed_out](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_request_timed_out);

- [is_concurrent_operations_limit_reached](https://pydoc.ytsaurus.tech/yt.html#yt.common.YtResponseError.is_concurrent_operations_limit_reached).

Другие наследники `YtError`:
- [YtHttpResponseError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtHttpResponseError) — наследник [YtResponseError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtResponseError), имеет поле `headers`;

- [YtProxyUnavailable](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtProxyUnavailable) — proxy сейчас недоступна, например, она слишком загружена запросами;
- [YtTokenError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtTokenError) — передан неверный токен;
- [YtFormatError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtFormatError) — неверный формат;
- [YtIncorrectResponse](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtIncorrectResponse) — некорректный ответ от прокси (например, некорректный JSON);
- [YtTimeoutError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtTimeoutError) — операция завершилась по таймауту.

Полный список наследников можно посмотреть в коде.

### Форматы { #formats }

Подробнее про форматы можно прочитать [в разделе](../../../user-guide/storage/formats.md).

Для каждого есть имеется отдельный класс:
  - [YsonFormat](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.YsonFormat);
  - [JsonFormat](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.JsonFormat);
  - [DsvFormat](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.DsvFormat);
  - [SchemafulDsvFormat](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.SchemafulDsvFormat);
  - [YamrFormat](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.YamrFormat).


В конструкторе эти классы принимают параметры, специфичные для конкретных форматов. Если опция формата недоступна в явном виде в конструкторе, то её можно передать через опцию `attributes`, принимающую `dict`.

Кроме того, у каждого класса имеется набор методов для сериализации/десериализации записей из потока:

- [load_row](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.Format.load_row) — вычитывает из потока одну запись. Некоторые форматы (например, Yson) не поддерживают загрузку одной записи, поэтому вызов `load_row` у них выбросит исключение;
- [load_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.Format.load_rows) — вычитывает из потока все записи, обрабатывает переключатели таблиц, возвращает итератор по записям. Если `raw==True`, то метод возвращает строки, не выполняя парсинг. Метод является генератором;
- [dump_row](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.Format.dumps_row) — записывает в поток одну запись;
- [dump_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.Format.dumps_rows) — записывает в поток набор записей. Добавляет в выходной поток переключатели таблиц.

Также есть функция [create_format](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.create_format), которая создает формат по указанному имени и с указанным набором атрибутов. Также в имени формата могут присутствовать атрибуты в YSON-формате, то есть можно создать формат следующим образом `create_format("<columns=[columnA;columnB]>schemaful_dsv")`.

С помощью функции [create_table_switch](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.create_table_switch), которая позволяет создать запись, являющую переключателем на таблицу с указанным индексом.

У функций `read_table, write_table, select_rows, lookup_rows, insert_rows, delete_rows` есть параметр `raw`, который позволяет указать, что вы хотите получать/передавать записи в нераспарсенном виде (в виде строк). Если параметр `raw` равен `False`, то это означает, что записи будут десериализованы в `dict` при чтении и сериализованы из `dict` при записи.

Пример:

```python
yt.write_table("//home/table", [b"x=value\n"], format=yt.DsvFormat(), raw=True)
assert list(yt.read_table("//home/table", format=yt.DsvFormat(), raw=False)) == [{"x": "value"}]  # Ok
```

Аналогичная опция есть для операций. По умолчанию операция десериализует записи из формата, указанного при запуске операции. Если вы хотите, чтобы операция принимала на вход нераспарсенные строки, то используйте декоратор `raw` и указывайте формат при запуске операции:

```python
@yt.raw
def mapper(rec):
    ...

yt.run_map(...., format=yt.JsonFormat())
```

{% note warning "Внимание" %}

Для использования YSON формата для работы с таблицами (операции, чтение/запись таблиц) вам необходимо будет дополнительно установить пакет с YSON C++ биндингами, смотрите [yson bindings](#yson_bindings). В противном случае вылетит исключение «Yson bindings required».

{% endnote %}

Особенности работы с JSON форматом: модуль JSON написан на Python, поэтому работает очень медленно. Библиотека старается использовать разные модули, для которых есть биндинги, написанные на более быстрых языках, например ujson. Включить его можно с помощью параметра [enable_ujson](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.format.JsonFormat): `JsonFormat(..., enable_ujson=True)`. По умолчанию ujson выключен, так как в некоторых случаях его поведение по умолчанию не является корректным:

```python
import ujson
s = '{"a": 48538100000000000000.0}'
ujson.loads(s)
{u'a': 1.1644611852580897e+19}
```

Если после прочтения документации к ujson есть понимание, что ваши входные данные будут корректно десериализованы, то рекомендуется использовать данный модуль для парсинга.


### YPath { #ypath }

Пути в {{product-name}} представляются в виде так называемого [YPath](../../../user-guide/storage/ypath.md). В коде для этого используется класс [YPath](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.YPath), а также его наследники [TablePath](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.TablePath) и [FilePath](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.FilePath). В конструкторах последних двух классов можно указывать соответствующие YPath-атрибуты, например, `schema`, `start_index`, `end_index` для `TablePath` и `append` и `executable` для `FilePath`. Подробнее про `TablePath` можно прочитать в [разделе](#tablepath_class).
Рекомендуется использовать указанные классы для работы с путями в коде вместо ручного форматирования YPath-литералов.

Также в модуле [YPath](https://pydoc.ytsaurus.tech/yt.wrapper.html#module-yt.wrapper.ypath) есть ещё несколько полезных функций, вот некоторые их них:
- [ypath_join](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.ypath_join) — объединяет несколько частей пути в один путь (аналог `os.path.join`);
- [ypath_split](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.ypath_split) — возвращает пару `(head, tail)`, где `tail` — последняя компонента пути, а `head` — остальное (аналог `os.path.split`);
- [ypath_dirname](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.ypath_dirname) — возвращает путь без последней компоненты (аналог `os.path.dirname`);
- [escape_ypath_literal](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.escape_ypath_literal) — экранирует строку для использования в качестве компоненты YPath.


### Python3 и байтовые строки { #python3_strings }

В {{product-name}} все строки байтовые, а в Python3 обычные строки юникодные, поэтому при работе со структурированными данными (словарями) в Python3 в форматах появилась следующая особенность. При записи и чтении таблиц в `raw` режиме библиотека работает с бинарными строками на вход и на выход.
В не-`raw` режиме включается следующая логика.

По умолчанию строки при чтении будут автоматически закодированы в utf-8, включая ключи словарей (выбрать другую кодировку можно с помощью параметра `encoding` у формата, смотрите далее). При этом если встретится байтовая строка, вернётся специальный объект типа [YsonStringProxy](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.yson_types.YsonStringProxy). Для такого объекта все попытки работать с ним как со строкой приведут к характерной ошибке [NotUnicodeError](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.yson_types.NotUnicodeError). Для этого объекта можно вызвать две функции: [yt.yson.is_unicode](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.yson_types.is_unicode) и [yt.yson.get_bytes](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.yson_types.get_bytes). Первая вернёт `False` для `YsonStringProxy`, а вторая вернёт сырые байты, которые не получилось декодировать. Те же функции можно вызывать и для обычных строк, для них ожидаемо `is_unicode` вернёт `True`. При вызове `get_bytes` для обычных строк можно указать вторым аргументом кодировку (по умолчанию `utf-8`) и получить `s.encode(encoding)`.

Предполагаемый сценарий работы со смешанными, обычными и байтовыми строками при чтении таблиц или в операциях следующий:
```python
## Если просто нужны байты
b = yt.yson.get_bytes(s)

## Если хочется по-разному отреагировать на байтовую и обычную строки
if yt.yson.is_unicode(s):
    # Обрабатываем случай обычной строки
else:
    b = yt.yson.get_bytes(s)
    # Обрабатываем байтовую строку
```
При записи можно как оставлять объект `YsonStringProxy` (он автоматически превратится в байтовую строку), так и отдавать байтовую или unicode строку. Unicode строки будут закодированы в UTF-8 (или в другую кодировку).

**Обратите внимание**, что в ключах словарей запрещено смешивать `bytes` и `str`. При `encoding != None` единственный способ указать байтовый ключ — использовать функцию [yt.yson.make_byte_key](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.yson_types.make_byte_key). Причина в том что в Python3 строки `"a"` и `b"a"` не равны. Недопустимо чтобы словарь вида: `{"a": 1, b"a": 1}` неявно отправлялся в систему, превращенный в строку с двумя одинаковыми ключами `a`.

При необходимости можно отключить логику по декодированию строк или выбрать другую кодировку. Для этого необходимо использовать параметр `encoding` при создании формата. Если параметр `encoding` указан и равен `None`, то библиотека работает с записями, в которых все строки ожидаются бинарными (и ключи, и значения; как при чтении, так и при записи). При попытке сериализации словаря с unicode строками c указанным `encoding=None` в большинстве форматов будет возникать ошибка.

Если параметр `encoding` указан и не равен `None`, то библиотека работает с unicode строками, но предполагает указанную кодировку для всех данных вместо UTF-8, которая используется по умолчанию.

Пример использования с комментариями можно найти [в отдельном разделе](../../../api/python/examples.md#yson_string_proxy)

### Batch-запросы

Низкоуровневое API:

- [execute_batch](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.execute_batch) — принимает набор описаний запросов в виде списка и возвращает набор результатов. Простая обертка над [командой API](../../../api/commands.md#execute_batch).

Высокоуровневое API:

- [create_batch_client](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.batch_helpers.create_batch_client) — создает batch-клиент. Функция доступна, как у клиента, так и глобально. Batch-клиент имеет в качестве методов все функции API, которые поддерживаются в batch-запросах.
- [batch_apply](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.batch_apply) — данный метод позволяет применить функцию к набору данных в batch-режиме. Возвращает список полученных результатов.

Пример:

```python
client = yt.YtClient("cluster-name")

batch_client = client.create_batch_client()
list_rsp = batch_client.list("/")
exists_rsp = batch_client.exists("/")
batch_client.commit_batch()

print(list_rsp.get_result())
print(exists_rsp.get_result())
```

Подробней про batch-запросы можно узнать в [tutorial](../../../user-guide/storage/batch-requests.md).

Особенности при работе с batch клиентом:

- У метода [create_batch_client](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.create_batch_client) и в конфигурации клиента имеется параметр `max_batch_size`, по умолчанию равный 100. Когда у клиента вызывается метод [commit_batch](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.batch_client.BatchClient.commit_batch), запросы разделяются на части по `max_batch_size` и выполняются указанными частями. Так сделано из-за естественных ограничений на размер одного запроса.

- Все запросы отправляются с транзакцией клиента, из которого сконструирован batch-client. Если клиент используется внутри транзакции, то все запросы от него выполняются в контексте этой транзакции. Батч-клиент ведет себя аналогично.

- По умолчанию все ошибки, которые возникают в запросах, можно обработать, посмотрев на [BatchResponse](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.batch_response.BatchResponse), который возвращают методы батч-клиента.

   Пример:

   ```python
   client = yt.YtClient("cluster-name")
   batch_client = client.create_batch_client()
   list_rsp = batch_client.list("//some/node")
   if list_rsp.is_ok():
       # ...
   else:
       error = yt.YtResponseError(list_rsp.get_error())
       if error.is_resolve_error():
           # Обрабатываем ситуацию, когда узла, на который сделан list не существует
           # ...
   ```

У метода [create_batch_client](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.create_batch_client) имеется параметр `raise_errors`, который можно указать равным `True`, тогда, если хоть один запрос выполнился неуспешно, появится исключение [YtBatchRequestFailedError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.batch_execution.YtBatchRequestFailedError) со всеми ошибками. Пример:

```python
client = yt.YtClient("cluster-name")
batch_client = client.create_batch_client(raise_errors=True)
list_rsp = batch_client.list("//some/node")
try:
    batch_client.commit_batch()
except yt.YtBatchRequestFailedError as err:
   # Напечатаем сообщение об ошибке
   print err.inner_errors[0]["message"]  # "Error resolving path //some/node"
```


## Команды { #commands }

Библиотека yt делает команды доступными в Python API системы. Публичной частью библиотеки являются только методы, находящиеся в `yt/wrapper/__init__.py` и `yt/yson/__init__.py`.

Ряд опций в командах являются общими опциями для целых классов команд. Подробнее можно прочитать в разделе [Команды](../../../api/commands.md).

### Работа с Кипарисом { #cypress_commands }

- [get](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.get) — получить значение узла Кипариса. [Подробнее](../../../user-guide/storage/cypress-example.md#get).

- [set](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.set) — записать значение в узел Кипариса. [Подробнее](../../../user-guide/storage/cypress-example.md#set).

- [create](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.create) — создать пустой узел Кипариса типа `type` и атрибутами `attributes`. [Подробнее](../../../user-guide/storage/cypress-example.md#create).

- [exists](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.exists) — проверить существование узла Кипариса. [Подробнее](../../../user-guide/storage/cypress-example.md#exists).

- [remove](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.remove) — удалить узел Кипариса. [Подробнее](../../../user-guide/storage/cypress-example.md#remove).

- [list](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.list) — получить список детей узла `path`. Опция `absolute` включает вывод абсолютных путей вместо относительных. [Подробнее](../../../user-guide/storage/cypress-example.md#list).

Примеры:

```python
yt.create("table", "//home/table", attributes={"mykey": "myvalue"}) # Output: <id of the created object>
yt.get("//home/table/@mykey")  # Output: "myvalue"
yt.create("map_node", "//home/dir") # Output: <id of the created object>
yt.exists("//home/dir")  # Output: True

yt.set("//home/string_node", "abcde")
yt.get("//home/string_node")  # Output: "abcde"
yt.get("//home/string_node", format="json")  # Output: '"abcde"'

yt.set("//home/string_node/@mykey", "value")  # Установить атрибут
yt.get("//home/string_node/@mykey")  # Output: "value"

## Создает список и добавляет в его конец число 7 и строку
yt.create("list_node", "//home/lst")
yt.set("//home/lst/end", 7)
yt.set("//home/lst/end", "cabbage")
yt.get("//home/lst")  # Output: [7L, "cabbage"]
```

- [copy](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.copy) и [move](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.move) — скопировать/переместить узел Кипариса. Подробнее про значение опций можно прочитать в [разделе](../../../user-guide/storage/cypress-example.md#copy_move).

- [link](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.link) — сделать символьную ссылку на узел Кипариса. [Подробнее](../../../user-guide/storage/cypress-example.md#link).
  Чтобы узнать, куда указывает символьная ссылка, необходимо прочитать значение атрибута `@path`. Для обращения к объекту типа `link` используйте `&` в конце пути.

Примеры:

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

Функция-алиас для создания директории:

- [mkdir](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.mkdir) — создает директорию, то есть узел типа `map_node`.

{% cut "Функции-алиасы для работы с атрибутами" %}

- [get_attribute](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.get_attribute)
- [has_attribute](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.has_attribute)
- [set_attribute](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.set_attribute)
- [remove_attribute](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.remove_attribute)

Обратите внимание, что данные функции намеренно не поддерживают обращение к вложенным атрибутам.
Для работы с вложенными атрибутами стоит использовать обычные глаголы Кипариса и навигацию с помощью [YPath](../../../user-guide/storage/ypath.md).

{% endcut %}

Объединение файлов/таблиц:

- [concatenate](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.concatenate) – объединяет чанки из таблиц или файлов.

Прочие команды:

- [find_free_subpath](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.find_free_subpath) — ищет свободный узел, начинающийся с пути `path`.

- [search](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.search) — выполняет рекурсивный обход поддерева, растущего из узла root. По умолчанию выдаются абсолютные пути всех узлов поддерева, также имеется ряд фильтров, чтобы отбирать отдельные записи. Опция `attributes` обозначает список атрибутов, которые должны быть извлечены с каждым узлом. Извлеченные атрибуты доступны в поле `.attributes` у возвращаемых путей.

  Пример:

  ```python
  for table in yt.search(
      "//home",
      node_type=["table"],
      object_filter=lambda obj: obj.attributes.get("account") == "dev",
      attributes=["account"],
  ):
      print(table)
  ```


### Работа с файлами { #file_commands }

Подробнее про файлы в Кипарисе можно прочитать в [разделе](../../../user-guide/storage/files.md) .

- [read_file](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.read_file)

  Прочитать файл из Кипариса на локальную машину. Возвращает объект [ResponseStream](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.response_stream.ResponseStream), который является итератором по строкам со следующими дополнительными методами:

  - [read](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.response_stream.ResponseStream.read) — прочитать из потока `length` байтов. Если `length==None`, то прочитать все до конца;
  - [readline](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.response_stream.ResponseStream.readline) — прочитать строку (включая "\n");
  - [chunk_iter](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.response_stream.ResponseStream.chunk_iter) — итератор по чанкам ответа.

  Команда поддерживает повторные запросы (retries, включены по умолчанию). Включить/выключить, увеличить число повторных запросов можно через опцию конфигурации `read_retries` (см. `read_table`). Повторные запросы на чтение можно включить через переменную `YT_RETRY_READ=1`.

- [write_file](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.write_file)
  Записать файл в Кипарис. Команда принимает поток, из которого читает данные. Команда поддерживает повторные запросы. Повторные запросы можно настроить через опцию конфигурации `write_retries` (подробнее см. [write_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.write_table)). Существует параметр `is_stream_compressed`, который указывает, что данные в потоке уже сжаты и можно передавать их напрямую прокси без пережатия.

- Файлы могут быть переданы в качестве аргументов операции. В таком случае они попадают в корень директории, где будут запускаться джобы. Подробнее можно прочитать в [разделе](#run_operation_commands) и [примере](../../../api/python/examples.md#files).

- [get_file_from_cache](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.get_file_from_cache)
  Возвращает путь к файлу в кеше по заданной md5 сумме

- [put_file_to_cache](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.put_file_to_cache)
  Загрузить файл из указанного пути в Кипарисе в кеш. Обратите внимание, что файл должен был быть загружен в Кипарис с указанием специальной опции, включающей вычисление md5.

### Работа с таблицами { #table_commands }

Подробнее про таблицы можно прочитать в разделе [Статические таблицы](../../../user-guide/storage/static-tables.md).

#### Датаклассы { #dataclass }
Основной способ представления строк таблицы — это классы с полями, проаннотированными типами (аналог [dataclasses](https://docs.python.org/3/library/dataclasses.html)). Такое представление позволяет эффективно (де)сериализовывать данные, допускать меньше ошибок и удобнее работать со сложными типами (структурами, списками и т.п.). Для определения датакласса используется декоратор `yt_dataclass`. Например:

```python
@yt.yt_dataclass
class Row:
    id: int
    name: str
    robot: bool = False
```

После двоеточия указывается тип поля. Это может быть либо обычный тип Python, либо тип из модуля [typing](https://docs.python.org/3/library/typing.html), либо же специальный тип, например, `OtherColumns`. Подробнее можно прочитать в разделе [Датаклассы](../../../api/python/dataclass.md#types). Объект данного класса можно создавать обычным образом: `row = Row(id=123, name="foo")`. При этом для всех полей, для которых не указаны дефолтные значения (как для `robot: bool = False`), необходимо передать соответствующие поля в конструктор, иначе будет порождено исключение.

Для датаклассов допустимо наследование. Подробнее см. в разделе [Датаклассы](../../../api/python/dataclass.md). Также смотрите [пример](../../../api/python/examples.md#dataclass).

#### Схемы { #table_schema }

Каждая таблица в YT имеет [схему](../../../user-guide/storage/static-schema.md). В Python API есть соответствующий класс [TableSchema](https://pydoc.yt.yandex.net/yt.wrapper.schema.html#yt.wrapper.schema.table_schema.TableSchema). Основной способ создания схемы — из [датакласса](#dataclass): `schema = TableSchema.from_row_type(Row)`, в частности, это происходит автоматически при записи таблицы.Однако иногда приходится собирать схему вручную, это может быть удобно делать с помощью builder-интерфейса, например:

```python
import yt.type_info.typing as ti

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

Тип колонки должен быть типом из библиотеки [type_info](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/yt/type_info).
Составные типы (`Optional`, `List`, `Struct`, `Tuple` и так далее) задаются с помощью квадратных скобок.

Схему можно указывать при создании или записи (пустой) таблицы (в атрибуте `schema` класса [TablePath](#tablepath_class)). Получить схему таблицы можно так:

```python
schema = TableSchema.from_yson_type(yt.get("//path/to/table/@schema"))
```

#### TablePath { #tablepath_class }

Все команды для работы с таблицами (в том числе операции) принимают в качестве входных и выходных таблиц не только строки, но и класс [TablePath](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.TablePath) (там, где это разумно). Данный класс представляет из себя путь к таблице с некоторыми модификаторами (обертка над [YPath](../../../user-guide/storage/ypath.md) для таблиц). Его конструктор принимает:

- `name` — путь к таблице в Кипарисе;
- `append` — добавлять записи в конец таблицы вместо перезаписи;
- `sorted_by` — набор колонок, по которым должна быть отсортирована таблица при записи в неё;
- `columns` — список выбранных колонок;
- `lower_key, upper_key, exact_key` — нижняя/верхняя/точная граница чтения, определяемая ключом. Только для сортированных таблиц;
- `start_index, end_index, exact_index` — нижняя/верхняя/точная граница чтения, определяемая номерами строк;
- `ranges` — опция, через которую можно задать произвольный набор диапазонов для чтения;
- `schema` — [схема таблицы](#table_schema); имеет смысл при создании или записи в пустую или несуществующую таблицу;
- `attributes` — опция, через которую можно задать любые дополнительные атрибуты.

Диапазоны являются полуинтервалами (не включают верхнюю границу). Обратите внимание, что часть модификаторов имеет смысл только при чтении данных из таблицы (все атрибуты, касающиеся диапазонов, columns, ranges), а часть модификаторов только при записи в таблицу (append, sorted_by). В качестве `name` можно передавать строку с ypath-модификаторами и ypath-атрибутами, они будут корректно прочитаны и переложены в поле `attributes`. У объекта [TablePath](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.ypath.TablePath) поле `attributes` доступно как на чтение, так и на запись.

Пример:

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

#### Команды { #table_commands }

- [create_temp_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.create_temp_table)

    Создает временную таблицу в директории `path` с префиксом `prefix`. Если `path` не указан, то будет использована директория из конфига: `config["remote_temp_tables_directory"]`. Для более удобной работы существует обертка с поддержкой with_statement, принимающая те же параметры.
    Пример:

    ```python
    with yt.TempTable("//home/user") as table1:
        with yt.TempTable("//home/user", "my") as table2:
            yt.write_table_structured(table1, Row, [Row(x=1)])
            yt.run_map(..., table1, table2, ...)
    ```

- [write_table_structured](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.write_table_structured)

    Записывает строки типа `row_type` (обязан быть [`yt_dataclass`-ом](#dataclass)) из `input_stream` в таблицу `table`.
    В случае, если таблица отсутствует, предварительно производится её создание со схемой.     Команда поддерживает повторные запросы. Повторные запросы можно настроить через опцию конфига `write_retries`.

    {% note warning "Внимание" %}

    Запись с повторными запросами потребляет больше памяти, чем обычная запись, так как запись буферизует записываемые строки в чанки перед записью
    (если записать чанк не получается, происходит повторный запрос), размер каждого чанка по умолчанию составляет 512МБ (см. [опцию конфигурации](https://github.com/ytsaurus/ytsaurus/blob/49f99ef8659f108f94d2d086f5f1dacfddb6b553/yt/python/yt/wrapper/default_config.py#L535)).

    {% endnote %}

    Опция `table_writer` позволяет указать разные системные [параметры записи](../../../user-guide/storage/io-configuration.md). Для записи сырых или сжатых данных используйте функцию `write_table`.

    Пример:
    ```python
    @yt.yt_dataclass
    class Row:
        id: str
        ts: int

    yt.write_table_structured("//path/to/table", Row, [Row(id="a", ts=10)])
    ```

    При записи в пустую или несуществующую таблицу схема будет создана автоматически.
    В более сложных случаях может потребоваться изготавливать схему вручную, подробнее можно прочитать в [разделе](#table_schema) и [примере](../../../api/python/examples.md#table_schema).

- [read_table_structured](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.read_table_structured)

    Читает таблицу в виде последовательности строк типа `row_type` (обязан быть [`yt_dataclass`-ом](#dataclass)).
    Команда поддерживает повторные запросы (включены по умолчанию). Настроить повторные запросы можно через опцию конфигурации `read_retries`.
    Опция `table_reader` (dict) позволяет указать разные системные [параметры чтения](../../../user-guide/storage/io-configuration.md#table_reader).
    Опция `unordered` (bool) позволяет заказать неупорядоченное чтение. В таком случае данные могут читаться быстрее, но порядок их чтения не будет гарантирован.
    Опция `response_parameters` (dict) позволяет передать в неё dict, в который будут дозаписаны специальные параметры команды read (сейчас это два параметра: `start_row_index` и `approximate_row_count`).

    Возвращаемый итератор поддерживает метод `.with_context()`, который возвращает итератор на пары вида `(row, ctx)`. Второй элемент позволяет получать индекс текущей строки и диапазона с помощью методов `ctx.get_row_index()` и `ctx.get_range_index()` (аналогичный итератор внутри джоба позволяет также получить индекс таблицы: `ctx.get_table_index()`). Про контекст в [обычном чтении](../../../api/python/examples.md#read_write) и [внутри операций](../../../api/python/examples.md#table_switches) есть примеры в туториале.

    Подробнее про чтение с повторными запросами: если чтение происходит с повторными запросами, создается транзакция в текущем контексте и на таблицу берется snapshot-лок. Блокировка держится до тех пор, пока не будет вычитан весь поток данных, либо не будет вызван `.close()` у стрима или итератора. Такая схема может приводить к разным ошибкам, например, следующий код **не будет** работать, так как из-за вложенной транзакции чтения не получится закоммитить транзакцию, явно созданную в коде (вложенная транзакция на чтение не завершена, так как `read_table_structured` создал итератор, который не был использован):

    ```python
    with Transaction():
        write_table_structured(table, Row, rows)
        read_table_structured(table, Row)
    ```

Примеры:

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

Функции-алиасы для работы с таблицами:

- [row_count](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.row_count) ­— возвращает количество записей в таблице
- [is_empty](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.is_empty) — проверяет, является ли таблица пустой
- [is_sorted](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.is_sorted) — проверяет, отсортирована ли таблица

Примеры:

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

- [write_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.write_table)
    Нетипизированный аналог `write_table_structured`, **не рекомендован к использованию**.

    {% cut "Подробнее" %}

    Записывает строки из `input_stream` в таблицу `table`.
    Если таблица отсутствует, предварительно производится ее создание. Повторные запросы можно настроить через опцию конфига `write_retries`.
    См. [пример](../../../api/python/examples.md#read_write_untyped) в соответствующем разделе.

    {% note warning "Внимание" %}

    Запись с повторными запросами потребляет больше памяти, чем обычная запись, так как запись буферизует записываемые строки в чанки перед записью
    (если записать чанк не получается, то происходит повторный запрос), размер каждого чанка по умолчанию составляет 512МБ (см. [опцию конфигурации](https://github.com/ytsaurus/ytsaurus/blob/49f99ef8659f108f94d2d086f5f1dacfddb6b553/yt/python/yt/wrapper/default_config.py#L535)).

    {% endnote %}

    Опция `table_writer` позволяет указать разные системные [параметры записи](../../../user-guide/storage/io-configuration.md). Также есть параметр `is_stream_compressed`, который указывает, что данные в потоке уже сжаты, и можно передавать их напрямую прокси без пережатия. Нужно иметь в виду, что при передаче сжатых данных требуется указать соответствующий `Content-Encoding` через конфигурацию: `config["proxy"]["content_encoding"] = "gzip"`, а также установить опцию `raw=True`. Опция `raw` регулирует то, в каком формате ожидаются данные. Если `raw=False`, то ожидается итератор по python-структурам. Если `raw=True`, то ожидается либо строка, либо итератор по строкам, либо поток, в котором должны быть данные в формате `format`.

    Примеры:

    ```python
    yt.write_table("//path/to/table", ({"a": i, "b": i * i} for i in xrange(100)))
    yt.write_table("//path/to/table", open("my_file.json"), format="json", raw=True)
    yt.write_table("//path/to/table", "a=1\na=2\n", format="dsv", raw=True)
    ```
    Чтобы записать таблицу со схемой, её нужно сначала отдельно создать:
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
    Нетипизированный аналог `read_table_structured`, **не рекомендован к использованию**.

    {% cut "Подробности" %}

    Читает таблицу в указанном формате. Возвращаемое значение зависит от значения опции `raw`. Если `raw=False` (значение по умолчанию), то возвращается итератор по списку записей. Одна запись представляет собой `dict` или `Record` (в случае формата `yamr`). Если `raw=True`, то возвращается stream-like объект, из которого можно вычитывать данные в формате `format`.
    Команда поддерживает повторные запросы (включены по умолчанию). Настроить повторные запросы можно через опцию конфигурации `read_retries`. При включенных повторных запросах чтение происходит медленнее из-за того, что приходится парсить поток, так как нужно считать число уже прочитанных строк.
    Опция `table_reader` (dict) позволяет указать разные системные [параметры чтения](../../../user-guide/storage/io-configuration.md#table_reader).
    Опция `control_attributes` (dict) позволяет при чтении заказать различные [контрольные атрибуты](../../../user-guide/storage/io-configuration.md#control_attributes)
    Опция `unordered` (bool) позволяет заказать неупорядоченное чтение. В таком случае данные могут читаться быстрее, но порядок их чтения будет не гарантирован.
    Опция `response_parameters` (dict) позволяет передать в нее dict, в который будут дозаписаны специальные параметры команды read (в текущей реализации это два параметра: start_row_index и approximate_row_count).

    смотрите [пример](../../../user-guide/storage/examples.md#read_write_untyped) в соответствующем разделе.

    Подробнее про чтение с повторными запросами: если вы читаете с повторными запросами, создается транзакция в текущем контексте и на таблицу берется snapshot-лок. Этот лок держится до тех пор, пока вы не вычитаете весь поток данных, либо не вызовете `.close()` у стрима или итератора, который вам вернули. Такая схема может приводить к ошибкам, например, следующий код **не будет** работать, так как из-за вложенной транзакции чтения не получится закоммитить транзакцию, явно созданную в коде (вложенная транзакция на чтение не завершена, так как [read_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.read_table) создал итератор, который не был использован):

    ```python
    with Transaction():
        write_table(table, rows)
        read_table(table, format="yson")
    ```

    {% note warning "Внимание" %}

    Чтение с повторными запросами обычно работает медленней, чем без них, так как приходится парсить поток записей.

    {% endnote %}

    {% endcut %}


#### Параллельное чтение таблиц и файлов { #parallel_read }

Таблица разбивается на небольшие диапазоны исходя из предположения, что данные распределены по строкам равномерно. Каждый такой диапазон читается отдельным потоком. В случае включенных повторных запросов будет повторно обрабатываться весь диапазон, а не отдельные строки таблицы. Такой подход позволяет не производить парсинг данных и ускоряет чтение.
Файлы просто разбиваются на части заданного размера и читаются параллельно.

Настройки параллельного чтения находятся в секции конфигурации `read_parallel`, имеющей следующие ключи:
 - `enable` - включение параллельного чтения;
 - `max_thread_count` - максимальное количество потоков;
 - `data_size_per_thread` - количество загружаемых каждым потоком данных.

Особенности параллельного чтения таблиц:

- Заказ контрольных атрибутов не поддерживается.
- Не поддерживается указание диапазонов строк таблицы с лимитами в виде ключей.
- Параллельное чтение может работать неэффективно на таблицах с большим разбросом количества данных в строках.

Пример ускорения:

```bash
$ time yt read-table //sys/scheduler/event_log.2[:#1000000] --proxy cluster-name --format yson > /dev/null

real    1m46.608s
user    1m39.228s
sys     0m4.216s

$ time yt read-table //sys/scheduler/event_log.2[:#1000000] --proxy cluster-name --format yson --config "{read_parallel={enable=%true;max_thread_count=50;}}" > /dev/null

real    0m14.463s
user    0m12.312s
sys     0m4.304s
```

  {% cut "Замеры скорости" %}

```bash
$ export YT_PROXY=cluster-name
$ yt read //sys/scheduler/event_log.2[:#20000000] --format json --config "{read_parallel={enable=%true;max_thread_count=50;}}" > scheduler_log_json
$ yt read //sys/scheduler/event_log.2[:#20000000] --format yson --config "{read_parallel={enable=%true;max_thread_count=50;}}" > scheduler_log_yson

$ ls -lah
total 153G
drwxrwxr-x 2 user group 4.0K Sep 29 21:23 .
drwxrwxr-x 7 user group 4.0K Sep 28 15:20 ..
-rw-r--r-- 1 user group  51G Sep 29 21:25 scheduler_log_json
-rw-r--r-- 1 user group  51G Sep 29 21:22 scheduler_log_yson
-rw-r--r-- 1 user group  51G Sep 29 21:20 test_file

$ time cat scheduler_log_yson | yt write //tmp/test_yson --format yson
real    36m51.653s
user    31m34.416s
sys     1m5.256s

$ time cat scheduler_log_json | yt write //tmp/test_json --format json
real    88m38.745s
user    21m7.188s
sys     1m1.400s

$ time cat test_file | yt upload //tmp/test_file
real    35m50.723s
user    17m31.232s
sys     1m39.132s

$ time cat scheduler_log_yson | yt write //tmp/test_yson --format yson --config "{write_parallel={enable=%true;max_thread_count=30;}}"
real    13m37.545s
user    37m20.516s
sys     4m16.436s

$ time cat scheduler_log_json | yt write //tmp/test_json --format json --config "{write_parallel={enable=%true;max_thread_count=30;}}"
real    3m53.308s
user    23m21.152s
sys     2m57.400s

$ time cat test_file | yt upload //tmp/test_file --config "{write_parallel={enable=%true;max_thread_count=30;}}"
real    1m49.368s
user    18m30.904s
sys     1m40.660s
```

  {% endcut %}

#### Параллельная запись таблиц и файлов { #parallel_write}

Чтобы воспользоваться, необходимо:

1. Установить `zlib_fork_safe`, если вы пользуетесь python2;
2. Включить опцию в конфиге: `config["write_parallel"]["enable"] = True`.

После этого стандартные команды [write_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.write_table) и [write_file](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.write_file) будут работать в многопоточном режиме.

В конфигурации клиента появился новый раздел `write_parallel` со следующими ключами:

- `enable` (False by default) - включает возможность записывать таблицы и файлы в {{product-name}} параллельно.
- `max_thread_count` (10 by default) - максимальное количество потоков для записи.
- `unordered` (False by default) - разрешает записывать строки в таблицу в произвольном порядке, это позволяет ускорить запись.
- `concatenate_size` (100 by default) - ограничение на количество таблиц/файлов, подаваемых на вход команде concatenate.

#### Как это работает

Весь процесс параллельной записи выглядит следующим образом:

1. Входной поток разбивается по строкам, строки группируются в чанки, размер которых регулируется опцией `/write_retries/chunk_size` из конфигурации (по умолчанию 512МБ);
2. Чанки подаются в ThreadPool;
3. Потоки получают на вход группу строк для записи, сжимают эти данные, загружают их на сервер во временную таблицу или файл;
4. Главный поток накапливает временные таблицы или файлы, запись в которые уже была произведена;
5. Как только количество записанных таблиц или файлов становится равно `/write_parallel/concatenate_size`, выполняется их слияние.

#### Ограничения

- Параллельная запись недоступна в случае, если на вход подан поток сжатых данных.
- Если на пути к выходной таблице стоит атрибут `sorted_by`, то параллельная запись также не будет доступна.
- Параллельная запись не будет эффективной в Python2, если не установлен `zlib_fork_safe` (из-за GIL).
4. Параллельная запись не будет эффективной, если парсер вашего формата медленный (например, в случае [schemaful_dsv](../../../user-guide/storage/formats.md#schemaful_dsv)).
5. Поскольку входной поток разбивается на чанки для параллельной загрузки на кластер, пользовательский скрипт будет потреблять память, пропорциональную `max_thread_count * chunk_size` (на практике получается множитель около 2).

**Q: Почему в несколько потоков запись в формате JSON работает в разы быстрее, чем в YSON, а в один поток наоборот?**
**A:** На то есть две причины:
Входной поток нужно разбить на строки. В JSON это можно сделать просто, поделив его по `\n`, в формате YSON это делается намного сложнее. Поскольку эта операция однопоточная, она является слабым местом и вся запись блокируется ей.

### Работа с транзакциями и локами { #transaction_commands }

- [start_transaction](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.start_transaction) — создать новую транзакцию с заданным таймаутом (в мс);

- [abort_transaction](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.abort_transaction) — прервать транзакцию с данным ID;

- [commit_transaction](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.commit_transaction) — закоммитить транзакцию с данным ID;

- [ping_transaction](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.ping_transaction) — выполнить пинг транзакции для продления срока жизни.


Эти функции дают исключение [YtResponseError](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.errors.YtResponseError) с `.is_resolve_error() == True` в случае, если транзакция не найдена.

- [lock](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.lock) — берет лок на указанный узел под текущей транзакцией, записанной в `TRANSACTION_ID`. В случае waitable лока и указанного `wait_for` ждет взятия лока в течение `wait_for` миллисекунд. Если дождаться не удалось — возвращает исключение.

- [unlock](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.unlock) — снимает с указанного узла все [явные](../../../user-guide/storage/transactions.md#implicit_locks) локи, взятые текущей транзакцией (записанной в `TRANSACTION_ID`), — как уже взятые, так и ожидающие в очереди. Если локов нет, не имеет эффекта. Если разблокировка невозможна (потому что заблокированная версия узла содержит изменения по сравнению с оригинальной версией), возвращает исключение.

  {% note info "Примечание" %}

  Завершение транзакции (как успешное, так и нет) снимает все взятые ею блокировки; `unlock` нужен лишь в тех случаях, когда необходимо разблокировать узел, не завершая транзакции.

  {% endnote %}

- [Transaction](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.transaction.Transaction) — **однопоточный** класс-обертка, для создания, коммита или аборта транзакций. Поддерживает синтаксис контекстного менеджера (`with` statement), то есть в случае успешного выхода из scope транзакция коммитится, а иначе прерывается. Все команды в scope запускаются под указанной транзакцией. Поддерживается возможность создавать вложенные scope'ы. Параметр `ping` (по умолчанию равен `True`) в конструкторе отвечает за запуск пингующего треда. При отсутствии пинга операция будет принудительно отменена по истечении таймаута.

Примеры:

```python
with yt.Transaction():
    yt.write_table_structured(table, [Row(x=5)])  # Запись будет проходить под транзакцией
    # После выхода из-под with будет произведен коммит транзакции.

with yt.Transaction():
    yt.write_table_structured(table, [Row(x=6)])
    raise RuntimeError("Something went wrong")
    # Исключение вызовет аборт транзакции и изменений никто не увидит

## Вложенные транзакции
with yt.Transaction():
    yt.lock("//home/table", waitable=True, wait_for=60000)
    with yt.Transaction():
        yt.set("//home/table/@attr", "value")
```

В случае, если пингующий тред завершился с ошибкой при попытке сделать ping транзакции, он вызывает `thread.interrupt_main()`. Данное поведение можно поменять с помощью опции `config["ping_failed_mode"]`.
Доступные опции:
   1. `pass`: не делать ничего
   2. `call_function`: вызывает функцию указанную в поле `ping_failed_function` конфига
   3. `interrupt_main`: бросить исключение `KeyboardInterrupt` в главном треде
   4. `send_signal`: послать процессу сигнал `SIGUSR1`.
   5. `terminate_process`: прибить процесс

### Запуск операций { #run_operation_commands }

Про доступные операции для работы с табличными данными можно в [разделе](../../../user-guide/data-processing/operations/overview.md).

Все функции запуска операций начинаются со префикса `run_`. Данные функции часто имеют множество параметров. Далее будут описаны общие параметры у всех функций запуска операций и логика выбора параметров. Полный список параметров для каждой отдельной функции можно найти в [pydoc](http://pydoc.ytsaurus.tech/yt.wrapper.html).

Команды запуска операций:

- [run_erase](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_erase) — [операция удаления данных из таблицы](../../../user-guide/data-processing/operations/erase.md)
- [run_merge](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_merge) — [операция слияния/обработки данных в таблице без пользовательского кода](../../../user-guide/data-processing/operations/merge.md)
- [run_sort](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_sort) — [операция сортировки данных](../../../user-guide/data-processing/operations/sort.md). В случае, если не указан destination_table делается inplace-сортировка таблицы.
- [run_map_reduce](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_map_reduce) — [операция MapReduce](../../../user-guide/data-processing/operations/mapreduce.md).
- [run_map](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_map) — [операция Map](../../../user-guide/data-processing/operations/map.md).
- [run_reduce](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_reduce) — [операция Reduce](../../../user-guide/data-processing/operations/reduce.md).
- [run_join_reduce](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_join_reduce) — [операция JoinReduce](../../../user-guide/data-processing/operations/reduce.md#foreign_tables).
- [run_operation](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_operation)– запускает операцию.
  По умолчанию команда запускает необходимую операцию as is, опция `enable_optimizations` разрешает оптимизации `treat_unexisting_as_empty`, `run_map_reduce_if_source_is_not_sorted`, `run_merge_instead_of_sort_if_input_tables_are_sorted`, если они включены в конфиге
- [run_remote_copy](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_remote_copy) — [операция RemoteCopy](../../../user-guide/data-processing/operations/remote-copy.md)
  Скопировать таблицу с одного {{product-name}} кластера на другой.


#### SpecBuilder { #spec_builder }

Рекомендуемым способом указывать параметры для запуска операции являются так называемые [spec builder](#spec_builder).

Поддерживаются следующие классы для заполнения спецификации операции:
  - [MapSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MapSpecBuilder);
  - [ReduceSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.ReduceSpecBuilder);
  - [MapReduceSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MapReduceSpecBuilder);
  - [JoinReduceSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.JoinReduceSpecBuilder);
  - [MergeSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MergeSpecBuilder);
  - [SortSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.SortSpecBuilder);
  - [RemoteCopySpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.RemoteCopySpecBuilder);
  - [EraseSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.EraseSpecBuilder);
  - [VanillaSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.VanillaSpecBuilder).

Имена методов соответствуют названиям опций в спецификации, описанным [здесь](../../../user-guide/data-processing/operations/operations-options.md).

В билдерах, позволяющих заполнять спецификацию для операции с пользовательскими джобами, существуют методы `begin_mapper`, `begin_reducer`, `begin_reduce_combiner`. Аналогично, есть методы `begin_job_io`, `begin_map_job_io`, `begin_sort_job_io`, `begin_reduce_job_io`, `begin_partition_job_io`, `begin_merge_job_io` в соответствующих spec builder.

Пример:

```python
import yt.wrapper as yt

if __name__ == "__main__":
    spec_builder = yt.spec_builders.MapSpecBuilder() \
        .input_table_paths("//tmp/input_table") \
        .output_table_paths("//tmp/output_table") \
        .begin_mapper() \
            .command("cat") \
            .format(yt.YsonFormat()) \
        .end_mapper()

    yt.run_operation(spec_builder)
```

Для удобства доступны ещё два типа spec builder:

1. Spec builder'ы для заполнения [I/O параметров](../../../user-guide/storage/io-configuration.md):
   - [PartitionJobIOSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.PartitionJobIOSpecBuilder);
   - [SortJobIOSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.SortJobIOSpecBuilder);
   - [MergeJobIOSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MergeJobIOSpecBuilder);
   - [ReduceJobIOSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.ReduceJobIOSpecBuilder);
   - [MapJobIOSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MapJobIOSpecBuilder).
2. Spec builder'ы для заполнения спецификации пользовательского джоба:
   - [MapperSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MapperSpecBuilder);
   - [ReducerSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.ReducerSpecBuilder);
   - [ReduceCombinerSpecBuilder](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.ReduceCombinerSpecBuilder).

смотрите [пример в туториале](../../../api/python/examples.md#spec_builder).

#### Другие параметры команд запуска операций

Общим для всех функций запуска операций является параметр `sync` (`True` по умолчанию). Если `sync=True`, то при вызове функции будет синхронно происходить ожидание окончания операции. В случае же `sync=False` клиенту вернется объект типа [Operation](#operation_class).

Если интересующего параметра нет в соответствующем spec builder'е, можно использовать параметр `spec` типа `dict`, который позволяет явно задать произвольные опции для спецификации операции (указанный параметр имеет наивысший приоритет при формировании спецификации). Например, можно указать желаемое количество данных на джоб: `yt.run_map(my_mapper, "//tmp/in", "//tmp/out", spec={"data_weight_per_job": 1024 * 1024})` (но конкретно в этом случае правильнее использовать [метод билдера](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.spec_builders.MapSpecBuilder.data_size_per_job)).

Все обязательные параметры спецификации вынесены в отдельные опции. К таким параметрам относятся пути входных/выходных таблиц, команда (для операций с пользовательским кодом). Для операций sort , reduce, map_reduce, join_reduce вынесены в отдельные опции параметры `sort_by`, `reduce_by` и `join_by`.

Параметры `source_table` и `destination_table` могут принимать на вход как путь к таблице, так и список путей к таблицам, корректно обрабатывая оба случая в зависимости от того, ожидает ли операция в данном месте путь к таблице или в набор путей к таблице. В качестве пути к таблице принимается как строки, так и объекты типа [TablePath](#tablepath_class).

В параметры вынесен ряд параметров, которые относительно часто возникает необходимость изменять. К ним относятся `job_io` и его часть `table_writer`. Данные параметры будут добавлены в соответствующие части спецификации. В случае если у операции несколько стадий, то указанные через `job_io` параметры доедут до всех стадий. Для всех операций, имеющих в спецификации параметр `job_count`, он вынесен в отдельную опцию.

Для всех операций с пользовательским кодом в отдельную опцию вынесен `memory_limit` и опции `format`, `input_format`,`output_format`, позволяющие специфицировать [формат входных/выходных данных](../../../user-guide/storage/formats.md). Имеются параметры `yt_files` и `local_files` (и алиас `files`), позволяющие задать пути файлов в Кипарисе и пути к локальным файлам (соответственно), которые приедут в окружение джоба.

Для операции map_reduce все параметры пользовательского скрипта, описанные в предыдущем абзаце, продублированы отдельно для mapper-а, reducer-a и reduce-combiner-а.

В качестве `binary` может быть передана как [команда запуска](../../../user-guide/data-processing/operations/overview.md) в виде строки, так и callable python-объект. Операции с callable-объектами в качестве `binary` называются [python-операциями](#python_operations). В случае, если в качестве `binary` передана команда запуска, обязательно должен быть указан [формат](#python_formats). В случае же callable-объекта передавать формат не обязательно.

Любая операция, которая пишет в destination_table, может не удалять существующую таблицу, а дописывать записи в конец. Для этого в качестве destination_table можно указать [объект типа TablePath](#tablepath_class) с опцией `append=True`.

При запуске операции `map_reduce` с несколькими входными таблицами нужно иметь в виду, что клиент {{product-name}} может добавить во входные записи переключатели таблиц, что может привести к попаданию этих переключателей в выходной поток (если эти записи передавать в выходной поток as is, e.g. mapper из одной строки `yield row`). Это может привести к ошибке записи в несуществующую таблицу. Чтобы избежать подобной ситуации следует либо выключить table index с помощью опции `enable_input_table_index` в спецификации, либо вручную удалять table index перед записью в выходной поток — `del row["@table_index"]`.

У функции [run_map_reduce](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_map_reduce) все опции, специфичные для конкретного джоба, продублированы в трех экземплярах – для mapper-а, для reducer-а и для reduce_combiner-а. То есть у функции есть опции `map_local_files`, `reduce_local_files`, `reduce_combiner_local_files`, и.т.п. Полный список опций можно посмотреть в [pydoc-е](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_map_reduce).

### Работа с операциями и джобами { #operation_and_job_commands }

- [get_operation_state](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.get_operation_state) — возвращает состояние текущей операции. Возвращаемым объектом является класс, у которого есть единственное поле `name` и методы `is_finished, is_running, is_unsuccessfully_finished`.
- [abort_operation](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.abort_operation)— прервать операцию, не сохранив её результат.
- [suspend_operation](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.suspend_operation) — приостановить операцию.
- [resume_operation](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.resume_operation) — продолжить приостановленную операцию.
- [complete_operation](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.complete_operation) — завершить операцию с текущим имеющимся результатом.

При запуске операции с флагом `sync=False` удобнее использовать методы `abort, suspend, resume, complete` класса `Operation` вместо перечисленных выше, смотрите раздел [Operation](#operation_class).


- [run_job_shell](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.run_job_shell) —  запустить job-shell для джоба. Проще использовать данную функцию с помощью аналогичной команды в [CLI](../../../api/cli/examples.md).

#### Получение информации о джобах и операциях { #operation_and_job_info_commands }
Операция имеет довольно нетривиальный жизненный цикл и в разные моменты времени информация об операции может быть получена из разных источников:

1. Кипарис (содержит информацию о выполняющихся и еще не заархивированных операциях)
2. Орхидея контроллер-агента (содержит всю актуальную информацию о выполняющихся операциях)
3. Архив операций (содержит информацию о завершенных операциях)

Так как информация об операции может быть получена из разных источников (которые могут меняться как по составу, так и по своему внутреннему устройству), то существуют следующие методы, которые в любой момент умеют собирать информацию об операции из перечисленных источников.

- [get_operation](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.get_operation) — получить информацию об операции по ее id. Возвращается `dict` с полями, аналогичными полям ответа [get_operation](../../../api/commands.md#get_operation).
- [list_operations](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.list_operations) — получить информацию про набор операций по фильтрам. Смысл полей аналогичен `get_operation`. Список фильтров см. в разделе [Команды](../../../api/commands.md#list_operations).
- [iterate_operations](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.iterate_operations) — получить итератор на набор операций. Функция аналогична `list_operations`, но не имеет ограничения на количество запрашиваемых операций.

Пример - вывести типы трех последних операций, запущенных пользователем `username`:
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

Другой пример - найти все выполняющиеся операции, запущенные в пользовательских эфемерных пулах:
```python
for op in client.iterate_operations(state="running"):
    if "$" in op.get("pool"):
        print(op["id"], op["start_time"])
```

Информация про джобы операции находится в планировщике и в архиве. Следующие методы позволяют получить информацию про джобы:
- [get_job](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.get_job) — получить информацию про джоб. Возвращается `dict` с полями, аналогичными полям ответа [get_job](../../../api/commands.md#get_job).
- [list_jobs](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.list_jobs) — получить информацию про набор джобов операции. Смысл полей в ответе аналогичен `get_job`. Список фильтров см. в разделе [Команды](../../../api/commands.md#list_jobs).
- [get_job_stderr](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.get_job_stderr) — получить stderr джоба.
- [get_job_input](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.get_job_input) — получить полный вход джоба.
- [get_job_input_paths](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.get_job_input_paths) — получить список входных таблиц (с диапазонами строк) джоба.
- [get_job_spec](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.get_job_spec) — получить спецификацию джоба.


{% note warning "Внимание" %}

Вызовы функций из данного раздела внутри себя могут обращаться к мастер-серверам, планировщику, узлам кластера (а обращения к мастер-серверам и планировщику не масштабируются) поэтому не стоит использовать их слишком часто.

{% endnote %}

Для отладки невыполнившихся (failed) джобов удобно использовать [job tool](../../user-guide/problems/jobstatistics.md). Данная утилита позволяет подготовить окружение, аналогичное окружению джоба и запустить его для тех же входных данных.

#### Operation { #operation_class }

Объект данного класса возвращают команды запуска операций `run_*`. Он предоставляет [небольшое API](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.Operation) для работы с уже запущенной (или завершенной) операцией.

#### OperationsTracker { #operations_tracker_class }

Для удобной работы с несколькими операциями можно использовать [OperationsTracker](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTracker). Его также можно использовать для работы с операциями, запущенными на разных кластерах. OperationsTracker имеет следующий интерфейс:

- [add](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTrackerBase.add) — добавить объект типа `Operation` во множество отслеживаемых

- [add_by_id](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTracker.add_by_id) — добавить операцию по id во множество отслеживаемых

- [wait_all](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTrackerBase.wait_all) — дождаться завершения всех операций

- [abort_all](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTrackerBase.abort_all) — прервать все отслеживаемые операции

Пример:

```python
with yt.OperationsTracker() as tracker:
    op1 = yt.run_map("sleep 10; cat", "//tmp/table1", "//tmp/table1_out", sync=False)
    op2 = yt.run_map("sleep 10; cat", "//tmp/table2", "//tmp/table2_out", sync=False)
    tracker.add(op1)
    tracker.add(op2)
    tracker.abort_all()
    tracker.add(yt.run_map("true", table, TEST_DIR + "/out", sync=False))
```
На выходе из `with`-блока трекер дождется завершения всех операций. Если произошло исключение, то все запущенные операции будут прерваны.

{% note warning "Внимание" %}

Использование трекера без `with` является устаревшим и не рекомендованным.

{% endnote %}


#### OperationsTrackerPool { #operations_tracker_pool_class }

Класс [OperationsTrackerPool](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTrackerPool) похож на [OperationsTracker](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTracker), но дополнительно гарантирует, что не будет запущено более чем `pool_size` операций одновременно. На вход методы класса принимают один или несколько spec builder-ов (смотрите соответствующий [раздел](#spec_builder)).
Данный класс создает фоновый поток, который постепенно запускает все операции из очереди, соблюдая гарантии на число одновременно выполняющихся операций.
Интерфейс:

- [add](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTrackerPool.add) — принимает на вход spec builder и добавляет его в очередь на запуск
- [map](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operations_tracker.OperationsTrackerPool.map) — принимает на вход список spec builder-ов. Остальные параметры имеют тот же смысл, что и у метода add.

### Работы с правами доступа { #acl_commands }

- [check_permission](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.check_permission)
- [add_member](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.add_member)
- [remove_member](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.remove_member)

Примеры:

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


### Работа с динамическими таблицами { #dyntables_commands }

[Динамические таблицы](../../../user-guide/dynamic-tables/overview.md) реализуют интерфейс точечного чтения и записи данных по ключу, поддерживающих транзакции и собственный диалект SQL.

Список команд для работы с содержимым динамических таблиц.
- [insert_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.insert_rows) — [вставить](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#insert_rows) строки в динамическую таблицу
- [lookup_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.lookup_rows) — [выбрать](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#chtenie-stroki) строки с заданными ключами
- [delete_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.delete_rows) — [удалить](../../../user-guide/dynamic-tables//sorted-dynamic-tables.md#udalenie-stroki) строки с заданными ключами
- [select_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.select_rows) — [выбрать](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#vypolnenie-zaprosa) строки, удовлетворяющие запросу на [диалекте SQL](../../../user-guide/dynamic-tables/dyn-query-language.md)
- [lock_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.lock_rows) — [заблокировать](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md#blokirovka-stroki) строки с заданными ключами
- [trim_rows](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.trim_rows) — [удалить](../../../user-guide/dynamic-tables/ordered-dynamic-tables#trim) заданное количество строк из упорядоченной таблицы

Команды, связанные с [монтирование](../../../user-guide/dynamic-tables/overview.md#mount_table) таблиц.
- [mount_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.mount_table) — примонтировать динамическую таблицу
- [unmount_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.unmount_table) — отмонтировать динамическую таблицу
- [remount_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.remount_table) — перемонтировать динамическую таблицу

Команды, связанные с [шардированием](../../../user-guide/dynamic-tables/resharding.md).
- [reshard_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.reshard_table) — решардировать таблицу по указанным pivot-ключам
- [reshard_table_automatic](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.reshard_table_automatic) — пошардировать примонтированные таблеты в соответствии с конфигурацией таблет-балансера.

Другие команды.
- [freeze_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.freeze_table) — [заморозить](../../../user-guide/dynamic-tables/overview.md#zamorozka) таблицу
- [unfreeze_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.unfreeze_table) — [разморозить](../../../user-guide/dynamic-tables/overview.md#zamorozka) таблицу
- [balance_tablet_cells](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.balance_tablet_cells) — распределить таблеты равномерно по [таблет-селлам](../../../user-guide/dynamic-tables/overview.md#tablet_cells)
- [get_tablet_infos](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.get_tablet_infos) — получить атрибуты [таблета](../../../user-guide/dynamic-tables/overview.md#tablets)
- [get_tablet_errors](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.get_tablet_errors) — получить ошибки [таблета](../../../user-guide/dynamic-tables/overview.md#tablets)
- [alter_table_replica](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.dynamic_table_commands.alter_table_replica) — [изменить атрибуты](../../../user-guide/dynamic-tables/replicated-dynamic-tables.md#nastrojki-replik) реплики [реплицированной таблицы](../../../user-guide/dynamic-tables/replicated-dynamic-tables.md)

### Другие команды { #etc_commands }

#### Преобразования таблиц

**Transform.** Иногда бывает необходимо переложить таблицу в новый кодек сжатия или erasure, переложить таблицу в [новый формат чанка](../../../user-guide/storage/chunks.md). В решении таких задач поможет функция [transform](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.transform).

Функция запускает `merge`-операцию над входной таблицей, выполняя пережатие данных в соответствии с указанными параметрами. Если опция `check_codecs` выставлена в `True`, то функция проверяет, сжаты ли уже данные, и если да, то операция не запускается. Если указана опция `optimize_for`, то операция запускается всегда (опция `check_codecs` игнорируется), так как нет возможности проверить какой формат имеют чанки таблицы.

**Shuffle.** Для перемешивания строк таблицы в случайном порядке существует функция [shuffle_table](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.client_impl.YtClient.shuffle_table).
Функция запускает операцию `map_reduce`, сортирующую таблицу по добавленной колонке со случайным числом.

## Python-объекты в качестве операций { #python_operations }

### Общая информация { #python_operations_intro }

Для того, чтобы запустить операцию, необходимо описать специальный класс-наследник `yt.wrapper.TypedJob` и передать объект данного класса в [функцию](#run_operation_commands) запуска операции (либо указать в соответствующем поле [SpecBuilder-а](#spec_builders)).

 В классе джоба обязательно должен быть определен метод `__call__(self, row)` (для mapper-а) или `__call__(self, rows)` (для reducer-а). На вход данному методу приходят строки таблицы (в случае reducer-а один вызов `__call__` соответствует набору строк с одинаковым ключом). Он обязан вернуть (**с помощью `yield`**) строки которые нужно записать в выходную таблицу. Если выходных таблиц несколько, нужно использовать класс-обёртку `yt.wrapper.OutputRow`, конструктор которого принимает записываемую строку и `table_index` в виде именованного параметра (смотрите [пример](../../../api/python/examples.md#table_switches) в туториале).

Дополнительно можно определить методы `start(self)` (будет вызван ровно один раз перед обработкой записей джоба) и `finish(self)` (будет вызван один раз после обработки записей джоба), которые, как и `__call__`, могут генерировать (с помощью `yield`) новые записи, что позволяет, например, удобно делать агрегирующие операции (не вызываются для "честных" агрегирующих операций типа `@yt.aggregator`). А также метод [`.prepare_operation(self, context, preparer)`](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.TypedJob.prepare_operation). Он используется для указания типов строк входных и выходных таблиц, а также для модификации спеки операции. Более подробно смотрите [ниже](#prepare_operation) и примеры в туториале: [раз](../../../api/python/examples.md#prepare_operation) и [два](#examples.md#grep).

### Подготовка операции из джоба { #prepare_operation }

Для указания входных и выходных типов строк в классе джоба можно использовать тайп хинты (смотрите примеры в туториале: [раз](../../../api/python/examples.md#simple_map), [два](../../../api/python/examples.md#multiple_input_reduce), [три](../../../api/python/examples.md#multiple_input_multiple_output_reduce) и [четыре](../../../api/python/examples.md#map_reduce_multiple_intermediate_streams)), либо переопределить метод [`.prepare_operation(self, context, preparer)`](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.TypedJob.prepare_operation). Указание типов производится через методы объекта `preparer` типа [`OperationPreparer`](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.OperationPreparer). Полезные методы:
   1. [`inputs`](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.OperationPreparer.inputs): позволяет указать для нескольких входных таблиц тип входной строки (обязан быть классом с декоратором [`@yt.wrapper.yt_dataclass`](#dataclass)), список имён колонок, которые нужны джобу, а также переименования колонок.
   2. [`outputs`](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.OperationPreparer.outputs): позволяет указать для нескольких выходных таблиц тип выходной строки (обязан быть классом с декоратором [`@yt.wrapper.yt_dataclass`](#dataclass)), а также схему, которую хочется вывести для этих таблиц (по умолчанию схема выводится из датакласса).
   3. `input` и `output` — аналоги соответствующих методов, принимающие единственный индекс.

Объект [`context`](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.OperationPreparationContext) позволяет получать информацию о входных и выходных потоках: их количество, схемы и пути к таблицам.

смотрите примеры в туториале: [раз](../../../api/python/examples.md#prepare_operation) и [два](#examples.md#grep).

Если запускается MapReduce с несколькими промежуточными потоками, то требуется также переопределить метод [.get_intermediate_stream_count(self)](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.prepare_operation.TypedJob.get_intermediate_stream_count), вернув из него количество промежуточных потоков. Смотрите [пример](../../../api/python/examples.md#map_reduce_multiple_intermediate_streams).

### Декораторы { #python_decorators }

Существует возможность пометить функции или классы джобов специальными декораторами, меняющими ожидаемый интерфейс взаимодействия с джобами.

- [aggregator](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.aggregator) — декоратор, который позволяет отметить, что данный mapper является агрегатором, то есть принимает на вход итератор на строки, а не одну строку.
- [raw](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.raw) — декоратор, который позволяет отметить, что функция принимает на вход поток сырых данных, а не распарсенные записи.
- [raw_io](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.raw_io) — декоратор, который позволяет отметить, что функция будет брать записи (строки) из `stdin` и писать в `stdout`.
- [reduce_aggregator](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.reduce_aggregator) — декоратор, который позволяет отметить, что reducer является агрегатором, то есть принимает на вход итератор не на записи с одним ключом, а генератор из пар, где каждая пара — (ключ, записи с этим ключом).
- [with_context](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.with_context) — декоратор, который позволяет заказать контекст для функции. В данном контексте будут находиться контрольные атрибуты, заказанные при запуске операции.

Обратите внимание, что декораторы реализованы через простановку атрибута на функции. Поэтому, например, объявить функцию с декоратором, а потом сделать поверх неё `functools.partial` не получится. Если вы хотите прямо при вызове передать какие-то параметры в функцию – стоит завести класс с декоратором (смотрите последний пример ниже).

Примеры можно найти в [туториале](../../../api/python/examples.md#job_decorators).


### Pickling функции и окружения { #pickling }
#### Общее устройство { #pickling_description }

При запуске операции, которая является python функцией, происходит следующее:

1. Библиотека, используя модуль [dill](https://github.com/uqfoundation/dill), преобразует запускаемый объект (класс или функцию) в поток байт
2. Собирается локальное окружение, в котором запущена функция, передается внутрь джоба на кластере и там используется для корректного запуска вашей функции.

После этого на кластере запускается специальный код из модуля `_py_runner.py`, который локально в каждом джобе распаковывает все зависимости, преобразует функцию из набора байт в python объект и запускает её правильным образом, читая данные из stdin.

#### Ссылка на пост с советами { #pickling_advises }

{% cut "Примеры фильтрации модулей" %}
Например, как отфильтровать передачу .so и .pyc файлов, сохранив передачу модуля yt (кстати, это же позволит запускать скрипты c Mac OS X):

```python
yt.config["pickling"]["module_filter"] = lambda module: hasattr(module, "__file__") and \
    not module.__file__.endswith(".so")
yt.config["pickling"]["force_using_py_instead_of_pyc"] = True

## При работе через клиента можно задать в его конфиге:
client = yt.wrapper.client.Yt(config={"pickling": {"module_filter": lambda ...}})
```

А можно написать свою функцию сбора зависимостей, взяв за основу [дефолтную реализацию отсюда](http://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.create_modules_archive_default):

```python
def my_create_modules_archive():
    ...

yt.config["pickling"]["create_modules_archive_function"] = my_create_modules_archive
```

Если вы используете бинарные модули, которые динамически линкуются с другими библиотеками, которые на кластере отсутствуют, то стоит привезти эти библиотеки на кластер вручную (и не забыть при этом выставить переменную окружения `LD_LIBRARY_PATH`), либо воспользоваться автоматическим сбором динамических библиотек для Python модулей. При автоматическом сборе на каждую `.so`-шку вызывается `ldd` и найденные зависимости пакуются.
Включается следующим образом:

```python
yt.config["pickling"]["dynamic_libraries"]["enable_auto_collection"] = True
```

Также можно указать фильтр, если какие-то библиотеки добавлять не нужно. Пример:

```python
yt.config["pickling"]["dynamic_libraries"]["library_filter"] = lambda lib: not lib.startswith("/lib")
```

По умолчанию рекомендуется отфильтровывать библиотеки, которые находятся в директориях `/lib`, `/lib64`, так как там находятся разные системные библиотеки (например, `libcgroup`) и их использование на кластере может привести к странным ошибкам. Отфильтровать можно функцией из примера выше.

Рецепты фильтров для разных случаев:

- Если вы получаете ошибку: `AttributeError: 'module' object has no attribute 'openssl_md_meth_names'`, то нужно отфильтровать hashlib:`yt.config["pickling"]["module_filter"] = lambda module: "hashlib" not in getattr(module, "__name__", "") `

- Если у вас Anaconda, то нужно отфильтровать hashlib (смотрите пример фильтра выше), а также .so библиотеки. Для фильтрации .so библиотек можно использовать следующий фильтр:

  ```python
  yt.config["pickling"]["module_filter"] = (
      lambda module: hasattr(module, "__file__") and
      not module.__file__.endswith(".so")
  )
  ```

  {% note warning "Внимание" %}

  Данный фильтр также отфильтровывает YSON [биндинги](#yson). Если используется YSON, то стоит добавить библиотеку в исключения:

  ```python
  yt.config["pickling"]["module_filter"] = (
      lambda module: hasattr(module, "__file__") and
      (not module.__file__.endswith(".so") or module.__file__.endswith("yson_lib.so")
  )
  ```

  {% endnote %}

- Вы запускаетесь свежим python2.7 и получаете ошибку вида: `ImportError: No module named urllib3` или `ImportError: cannot import name _compare_digest`, или не можете поимпортировать модуль `hmac`

    Чтобы решить эту проблему, нужно отфильтровать hmac из модулей, которые вы забираете с собой (он импортирует из модуля operator метод, которого нет в python2.7.3, установленном на кластере).

    ```python
    yt.config["pickling"]["module_filter"] = ... and getattr(module, "__name__", "") != "hmac"
    ```

Также поддерживается автоматическая фильтрация .pyc и .so файлов в случае, если версия питона / операционной системы на кластере и клиенте отличаются. Опцию предварительно необходимо включить в конфиге:

```python
yt.config["pickling"]["enable_modules_compatibility_filter"] = True
```

При запуске python-функции в джобе происходит распаковка и импорт всех модулей, которые есть в зависимостях, в частности вашего main-модуля. В связи с этим, вся бизнес-логика должна быть спрятана под `__main__`, то есть, правильно делать следующим образом:

```python
class Mapper(yt.TypedJob):
    ...

if __name__ == "__main__":
    yt.run_map(mapper, ...)
```

{% endcut %}

### Porto-слои { #porto_layers }

При запуске операции можно указать, какой [образ ФС](../../../user-guide/data-processing/porto/layer-paths.md) необходимо подготовить перед запуском джобов.
Есть некоторый набор [готовых слоёв](../../../user-guide/data-processing/porto/layer-paths.md#gotovye-sloi-v-kiparise), которые находятся по пути `//porto_layers`.

Указать путь до нужного слоя можно через параметр `layer_paths` в спеке джоба, например, так:
```python
spec_builder = ReduceSpecBuilder() \
    .begin_reducer() \
        .command(reducer) \
        .layer_paths(["//porto_layers/ubuntu-precise-base.tar.xz"]) \
    .end_reducer() \
    ...
yt.run_operation(spec_builder)
```

### tmpfs в джобах { #tmpfs_in_jobs }

Поддержка tmpfs в джобах состоит из двух частей:

1. Для python-операций tmpfs включен по умолчанию, он заказывается в специальную директорию tmpfs и туда происходит распаковка архива модулей. Дополнительная память под tmpfs прибавляется к лимиту, который указал пользователь. Поведение регулируется опциями `pickling/enable_tmpfs_archive` и `pickling/add_tmpfs_archive_size_to_memory_limit`.
2. Имеется возможность автоматически заказать tmpfs на все файлы джоба, опция называется `mount_sandbox_in_tmpfs/enable` и по умолчанию выключена. Её включение приводит к тому, что в ваших операциях в спеке будет указано `tmpfs_path="."`, а также `tmpfs_size`, равный суммарному размеру файлов. Также tmpfs_size будет добавлен к `memory_limit`. Обратите внимание, что если вы используете табличные файлы – у системы нет возможности узнать их размер на диске после форматирования, поэтому размер необходимо указать в атрибутах пути с помощью атрибута `disk_size`. Также есть возможность заказать дополнительное место в tmpfs, если ваш джоб порождает в процессе работы какие-то файлики, для этого можно указать нужное количество байт через опцию `mount_sandbox_in_tmpfs/additional_tmpfs_size`.

### Статистики в джобах { #python_jobs_statistics }

В процессе работы джоба пользователь может экспортировать свои собственные статистики (например, подсчитывать время работы отдельных стадий внутри джоба). В библиотеке доступны следующие функции:

- [write_statistics](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.user_statistics.write_statistics) — записывает словарь с собранной статистикой в нужный файловый дескриптор. Функция обязательно должна вызываться только из джоба.

Также у класса `Operation` есть метод [get_job_statistics](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.operation_commands.Operation.get_job_statistics) для быстрого доступа к статистике операции.

Пример:

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


## Нетипизированные Python-операции { #python_operations_untyped }

Помимо типизированных джобов поддерживаются также нетипизированные.
Вместо наследника `TypedJob` в качестве пользовательского джоба выступает python-функция или любой callable-объект. В качестве аргументов функция принимает запись (в случае маппера) и ключ + итератор по записям в случае редьюсера. В последнем случае ключ является `dict`-like объектом, в котором заполнены только ключевые колонки. Сама функция обязана являться генератором, с помощью `yield` порождающим записи, которые будут записаны в выходную таблицу.

Пример фильтрующего маппера:

```python
def mapper(row):
    if row.get("type") == "job_started":
        yield row
```

Пример reducer-а, считающего сумму по колонке value:

```python
def reducer(key, rows):
    row = dict(key.iteritems())
    row["sum"] = sum((row["value"] for row in rows))
    yield row
```

### Декораторы { #python_decorators_untyped }

В нетипизированном API смысл некоторых декораторов немного меняется по сравнению [с типизированным](#python_decorators).

- [reduce_aggregator](https://pydoc.ytsaurus.tech/yt.wrapper.html#yt.wrapper.py_wrapper.reduce_aggregator) — reducer-у будет передана не одна пара (ключ, записи), а итератор пар, где каждая пара — (ключ, записи с этим ключом).

Обратите внимание, что декораторы реализованы через простановку атрибута на функции. Поэтому, например, объявить функцию с декоратором, а потом сделать поверх неё `functools.partial` не получится. Если вы хотите прямо при вызове передать какие-то параметры в функцию – стоит завести класс с декоратором (смотрите последний пример ниже).

Примеры можно найти в [туториале](../../../api/python/examples.md#job_decorators_untyped).


### Форматы { #python_formats }

По умолчанию, указывать формат при работе с python-операциями не нужно.

В {{product-name}} хранятся структурированные данные, никакого предопределенного текстового представления у них нет, все джобы работают в стриминг режиме, и пользователю необходимо явно указать, в каком виде его скрипт ожидает увидеть данные во входном потоке. В противоположность этому, работая с данными в python-функции вы тоже получаете их в структурированном виде. Между структурированным видом данных в {{product-name}} и в python можно построить соответствие, а промежуточный формат, который будет использован при передаче данных в джоб, уже не так важен.

Важно понимать, что идеального взаимооднозначного соответствия между структурированными данными в {{product-name}} и dict-ами в python нет, поэтому существуют разные особенности, про которые будет рассказано далее.

#### Структурированное представление данных { #structured_data }

Существуют следующие важные особенности представления структурированных данных в python:

1. Объекты в табличных записях в {{product-name}} могут иметь [атрибуты](../../../user-guide/storage/attributes.md) (это относится только к колонкам типа any). Для их представления в библиотеке заведены [специальные типы](http://pydoc.ytsaurus.tech/yt.yson.html#module-yt.yson.yson_types), которые унаследованы от стандартных python-типов, но еще имеют поле `attributes`. Так как создание custom объектов является дорогой процедурой, то по умолчанию такой объект создается, только если у него есть атрибуты. Чтобы регулировать данное поведение существует опция: `always_create_attributes`. Сравнение yson типов происходит следующим образом: сначала сравниваются значения базовых типов, а затем сравниваются атрибуты. Если атрибуты не равны (в том числе если у одного объекта есть атрибуты, а у другого нет), то объекты считаются не равными. Это стоит учитывать при сравнении с базовыми типами в python: чтобы не зависеть от наличия атрибутов у объекта стоит явно приводить объект к базовому типу.

   Поясняющий пример:

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

2. В формате YSON имеется два целочисленных типа: [int64 и uint64](../../../user-guide/storage/yson.md) . В то время, как в python с точки зрения модели данных тип один и никаких ограничений не имеет. Поэтому при чтении данных беззнаковый тип представляется как YsonUint64, в то время, как знаковый представляется обычным int-ом. При записи int-ов сделана автоматика: числа в диапазоне [-2^63, 2^63) представляются как знаковые, а числа в диапазоне [2^63, 2^64) как беззнаковые. Но всегда можно явно указать тип, создав явно Yson-объект.

3. Unicode-строки. Так как в {{product-name}} все строки байтовые, то python unicode-строки по умолчанию кодируются в байтовые строки utf-8 кодировкой. При чтении в Python3 происходит попытка раскодировать байтовые строки с помощью utf-8 декодера, если же это не получается, то возвращается специальный объект [YsonStringProxy](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.yson_types.YsonStringProxy). Более подробно смотрите соответствующий [раздел](#python3_strings).

При записи данных, YsonFormat корректно отличает элементарные python-типы от yson-типов.

#### Контрольные атрибуты { #control_attributes }

Кроме потока записей при чтении из таблицы (или в джобе) можно заказать разные [контрольные атрибуты](../../../user-guide/storage/io-configuration.md#control_attributes) . Работа с контрольными атрибутами зависит от формата, например в форматах, отличных от Yamr, JSON и YSON, большинство контрольных атрибутов не представимы.

В формате Yamr в python API корректно поддержан парсинг атрибутов row_index и table_index, которые будут представлены как поля tableIndex и recordIndex и объектов типа Record.

В формате JSON никакой специальной обработки контрольных атрибутов не производится, то есть при их заказе необходимо будет самостоятельно обрабатывать контрольные записи в потоке.

В формате YSON имеется несколько режимов для автоматической обработки контрольных атрибутов. Выбор режима контролируется опцией `control_attributes_mode`, которая может принимать следующие значения **(важно: по историческим причинам, чтобы опция правильно работала —  необходимо указать также process_table_index=None при создании формата, по умолчанию process_table_index равен True, что форсирует control_attributes_mode=row_fields)**:

- `iterator`(default) — при парсинге потока формат выдаст итератор по записям, чтобы получить в джобе контрольные атрибуты будет необходимо воспользоваться контекстом.
- `row_fields` — заказанные контрольные атрибуты будут записаны в качестве полей у каждой записи. Например, если заказан row_index, то у каждой записи будет поле `@row_index`, в котором записан номер данной записи.
- `none` — никакой специальной обработки контрольных атрибутов производиться не будет, то есть клиенту придет поток записей, в котором будут встречаться записи типа entity с контрольными атрибутами.

Примеры переключения между выходными таблицами с `table_index` при `control_attributes_mode`, равном `iterator` и `row_fields`, а также пример получения индекса текущей строки таблицы в reducer-е с помощью `context`, можно найти [здесь](../../../api/python/examples.md#table_switches_untyped)

#### Другие форматы { #other_formats }

По умолчанию библиотека проводит сериализацию через [YSON формат](../../../user-guide/storage/formats.md#yson), так как именно он обеспечивает корректное и однозначное представление любых данных, хранящихся в таблицах {{product-name}}. Для эффективной работы с YSON-форматом в python необходима [нативная библиотека](#yson_bindings). В случае отсутствия данной библиотеки на кластере вы получите ошибку при выполнении джоба (fallback на python-библиотеку для YSON не делается из-за того, что она очень медленная и неэффективная). В такой ситуации вы можете переключиться на использование [JSON-формата](../../../user-guide/storage/formats.md#json).

Большинство других форматов являются текстовыми (то есть числа в них никак не отличаются от строк) и таким образом вы потеряете типизацию.

В случае использования форматов отличных от YSON и Yamr в качестве рекордов всегда будет приходить `dict`. Например, никаких преобразований из JSON-представления записей в YSON-представление автоматически происходить не будет.




## Другое { #other }

### gRPC { #grpc }

Поддержки RPC proxy с использованием gRPC транспорта нет. Если вы не можете поставить бинарный пакет с RPC-биндингами, вы можете самостоятельно пользоваться gRPC-транспортом.

<!-- Подробное описание и пример есть на [отдельной странице](../../description/proxy/grpc.md). -->

### YSON { #yson }

Вместе с библиотекой для работы с кластерами {{product-name}} также поставляется библиотека для работы с [форматом YSON](../../../user-guide/storage/yson.md). Библиотека доступна в модуле `yt.yson` и реализует стандартные функции `load`, `loads`, `dump` и `dumps`. Кроме того, она предоставляет доступ к [Yson-типам](#structured_data). Также библиотека реализует следующие общеполезные функции:

- [to_yson_type](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.convert.to_yson_type) — создание yson-типа из python-объекта.
- [json_to_yson](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.convert.json_to_yson) — рекурсивно преобразует python-объект из JSON-представления (читайте про особенности представления структурированных данных в {{product-name}} в [формате JSON](../../../user-guide/storage/formats.md#json) ) в YSON-представлении.
- [yson_to_json](https://pydoc.ytsaurus.tech/yt.yson.html#yt.yson.convert.yson_to_json) — рекурсивно преобразует python-объект из YSON-представления в JSON-представление.

#### YSON-биндинги { #yson_bindings }

Библиотека yson имеет две реализации: на чистом python и в виде C++-биндингов.
Нативный parser и writer YSON-а, написанные на python, очень медленные и подходят только для работы с небольшими объемами данных.
**Важно**: в частности, запустить операции или читать таблицы в формате YSON не получится

C++-биндинги поставляются в виде debian- и pip-пакетов.

Пакеты собираются в виде универсальной .so библиотеки, в которую вкомпилена libcxx, поэтому должны работать в любой debian-системе.
