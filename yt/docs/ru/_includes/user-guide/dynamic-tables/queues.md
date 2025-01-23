В данном разделе описана экосистема очередей {{product-name}}, специальные методы работы с ними, способы конфигурации и использования.

## Модель данных { #data_model }

*Очередью (queue)* в {{product-name}} называется любая упорядоченная динамическая таблица. Партиция очереди — таблет динамической таблицы, её индекс соответствует индексу таблета.

*Консьюмером (queue_consumer)* в {{product-name}} называется сортированная таблица с некоторой фиксированной схемой. Консьюмер находится в соотношении many-to-many с очередями и представляет из себя потребителя одной или нескольких очередей. Задача консьюмера — хранить оффсеты по партициям читаемых очередей.

Связь между консьюмерами и очередями обеспечивается объектами *регистраций*. {% if audience == "internal" %} Регистрации хранятся надёжно и кросс-кластерно, поэтому допускается регистрация консьюмера к очереди с другого кластера {{product-name}}. {% endif %}


<small>Таблица 1 — Схема таблицы консьюмера</small>

| Имя                  | Тип    | Описание                                                                                |
|----------------------|--------|-----------------------------------------------------------------------------------------|
| queue_cluster        | string | Имя кластера очереди                                                                   |
| queue_path           | string | Путь к динамической таблице-очереди                                                    |
| partition_index      | uint64 | Номер партиции, он же `tablet_index`                                                   |
| offset               | uint64 | Номер **первой необработанной** консьмером строки указанной партиции указанной очереди |
| meta                 | any    | Системная мета-информация                                                              |

*Продьюсером (queue_producer)* в {{product-name}} так же называется сортированная таблица с некоторой фиксированной схемой. Продьюсер хранит порядковые номера для последних записанных строчек в рамках сессий записи в очереди, что позволяет не писать дублирующиеся строчки в очередь.

<small>Таблица 2 — Схема таблицы продьюсера</small>

| Имя                  | Тип    | Описание                                                                                |
|----------------------|--------|-----------------------------------------------------------------------------------------|
| queue_cluster        | string | Имя кластера очереди                                                                   |
| queue_path           | string | Путь к динамической таблице-очереди                                                    |
| session_id           | string | Идентификатор сессии                                                                   |
| sequence_number      | int64  | Порядковый номер последней записанной строчки                                          |
| epoch                | int64  | Номер текущей эпохи                                                                    |
| user_meta            | any    | Пользовательская мета-информация                                                       |
| system_meta          | any    | Системная мета-информация                                                              |


## API { #api }

{% if audience == "internal" %}
{% note info "Примечание" %}

Если вы пользуетесь [BigRT](https://{{doc-domain}}.{{internal-domain}}/big_rt/), то стоит ознакомиться с [описанием](https://{{doc-domain}}.{{internal-domain}}/big_rt/work_details/qyt#native-qyt) нативных QYT-очередей и [инструкцией](https://{{doc-domain}}.{{internal-domain}}/big_rt/configuration/yt_sync) по использованию нативных {{product-name}} консьюмеров в yt_sync, которые поддерживаются командой [BigB](https://abc.{{internal-domain}}/services/bigb/).

Если вы планируете конфигурировать множество очередей и консьюмеров в своих процессах, рекомендуется также посмотреть в сторону yt_sync.

{% endnote %}
{% endif %}

### Создание очереди

Создание очереди ничем не отличается от создания обычной упорядоченной динамической таблицы.

Для наибольшей полноты графиков/статистик рекомендуется добавлять в схему таблицы колонки [`$timestamp`](ordered-dynamic-tables.md#timestamp_column) и [`$cumulative_data_weight`](ordered-dynamic-tables.md#cumulative_data_weight_column).

Проверить, что автоматика подцепила очередь, можно по появлению вкладки **Queue** на странице объекта динамической таблицы.

### Создание консьюмера

Создать консьюмер можно через алиас `create queue_consumer`, или явно создав сортированную динамическую таблицу со схемой, [указанной выше](#data_model), выставив на неё атрибут `@treat_as_queue_consumer = %true`.

Проверить, что автоматика подцепила консьюмер, можно по появлению вкладки **Consumers** на странице объекта динамической таблицы.

### Регистрация консьюмера к очереди

Регистрация консьюмера к очереди производится с помощью метода `register_queue_consumer`. Обязательно указывать булевый параметр `vital`, который играет роль для настроек автоматического тримминга очередей (см. [секцию](#automatic_trimming) про автоматический тримминг).

Для того чтобы выполнять команду выше, нужно иметь право `register_queue_consumer` с указанием `vital=True/False` на директорию, содержащую очередь. Запросить это право можно аналогично остальным правам в UI {{product-name}} на странице директории. Право `register_queue_consumer` с `vital=True` даёт возможность регистрировать как vital-консьюмеры, так и non-vital.

Удаление регистрации производится с помощью метода `unregister_queue_consumer`. Для выполнения команды нужно иметь write-доступ к очереди или консьюмеру.

Оба Cypress аргумента обеих команд имеют тип [rich YPath](../../../user-guide/storage/ypath.md#rich_ypath) и поддерживают указание имени кластера. В ином случае используется кластер, на котором выполняется команда.

### Чтение данных

Для чтения данных из очередей доступно два схожих метода.

Метод `pull_queue` позволяет вычитать порцию строк указанной партиции указанной очереди, ограничив её по числу строк (`max_row_count`) или по объёму данных в байтах (`max_data_weight`). Для выполнения этого запроса нужно иметь право на чтение очереди.

Метод `pull_queue_consumer` аналогичен предыдущему, но принимает первым аргументом путь до таблицы консьюмера. Для выполнения этого запроса должны быть выполнено два свойства: у пользователя есть право на чтение консьюмера **и** присутствует регистрация указанного консьюмера к указанной очереди.
Параметр очереди в данном методе представляет из себя [rich YPath](../../../user-guide/storage/ypath.md#rich_ypath) и поддерживает указание кластера, на котором она расположена. В ином случае используется кластер, на котором выполняется команда.

Чтение данных из очереди также можно делать обычным способом, через метод `select_rows`.

{% note warning "Внимание" %}

Методы `pull_queue` и `pull_queue_consumer` могут вернуть меньше строк, чем указано в `max_row_count`, даже если столько строк есть в партиции очереди и их объем меньше `max_data_weight`. Лишь возврат пустого множества строк означает, что в партиции нет строк по указанному оффсету.

{% endnote %}

### Работа с консьюмером

Для работы с консьюмером доступен метод `advance_queue_consumer`, продвигающий оффсет консьюмера по указанной партиции указанной очереди. В переданной транзакции делается изменение соответствующей строки консьюмера с оффсетом. В той же транзакции могут быть произведены иные действия в рамках динамических таблиц.

При указании non-null параметра `old_offset`, в той же транзакции сначала производится чтение текущего оффсета и его сравнение с переданным значением — при их неравенстве выбрасывается исключение.

Оффсеты очередей {{product-name}} интерпретируются как индекс *первой непрочитанной строки*.

### Создание продьюсера

Создать продьюсер можно через алиас `create queue_producer`, или явно создав сортированную динамическую таблицу со схемой, [указанной выше](#data_model), выставив на неё атрибут `@treat_as_queue_producer = %true`.

### Запись данных

Для записи данных в очередь можно либо использовать API динамических таблиц, а именно метод `insert_rows`, либо воспользоваться API продьюсеров для записи без дубликатов.

Чтобы писать через продьюсер, нужно иметь право `write` как для очереди, так и для продьюсера.

Перед началом записи нужно позвать метод `create_queue_producer_session`, который принимает путь до очереди, путь до продьюсера, а также идентификатор сессии записи (`session_id`). В качестве `session_id` можно передать произвольную строку, например, имя хоста, откуда производится запись. В результате, если такой сессии ранее не было, то в таблице-продьюсере создастся сессия с `epoch` равным `0` и `sequence_number` равным `-1`. Если же сессия с таким идентификатором уже ранее создавалась, то для нее будет увеличено значение `epoch`.

Метод `create_queue_producer_session` вернет текущее (обновленное) значение эпохи, а также порядковый номер последнего записанного сообщение, то есть актуально состояние сессии записи, хранящееся в продьюсере.

Затем, с помощью метода `push_queue_producer` можно писать данные в очередь. Метод принимать путь до очереди, путь до продьюсера, идентификатор сессии, эпоху, а также сами записываемые данные. В каждой строчке необходимо передать значение `$sequence_number`, соответствующее порядковому номеру этой строчки. Либо, можно не передавать порядковый номер в каждой строчке в самих данных, а указать лишь порядковый номер, соответствующий первой строчке, в опциях метода - в таком случае мы будем считать, что для остальных строк он инкрементально увеличивается на единицу.

Эпоха (`epoch`) сессии может использоваться для того, чтобы бороться с зомби-процессами.

## Queue Agent

Queue Agent — выделенный микросервис, следящий за очередями, консьюмерами и регистрациями.

### Автоматические политики очистки очередей { #automatic_trimming }

Для очереди можно настроить автоматическую политику очистки (тримминга), выставив на ней атрибут `@auto_trim_config` с конфигурацией [соответствующего формата]({{source-root}}/yt/yt/client/queue_client/config.h?rev=11720161#L41).

Доступные опции:
  - `enable: True` — включает тримминг по vital консьюмерам. Если есть хотя бы один vital-консьюмер, то Queue Agent будет с периодичностью в единицы секунд звать Trim по партиции вплоть до минимума по оффсетам vital-консьюмеров.<br> **NB:** Если vital-консьюмеров нет, то тримминг не производится.
  - `retained_rows: x` — гарантирует, что в каждой партиции безусловно будут держаться последние x строк. Предназначен для использования в совокупности с предыдущей опцией.
  - `retained_lifetime_duration: x` — гарантирует, что в каждой партиции безусловно будут держаться строки, которые были записаны в очередь не более x миллисекунд назад, при этом указанное количество миллисекунд должно быть кратно одной секунде. Предназначен для использования в совокупности с включённой опцией тримминга (`enable: True`).

Такая настройка не конфликтует с [существующими](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md#remove_old_data) настройками TTL динамических таблиц: можно настроить тримминг по vital consumers и `max_data_ttl=36h, min_data_versions=0`, чтобы, помимо удаления данных по оффсетам, всегда хранить не более трёх дней данных.

### Графики и статусы

{% if audience == "internal" %}
С помощью Queue Agent для очередей и консьюмеров экспортируются графики в Solomon, которые можно увидеть на страницах таблиц очередей и консьюмеров в UI.

Также доступен общий дашборд по паре (консьюмер, очередь): [пример]({{nda-link}}/ymXONQWw6Tscmm).
{% endif %}

С помощью атрибутов `@queue_status`, `@queue_partitions` на таблицах очередей и `@queue_consumer_status`, `@queue_consumer_partitions` на таблицах консьюмеров можно узнать текущее состояние и мета-информацию очередей/консьюмеров с точки зрения Queue Agent, как в целом, так и по отдельным партициям.

Эти атрибуты не предназначены для высоконагруженных сервисов и их стоит использовать исключительно для интроспекции.

## Пример использования

{% if audience == "internal" %}
Представленные примеры показаны для кластеров [Hume](https://{{yt-domain}}.{{internal-domain}}/hume) и [Pythia](https://{{yt-domain}}.{{internal-domain}}/pythia).
{% else %}
Представленные примеры используют гипотетическую конфигурацию из нескольких кластеров, с адресам `hume` и `pythia`.
{% endif %}

<small>Листинг 1 — Пример использования API очередей</small>

```bash
# Create queue on pythia.
$ yt --proxy pythia create table //tmp/$USER-test-queue --attributes '{dynamic=true;schema=[{name=data;type=string};{name="$timestamp";type=uint64};{name="$cumulative_data_weight";type=int64}]}'
2826e-2b1e4-3f30191-dcd2013e

# Create queue_consumer on hume.
$ yt --proxy hume create queue_consumer //tmp/$USER-test-consumer

# OR: Create queue_consumer on hume as table with explicit schema specification.
$ yt --proxy hume create table //tmp/$USER-test-consumer --attributes '{dynamic=true;treat_as_queue_consumer=true;schema=[{name=queue_cluster;type=string;sort_order=ascending;required=true};{name=queue_path;type=string;sort_order=ascending;required=true};{name=partition_index;type=uint64;sort_order=ascending;required=true};{name=offset;type=uint64;required=true};{name=meta;type=any;required=false}]}'
18a5b-28931-3ff0191-35282540

# Register consumer for queue.
$ yt --proxy pythia register-queue-consumer //tmp/$USER-test-queue "<cluster=hume>//tmp/$USER-test-consumer" --vital

# Check registrations for queue.
$ yt --proxy pythia list-queue-consumer-registrations --queue-path //tmp/$USER-test-queue
[
  {
    "queue_path" = <
      "cluster" = "pythia";
    > "//tmp/bob-test-queue";
    "consumer_path" = <
      "cluster" = "hume";
    > "//tmp/bob-test-consumer";
    "vital" = %true;
    "partitions" = #;
  };
]

# Check queue status provided by Queue Agent.
$ yt --proxy pythia get //tmp/$USER-test-queue/@queue_status
{
    "partition_count" = 1;
    "has_cumulative_data_weight_column" = %true;
    "family" = "ordered_dynamic_table";
    "exports" = {
        "progress" = {};
    };
    "alerts" = {};
    "queue_agent_host" = "yt-queue-agent-1.ytsaurus.tech";
    "has_timestamp_column" = %true;
    "write_row_count_rate" = {
        "1m_raw" = 0.;
        "1h" = 0.;
        "current" = 0.;
        "1d" = 0.;
        "1m" = 0.;
    };
    "registrations" = [
        {
            "consumer" = "hume://tmp/bob-test-consumer";
            "vital" = %true;
            "queue" = "pythia://tmp/bob-test-queue";
        };
    ];
    "write_data_weight_rate" = {
        "1m_raw" = 0.;
        "1h" = 0.;
        "current" = 0.;
        "1d" = 0.;
        "1m" = 0.;
    };
}

$ yt --proxy pythia get //tmp/$USER-test-queue/@queue_partitions
[
    {
        "error" = {
            "attributes" = {
                "trace_id" = "21503942-85d82a3a-2c9ea16d-e2149d9c";
                "span_id" = 17862985281506116291u;
                "thread" = "Controller:4";
                "datetime" = "2025-01-23T13:42:21.839124Z";
                "tid" = 9166196934387883291u;
                "pid" = 481;
                "host" = "yt-queue-agent-1.ytsaurus.tech";
                "state" = "unmounted";
                "fid" = 18445202819181375616u;
            };
            "code" = 1;
            "message" = "Tablet 3d3c-50e7d-7db02be-7e178361 is not mounted or frozen";
        };
    };
]

# Check queue consumer status provided by Queue Agent.
$ yt --proxy hume get //tmp/$USER-test-consumer/@queue_consumer_status
{
    "queues" = {
        "pythia://tmp/bob-test-queue" = {
            "error" = {
                "attributes" = {
                    "trace_id" = "623ba99c-b2dce5fe-50174949-5f508824";
                    "span_id" = 14498308957160432715u;
                    "thread" = "Controller:1";
                    "datetime" = "2025-01-23T13:42:55.747430Z";
                    "tid" = 627435960759374310u;
                    "pid" = 481;
                    "host" = "yt-queue-agent-1.ytsaurus.tech";
                    "fid" = 18442320640360156096u;
                };
                "code" = 1;
                "message" = "Queue \"pythia://tmp/bob-test-queue\" snapshot is missing";
            };
        };
    };
    "registrations" = [
        {
            "consumer" = "hume://tmp/bob-test-consumer";
            "vital" = %true;
            "queue" = "pythia://tmp/bob-test-queue";
        };
    ];
    "queue_agent_host" = "yt-queue-agent-1.ytsaurus.tech";
}

# We can see some errors in the responses above, since both tables are unmounted.
# Mount queue and consumer tables.
$ yt --proxy pythia mount-table //tmp/$USER-test-queue
$ yt --proxy hume mount-table //tmp/$USER-test-consumer

# Check statuses again:
$ yt --proxy pythia get //tmp/$USER-test-queue/@queue_partitions
[
    {
        "meta" = {
            "cell_id" = "2dd9a-d4f7-3f302bc-f21fc0c";
            "host" = "node-1.ytsaurus.tech:9022";
        };
        "lower_row_index" = 0;
        "cumulative_data_weight" = #;
        "upper_row_index" = 0;
        "available_row_count" = 0;
        "write_row_count_rate" = {
            "1m_raw" = 0.;
            "1h" = 0.;
            "current" = 0.;
            "1d" = 0.;
            "1m" = 0.;
        };
        "available_data_weight" = #;
        "trimmed_data_weight" = #;
        "last_row_commit_time" = "1970-01-01T00:00:00.000000Z";
        "write_data_weight_rate" = {
            "1m_raw" = 0.;
            "1h" = 0.;
            "current" = 0.;
            "1d" = 0.;
            "1m" = 0.;
        };
        "commit_idle_time" = 1737639851870;
    };
]

$ yt --proxy hume get //tmp/$USER-test-consumer/@queue_consumer_status
{
    "queues" = {
        "pythia://tmp/bob-test-queue" = {
            "read_data_weight_rate" = {
                "1m_raw" = 0.;
                "1h" = 0.;
                "current" = 0.;
                "1d" = 0.;
                "1m" = 0.;
            };
            "read_row_count_rate" = {
                "1m_raw" = 0.;
                "1h" = 0.;
                "current" = 0.;
                "1d" = 0.;
                "1m" = 0.;
            };
            "partition_count" = 1;
        };
    };
    "registrations" = [
        {
            "consumer" = "hume://tmp/bob-test-consumer";
            "vital" = %true;
            "queue" = "pythia://tmp/bob-test-queue";
        };
    ];
}

# Enable automatic trimming based on vital consumers for queue.
$ yt --proxy pythia set //tmp/$USER-test-queue/@auto_trim_config '{enable=true}'

# Write rows without exactly once semantics.
$ for i in {1..20}; do echo '{data=foo}; {data=bar}; {data=foobar}; {data=megafoo}; {data=megabar}' | yt insert-rows --proxy pythia //tmp/$USER-test-queue --format yson; done;

# Check that queue status reflects writes.
$ yt --proxy pythia get //tmp/$USER-test-queue/@queue_status/write_row_count_rate
{
    "1m_raw" = 2.6419539762457456;
    "current" = 5.995956327053036;
    "1h" = 2.6419539762457456;
    "1d" = 2.6419539762457456;
    "1m" = 2.6419539762457456;
}

# Read data via consumer.
$ yt --proxy hume pull-queue-consumer //tmp/$USER-test-consumer "<cluster=pythia>//tmp/$USER-test-queue" --partition-index 0 --offset 0 --max-row-count 5 --format "<format=text>yson"
{"$tablet_index"=0;"$row_index"=0;"data"="foo";"$timestamp"=1865777451725072279u;"$cumulative_data_weight"=20;};
{"$tablet_index"=0;"$row_index"=1;"data"="bar";"$timestamp"=1865777451725072279u;"$cumulative_data_weight"=40;};
{"$tablet_index"=0;"$row_index"=2;"data"="foobar";"$timestamp"=1865777451725072279u;"$cumulative_data_weight"=63;};
{"$tablet_index"=0;"$row_index"=3;"data"="megafoo";"$timestamp"=1865777451725072279u;"$cumulative_data_weight"=87;};
{"$tablet_index"=0;"$row_index"=4;"data"="megabar";"$timestamp"=1865777451725072279u;"$cumulative_data_weight"=111;};

# Advance queue consumer.
$ yt --proxy hume advance-queue-consumer //tmp/$USER-test-consumer "<cluster=pythia>//tmp/$USER-test-queue" --partition-index 0 --old-offset 0 --new-offset 42

# Since trimming is enabled and the consumer is the only vital consumer for the queue, soon the rows up to index 42 will be trimmed.
# Calling pull-queue-consumer now returns the next available rows.
$ yt --proxy hume pull-queue-consumer //tmp/$USER-test-consumer "<cluster=pythia>//tmp/$USER-test-queue"  --partition-index 0 --offset 0 --max-row-count 5 --format "<format=text>yson"
{"$tablet_index"=0;"$row_index"=42;"data"="foobar";"$timestamp"=1865777485011069884u;"$cumulative_data_weight"=951;};
{"$tablet_index"=0;"$row_index"=43;"data"="megafoo";"$timestamp"=1865777485011069884u;"$cumulative_data_weight"=975;};
{"$tablet_index"=0;"$row_index"=44;"data"="megabar";"$timestamp"=1865777485011069884u;"$cumulative_data_weight"=999;};
{"$tablet_index"=0;"$row_index"=45;"data"="foo";"$timestamp"=1865777486084785133u;"$cumulative_data_weight"=1019;};
{"$tablet_index"=0;"$row_index"=46;"data"="bar";"$timestamp"=1865777486084785133u;"$cumulative_data_weight"=1039;};

# Create queue producer on pythia.
$ yt --proxy pythia create queue_producer //tmp/$USER-test-producer
309db-eb36-3f30191-f83f27c0

# Create queue producer session.
$ yt --proxy pythia create-queue-producer-session --queue-path //tmp/$USER-test-queue --producer-path //tmp/$USER-test-producer --session-id session_123
{
  "epoch" = 0;
  "sequence_number" = -1;
  "user_meta" = #;
}

# Write rows via queue producer.
$ echo '{data=value1;"$sequence_number"=1};{data=value2;"$sequence_number"=2}' | yt --proxy pythia push-queue-producer //tmp/$USER-test-producer //tmp/$USER-test-queue --session-id session_123 --epoch 0 --input-format yson
{
  "last_sequence_number" = 2;
  "skipped_row_count" = 0;
}

# Check written rows.
$ yt --proxy pythia pull-queue //tmp/$USER-test-queue --offset 100 --partition-index 0 --format "<format=pretty>yson"
{
    "$tablet_index" = 0;
    "$row_index" = 100;
    "data" = "value1";
    "$timestamp" = 1865777698685609732u;
    "$cumulative_data_weight" = 2243;
};
{
    "$tablet_index" = 0;
    "$row_index" = 101;
    "data" = "value2";
    "$timestamp" = 1865777698685609732u;
    "$cumulative_data_weight" = 2266;
};

# Write one more row batch with row duplicates.
$ echo '{data=value2;"$sequence_number"=2};{data=value3;"$sequence_number"=10}' | yt --proxy pythia push-queue-producer //tmp/$USER-test-producer //tmp/$USER-test-queue --session-id session_123 --epoch 0 --input-format yson
{
  "last_sequence_number" = 10;
  "skipped_row_count" = 1;
}

# Check that there is no row dublicates.
$ yt --proxy pythia pull-queue //tmp/$USER-test-queue --offset 100 --partition-index 0 --format "<format=pretty>yson"
{
    "$tablet_index" = 0;
    "$row_index" = 100;
    "data" = "value1";
    "$timestamp" = 1865777698685609732u;
    "$cumulative_data_weight" = 2243;
};
{
    "$tablet_index" = 0;
    "$row_index" = 101;
    "data" = "value2";
    "$timestamp" = 1865777698685609732u;
    "$cumulative_data_weight" = 2266;
};
{
    "$tablet_index" = 0;
    "$row_index" = 102;
    "data" = "value3";
    "$timestamp" = 1865777742709000317u;
    "$cumulative_data_weight" = 2289;
};
```

{% if audience == "internal" %}
Результат операций на графиках можно видеть на вкладке очереди и консьюмера, а также на общем [дашборде](https://{{monitoring-domain}}.{{internal-domain}}/projects/yt/dashboards/monuob4oi7lf8uddjs76?p%5Bconsumer_cluster%5D=hume&p%5Bconsumer_path%5D=%2F%2Ftmp%2Ftest_consumer&p%5Bqueue_cluster%5D=pythia&p%5Bqueue_path%5D=%2F%2Ftmp%2Ftest_queue&from=1687429383727&to=1687444244000&forceRefresh=1687444307283).

## Ограничения и проблемы

  - Асинхронное вычисление лагов консьюмеров может приводить к [небольшим отрицательным значениям в реактивных процессингах](https://st.{{internal-domain}}/YT-19361).
{% endif %}


