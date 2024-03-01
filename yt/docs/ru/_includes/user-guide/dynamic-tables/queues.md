В данном разделе описана экосистема очередей {{product-name}}, специальные методы работы с ними, способы конфигурации и использования.

## Модель данных { #data_model }

*Очередью (queue)* в {{product-name}} называется любая упорядоченная динамическая таблица. Партиция очереди — таблет динамической таблицы, её индекс соответствует индексу таблета.

*Консьюмером (consumer)* в {{product-name}} называется сортированная таблица с [некоторой фиксированной схемой]({{source-root}}/yt/python/yt/environment/init_queue_agent_state.py?rev=r11651845#L46).

Консьюмер находится в соотношении many-to-many с очередями и представляет из себя потребителя одной или нескольких очередей. Задача консьюмера — хранить оффсеты по партициям читаемых очередей.

<small>Таблица 1 — Схема таблицы консьюмера</small>

| Имя                  | Тип    | Описание                                                                                |
|----------------------|--------|-----------------------------------------------------------------------------------------|
| queue_cluster        | string | Имя кластера очереди                                                                   |
| queue_path           | string | Путь к динамической таблице-очереди                                                    |
| partition_index      | uint64 | Номер партиции, он же `tablet_index`                                                   |
| offset               | uint64 | Номер **первой необработанной** консьмером строки указанной партиции указанной очереди |

Связь между консьюмерами и очередями обеспечивается объектами *регистраций*. Регистрации хранятся надёжно и кросс-кластерно, поэтому допускается регистрация консьюмера к очереди с другого кластера {{product-name}}.

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

Проверить, что автоматика подцепила очередь, можно по появлению вкладки Queue на странице объекта динамической таблицы (*пока только в бете*).

### Создание консьюмера

На текущий момент для создания консьюмера нужно создать сортированную динамическую таблицу с схемой, [указанной выше](#data_model), выставив на неё атрибут `@treat_as_queue_consumer = %true`.

В ближайшем будущем консьюмера можно будет создавать через алиас `create consumer`.

Проверить, что автоматика подцепила консьюмер, можно по появлению вкладки **Consumers** на странице объекта динамической таблицы (*пока только в бете*).

### Регистрация консьюмера к очереди

Регистрация консьюмера к очереди производится с помощью метода `register_queue_consumer`. Обязательно указывать булевый параметр `vital`, который играет роль для настроек автоматического тримминга очередей (см. [секцию](#automatic_trimming) про автоматический тримминг).

Для того чтобы выполнять команду выше, нужно иметь право `register_queue_consumer` с указанием `vital=True/False` на директорию, содержащую очередь. Запросить это право можно аналогично остальным правам в UI {{product-name}} на странице директории. Право `register_queue_consumer` с `vital=True` даёт возможность регистрировать как vital-консьюмеры, так и non-vital.

Удаление регистрации производится с помощью метода `unregister_queue_consumer`. Для выполнения команды нужно иметь write-доступ к очереди или консьюмеру.

Оба Cypress аргумента обеих команд имеют тип [rich YPath](../../../user-guide/storage/ypath.md#rich_ypath) и поддерживают указание имени кластера. В ином случае используется кластер, на котором выполняется команда.

### Чтение данных

Для чтения данных из очередей доступно два схожих метода.

Метод `pull_queue` позволяет вычитать порцию строк указанной партиции указанной очереди, ограничив её по числу строк (`max_row_count`) или по объёму данных в байтах (`max_data_weight`). Для выполнения этого запроса нужно иметь право на чтение очереди.

Метод `pull_consumer` аналогичен предыдущему, но принимает первым аргументом путь до таблицы консьюмера. Для выполнения этого запроса должны быть выполнено два свойства: у пользователя есть право на чтение консьюмера **и** присутствует регистрация указанного консьюмера к указанной очереди.
Параметр очереди в данном методе представляет из себя [rich YPath](../../../user-guide/storage/ypath.md#rich_ypath) и поддерживает указание кластера, на котором она расположена. В ином случае используется кластер, на котором выполняется команда.

Чтение данных из очереди также можно делать обычным способом, через метод `select_rows`.

{% note warning "Внимание" %}

Методы `pull_queue` и `pull_consumer` могут вернуть меньше строк, чем указано в `max_row_count`, даже если столько строк есть в партиции очереди и их объем меньше `max_data_weight`. Лишь возврат пустого множества строк означает, что в партиции нет строк по указанному оффсету.

{% endnote %}


### Работа с консьюмером

Для работы с консьюмером доступен метод `advance_consumer`, продвигающий оффсет консьюмера по указанной партиции указанной очереди. В переданной транзакции делается изменение соответствующей строки консьюмера с оффсетом. В той же транзакции могут быть произведены иные действия в рамках динамических таблиц.

При указании non-null параметра `old_offset`, в той же транзакции сначала производится чтение текущего оффсета и его сравнение с переданным значением — при их неравенстве выбрасывается исключение.

Оффсеты очередей {{product-name}} интерпретируются как индекс *первой непрочитанной строки*.

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
Эти атрибуты не предназначены для реактивных сервисов и их стоит использовать исключительно для интроспекции.

## Пример использования


Так как некоторые команды и исправления появились сравнительно недавно, для выполнения команд ниже пока рекомендуется использовать утилиту `yt`, собранную из свежего исходного кода.
{% if audience == "internal" %}
Представленные примеры показаны для кластеров [Hume](https://{{yt-domain}}.{{internal-domain}}/hume) и [Pythia](https://{{yt-domain}}.{{internal-domain}}/pythia).
{% else %}
Представленные примеры используют гипотетическую конфигурацию из нескольких кластеров, с названиями `hume` и `pythia`.
{% endif %}

<small>Листинг 1 — Пример использования API очередей</small>

```bash
# Create queue on pythia.
$ yt --proxy pythia create table //tmp/test_queue --attributes '{dynamic=true;schema=[{name=data;type=string};{name="$timestamp";type=uint64};{name="$cumulative_data_weight";type=int64}]}'
2826e-2b1e4-3f30191-dcd2013e

# Create consumer on hume.
yt --proxy hume create table //tmp/test_consumer --attributes '{dynamic=true;treat_as_queue_consumer=true;schema=[{name=queue_cluster;type=string;sort_order=ascending;required=true};{name=queue_path;type=string;sort_order=ascending;required=true};{name=partition_index;type=uint64;sort_order=ascending;required=true};{name=offset;type=uint64;required=true}]}'
18a5b-28931-3ff0191-35282540

# Register consumer for queue.
$ yt --proxy pythia register-queue-consumer //tmp/test_queue "<cluster=hume>//tmp/test_consumer" --vital true

# Check registrations for queue.
$ yt --proxy pythia list-queue-consumer-registrations --queue-path //tmp/test_queue
[
  {
    "queue_path" = <
      "cluster" = "pythia";
    > "//tmp/test_queue";
    "consumer_path" = <
      "cluster" = "hume";
    > "//tmp/test_consumer";
    "vital" = %true;
    "partitions" = #;
  };
]

# Check queue status provided by Queue Agent.
$ yt --proxy pythia get //tmp/test_queue/@queue_status
{
    "write_data_weight_rate" = {
        "1m_raw" = 0.;
        "1h" = 0.;
        "current" = 0.;
        "1d" = 0.;
        "1m" = 0.;
    };
    "write_row_count_rate" = {
        "1m_raw" = 0.;
        "1h" = 0.;
        "current" = 0.;
        "1d" = 0.;
        "1m" = 0.;
    };
    "registrations" = [
        {
            "consumer" = "hume://tmp/test_consumer";
            "vital" = %true;
            "queue" = "pythia://tmp/test_queue";
        };
    ];
    "family" = "ordered_dynamic_table";
    "has_cumulative_data_weight_column" = %true;
    "has_timestamp_column" = %true;
    "partition_count" = 1;
}

$ yt --proxy pythia get //tmp/test_queue/@queue_partitions
[
    {
        "error" = {
            "attributes" = {
                "trace_id" = "46143a68-3de2e728-eae939cd-ad813095";
                "span_id" = 12293679318704208190u;
                "datetime" = "2023-06-22T10:11:02.813875Z";
                "tid" = 15645118512615789701u;
                "pid" = 1484;
                "host" = "yt-queue-agent-testing-1.vla.yp-c.yandex.net";
                "state" = "unmounted";
                "fid" = 18443819153702847241u;
            };
            "code" = 1;
            "message" = "Tablet 2826e-2b1e4-3f302be-1d95616f is not mounted";
        };
    };
]

# Check consumer status provided by Queue Agent.
$ yt --proxy hume get //tmp/test_consumer/@queue_consumer_status
{
    "queues" = {
        "pythia://tmp/test_queue" = {
            "error" = {
                "attributes" = {
                    "trace_id" = "ae5fd7bf-d5ab2d1b-10b3f8fe-baeadb24";
                    "span_id" = 12174663615949324932u;
                    "tablet_id" = "7588-33794-fb702be-a1dc6861";
                    "datetime" = "2023-06-22T10:07:32.213995Z";
                    "tid" = 14514940285303313587u;
                    "pid" = 1477;
                    "is_tablet_unmounted" = %true;
                    "host" = "yt-queue-agent-prestable-2.vla.yp-c.yandex.net";
                    "fid" = 18446093353932736666u;
                };
                "code" = 1702;
                "message" = "Cannot read from tablet 7588-33794-fb702be-a1dc6861 of table #18a5b-28931-3ff0191-35282540 while it is in \"unmounted\" state";
            };
        };
    };
    "registrations" = [
        {
            "consumer" = "hume://tmp/test_consumer";
            "vital" = %true;
            "queue" = "pythia://tmp/test_queue";
        };
    ];
}

# We can see some errors in the responses above, since both tables are unmounted.
# Mount queue and consumer tables.
$ yt --proxy pythia mount-table //tmp/test_queue
$ yt --proxy hume mount-table //tmp/test_consumer

# Check statuses again:
$ $ yt --proxy pythia get //tmp/test_queue/@queue_partitions
[
    {
        "meta" = {
            "cell_id" = "185bc-b318a-3f302bc-ea8d7f14";
            "host" = "vla3-2329-node-pythia.vla.yp-c.yandex.net:9012";
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
        "commit_idle_time" = 1687428752830;
    };
]
$ yt --proxy hume get //tmp/test_consumer/@queue_consumer_status
{
    "queues" = {
        "pythia://tmp/test_queue" = {
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
            "consumer" = "hume://tmp/test_consumer";
            "vital" = %true;
            "queue" = "pythia://tmp/test_queue";
        };
    ];
}

# Enable automatic trimming based on vital consumers for queue.
$ yt --proxy pythia set //tmp/test_queue/@auto_trim_config '{enable=true}'

# Insert some data into queue.
$ for i in {1..20}; do echo '{data=foo}; {data=bar}; {data=foobar}; {data=megafoo}; {data=megabar}' | yt insert-rows --proxy pythia //tmp/test_queue --format yson; done;

# Check that queue status reflects writes.
$ yt --proxy pythia get //tmp/test_queue/@queue_status/write_row_count_rate
{
    "1m_raw" = 2.6419539762457456;
    "current" = 5.995956327053036;
    "1h" = 2.6419539762457456;
    "1d" = 2.6419539762457456;
    "1m" = 2.6419539762457456;
}

# Read data via consumer.
$ yt --proxy hume pull-consumer //tmp/test_consumer "<cluster=pythia>//tmp/test_queue"  --partition-index 0 --offset 0 --max-row-count 10 --format "<format=text>yson"
{"$tablet_index"=0;"$row_index"=0;"data"="foo";"$timestamp"=1811865535093168466u;"$cumulative_data_weight"=20;};
{"$tablet_index"=0;"$row_index"=1;"data"="bar";"$timestamp"=1811865535093168466u;"$cumulative_data_weight"=40;};
{"$tablet_index"=0;"$row_index"=2;"data"="foobar";"$timestamp"=1811865535093168466u;"$cumulative_data_weight"=63;};
{"$tablet_index"=0;"$row_index"=3;"data"="megafoo";"$timestamp"=1811865535093168466u;"$cumulative_data_weight"=87;};
{"$tablet_index"=0;"$row_index"=4;"data"="megabar";"$timestamp"=1811865535093168466u;"$cumulative_data_weight"=111;};

# Advance consumer.
$ yt --proxy hume advance-consumer //tmp/test_consumer "<cluster=pythia>//tmp/test_queue" --partition-index 0 --old-offset 0 --new-offset 42

# Since trimming is enabled and the consumer is the only vital consumer for the queue, soon the rows up to index 42 will be trimmed.
# Calling pull-consumer now returns the next available rows.
$ yt --proxy hume pull-consumer //tmp/test_consumer "<cluster=pythia>//tmp/test_queue"  --partition-index 0 --offset 0 --max-row-count 5 --format "<format=text>yson"
{"$tablet_index"=0;"$row_index"=42;"data"="foobar";"$timestamp"=1811865674679584351u;"$cumulative_data_weight"=951;};
{"$tablet_index"=0;"$row_index"=43;"data"="megafoo";"$timestamp"=1811865674679584351u;"$cumulative_data_weight"=975;};
{"$tablet_index"=0;"$row_index"=44;"data"="megabar";"$timestamp"=1811865674679584351u;"$cumulative_data_weight"=999;};
{"$tablet_index"=0;"$row_index"=45;"data"="foo";"$timestamp"=1811865674679607803u;"$cumulative_data_weight"=1019;};
{"$tablet_index"=0;"$row_index"=46;"data"="bar";"$timestamp"=1811865674679607803u;"$cumulative_data_weight"=1039;};
```

{% if audience == "internal" %}
Результат операций на графиках можно видеть на вкладке очереди и консьюмера, а также на общем [дашборде](https://{{monitoring-domain}}.{{internal-domain}}/projects/yt/dashboards/monuob4oi7lf8uddjs76?p%5Bconsumer_cluster%5D=hume&p%5Bconsumer_path%5D=%2F%2Ftmp%2Ftest_consumer&p%5Bqueue_cluster%5D=pythia&p%5Bqueue_path%5D=%2F%2Ftmp%2Ftest_queue&from=1687429383727&to=1687444244000&forceRefresh=1687444307283).

## Ограничения и проблемы

  - Асинхронное вычисление лагов консьюмеров может приводить к [небольшим отрицательным значениям в реактивных процессингах](https://st.{{internal-domain}}/YT-19361).
{% endif %}


