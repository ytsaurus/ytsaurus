# Shuffle в {{product-name}} Flow (C++)

[Пайплайн]({{source-root}}/yt/yt/flow/examples/cpp/shuffle) читает поток в формате `JSON` из сортированной динамической таблицы, многократно группирует по разным ключам, а после считает число уникальных ключей во всех получившихся потоках. Для пайплайна также описан [тест]({{source-root}}/yt/yt/flow/examples/cpp/shuffle/test/test_shuffle.py).

Пайплайн не пытается решить какую-либо бизнесовую задачу.

## Общее описание пайплайна

### Reader. Чтение входных данных

Первый компьютейшн - `Reader`. Он занимается чтением и первичным преобразованием данных из очереди. Вот части [спеки](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec), связанные с данным компьютейшном:

```yson
{
    "spec" = {
        "computations" = {
            "reader" = {
                "computation_class_name" = "TQueueReader";
                "output_stream_ids" = ["event"];
                "sources" = {
                    "source_stream" = {
                        "source_class_name" = "NYT::NFlow::TQueueSource";
                        "parameters" = {
                            "queue_path" = "<cluster=cluster_name>//path/to/queue";
                            "consumer_path" = "<cluster=cluster_name>//path/to/consumer";
                            "finite" = false;
                        };
                    };
                };
                "parameters" = {};
            };
        };
        "streams" = {
            "event" = {
                "schema" = [
                    {"name" = "key_a"; "type" = "uint64";};
                    {"name" = "key_b"; "type" = "uint64";};
                    {"name" = "key_c"; "type" = "uint64";};
                    {"name" = "key_d"; "type" = "uint64";};
                    {"name" = "value"; "type" = "string";};
                ];
            }
        };
    };
}
```

Разберем детально.

- `computations/reader/sources` содержит поток `source_stream` с типом `NYT::NFlow::TQueueSource`. Данный `source` предназначен для чтения данных из сортированной динамической таблицы с использованием `consumer`. В `parameters` указывается из какой очереди и каким консьюмером необходимо читать данные. С параметрами детальнее можно познакомиться в рамках класса `NYT::NFlow::TQueueSourceParameters`.
- Для управления `Computation` необходимо использовать `NYT::NFlow::TQueueSourceController` - так как нам нужно определять число и настройки [партиций](../../../../flow/concepts/glossary.md#partition) на базе входной сортированной динамической таблицы.
- `streams` содержит один поток `event` - распаршенный поток на выходе из `reader`, доступный другим `Computation`. Для него описана соответствующая схема. Этот же поток зарегистрирован и в `computations/reader/output_stream_ids`.
- Класс `TQueueReader` - пользовательский класс, отнаследованный от `TSwiftOrderedSourceComputation`. В данном случае не подходит `TDelayableSwiftPassthroughSourceComputation`, так как необходимо реализовать специальный парсинг в рамках `DoProcessMessage` для парсинга `JSON`.

{% code '/yt/yt/flow/examples/cpp/shuffle/main.cpp' lang='cpp' lines='[BEGIN example_shuffle_queue_reader]-[END example_shuffle_queue_reader]' %}

- Так как `TQueueReader` является наследником `TDelayableSwiftSourceComputation`, то он не сохраняет `output` потоки в {{product-name}}. А сохраняет только метаинформацию, необходимую для детерминированной работы.
- Так как `TQueueReader` может работать с нелокальными очередями, он берет клиентов {{product-name}} из `GetContext()->ClientsCache`, который отдаёт клиента под нужный кластер.

### Shuffle

В пайплайне присутствует несколько перемешиваний: `shuffle_a`, `shuffle_b`, `shuffle_c`, `shuffle_d`. Каждый из них группирует входной поток по соответствующему ключу - `key_a`, `key_b`, `key_c` или `key_d`. Никаких преобразований с данными они не делают, лишь демонстрируют возможность сгруппировать разные объекты.

Разберем спеку на примере`shuffle_b`:

```yson
{
    "spec" = {
        "computations" = {
            "shuffle_b" = {
                "computation_class_name" = "NYT::NFlow::TSwiftPassthroughComputation";
                "group_by_schema" = [
                    {"name" = "hash"; "expression" = "farm_hash(key_b)"; "type" = "uint64";};
                    {"name" = "key_b"; "type" = "uint64";};
                ];
                "input_stream_ids" = ["event_a"];
                "output_stream_ids" = ["event_b"];
            };
        };
        streams = {
            "event_a" = {
                "schema" = [
                    {"name" = "key_a"; "type" = "uint64";};
                    {"name" = "key_b"; "type" = "uint64";};
                    {"name" = "key_c"; "type" = "uint64";};
                    {"name" = "key_d"; "type" = "uint64";};
                    {"name" = "value"; "type" = "string";};
                ];
            };
            "event_b" = {
                "schema" = [
                    {"name" = "key_a"; "type" = "uint64";};
                    {"name" = "key_b"; "type" = "uint64";};
                    {"name" = "key_c"; "type" = "uint64";};
                    {"name" = "key_d"; "type" = "uint64";};
                    {"name" = "value"; "type" = "string";};
                ];
            };
        };
    };
}
```

- Так как в рамках примера нет какого-либо преобразования данных, то нам достаточно `NYT::NFlow::TSwiftPassthroughComputation`. Однако, в случае необходимости такого парсинга, стоит реализовать собственный класс, отнаследовав его от `NYT::NFlow::TSwiftMapComputation`.
- `NYT::NFlow::TSwiftOrderedSourceComputation` не сохраняет чего-либо в {{product-name}}.
- `group_by_schema` содержит соответствующий ключ `key_b`. В него добавлена колонка `hash`, так как партиционирование во `Flow` работает только в предположении, что первая колонка содержит равномерно распределенные значения типа `uint64`.
- `input_stream_ids` и `output_stream_ids` содержат соответственно `event_a` и `event_b`.
- В `spec/streams` также содержатся `event_a` и `event_b` с описанием схемы целиком.

### Reduce

Последний `Computation`. Он читает потоки `event_a`, `event_b`, `event_c`, `event_d` и подсчитывает число встреч каждого `value`. Фактически, исходный поток обрабатывается четыре раза.

```yson
{
    "spec" = {
        "computations" = {
            "reducer" = {
                "computation_class_name" = "TReducer";
                "group_by_schema" = [
                    {"name" = "hash"; "expression" = "farm_hash(value)"; "type" = "uint64";};
                    {"name" = "value"; "type" = "string";};
                ];
                "input_stream_ids" = ["event_a"; "event_b"; "event_c"; "event_d";];
                "output_stream_ids" = [];
                "parameters" = {
                    "state": {
                        "state_path" = "//path/to/state"
                    };
                };
            };
        };
    };
};
```

- Для описания логики используется `TReducer`.
- Для работы со [стейтом](../../../../flow/concepts/glossary.md#state) мы используем `TSimpleExternalStateManager`, который предоставляет прямой доступ к таблице. Мы заводим поле с менеджером и регистрируем его в рамках реализации метода `DoInit()`.

{% code '/yt/yt/flow/examples/cpp/shuffle/main.cpp' lang='cpp' lines='[BEGIN example_shuffle_reducer]-[END example_shuffle_reducer]' %}

## См. также

- [Быстрый старт (C++)](../../../../flow/cpp/getting-started.md)
- [Computation (C++)](../../../../flow/cpp/computation.md)

- Для группировки по `value` обязательно необходимо указать эту колонку (и хэш от неё) в `group_by_schema`.
- В `input_stream_ids` перечисляются все потоки: `event_a`, `event_b`, `event_c`, `event_d` &mdash; чтобы читать все получившиеся потоки. С точки зрения "бизнес логики" это не самое осмысленное действие, однако исходной целью данного пайплайна было протестировать гарантии `exactly-once` даже в случае `Swift` цепочки.
- Так как `TReducer` является наследником `TTransformComputation` &mdash; то `input_message_ids` и `output_messages` в обязательном порядке сохраняются в {{product-name}}. Но `output_messages` у нас пустые. По сути, данный пайплайн сохраняет в {{product-name}} только метаинформацию в рамках `reader`, метаинформацию (`message_id` и `key`) на каждое входное сообщение и таблицу `value => count` в рамках `reducer`. Промежуточные `computation` вообще не взаимодействуют с {{product-name}}.

### DynamicSpec

- Поле `dynamic_spec/computations/<computation_id>/desired_partition_count` заполняется для каждого `computation`, кроме `reader`. В рамках теста `test_shuffle.py` происходит изменение числа партиций.
- В `dynamic_spec/job_tracker/job_threads` указывается необходимое число тредов для выполнения всех джобов.

### Config для запуска

- Ключевое для запуска: `cluster_url`, `proxy_role`, `path`, `rpc_proxy`, `monitoring_port`.
- `controller/scheduler_period` выставлен в 200 для конкретного теста - в реальности должно быть достаточно дефолтного значения.
- `logging` - настройки логирования.

```yson
{
    "cluster_url" = "cluster_name";
    "path" = "//path/to/pipeline";
    "rpc_port" = 81;
    "monitoring_port" = 80;
    "controller" = {
        "scheduler_period" = 200;
    };
    "logging" = {
        "suppressed_messages" = [
        ];
        "rules" = [
            {
                "exclude_categories" = [
                    "Bus";
                    "Dns";
                    "Concurrency";
                    "QueryClient";
                    "Profiling";
                    "RpcClient";
                    "Monitoring";
                    "Net";
                    "Solomon";
                    "Jaeger";
                    "RpcProxyClient";
                    "RpcServer";
                    "Dns";
                    "BufferMetrics";
                ];
                "min_level" = "debug";
                "writers" = [
                    "Stderr";
                ];
            };
        ];
        "writers" = {
            "Stderr" = {
                "type" = "file";
                "file_name" = "/path/to/file.log";
            };
        };
    }
}
```

