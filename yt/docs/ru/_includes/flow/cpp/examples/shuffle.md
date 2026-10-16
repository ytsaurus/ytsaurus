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
                "computation_class_name" = "NYT::NFlow::TProcessFunctionSourceComputation";
                "processing_function" = "NYT::NFlow::NExample::TQueueReader";
                "output_stream_ids" = ["event"];
                "source_streams" = {
                    "queue" = {
                        "source_class_name" = "NYT::NFlow::TQueueSource";
                        "parameters" = {
                            "queue_path" = "<cluster=cluster_name>//path/to/queue";
                            "consumer_path" = "<cluster=cluster_name>//path/to/consumer";
                            "finite" = false;
                        };
                    };
                };
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

- `computations/reader/source_streams` содержит источник `queue` с типом `NYT::NFlow::TQueueSource`. Данный `source` предназначен для чтения данных из сортированной динамической таблицы с использованием `consumer`. В `parameters` указывается из какой очереди и каким консьюмером необходимо читать данные. С параметрами детальнее можно познакомиться в рамках класса `NYT::NFlow::TQueueSourceParameters`.
- Для управления `Computation` необходимо использовать `NYT::NFlow::TQueueSourceController` - так как нам нужно определять число и настройки [партиций](../../../../flow/concepts/glossary.md#partition) на базе входной сортированной динамической таблицы.
- `streams` содержит один поток `event` - распаршенный поток на выходе из `reader`, доступный другим `Computation`. Для него описана соответствующая схема. Этот же поток зарегистрирован и в `computations/reader/output_stream_ids`.
- Класс `TQueueReader` реализует `IProcessFunction`; его запускает `TProcessFunctionSourceComputation`, указанный в `computation_class_name`. Встроенный passthrough здесь не подходит, поскольку для разбора `JSON` нужна пользовательская реализация `ProcessMessage`.

{% code '/yt/yt/flow/examples/cpp/shuffle/lib/shuffle_functions.cpp' lang='cpp' lines='[BEGIN example_shuffle_queue_reader]-[END example_shuffle_queue_reader]' %}

- Source-адаптер работает в Swift-режиме: выходные потоки не материализуются в {{product-name}}, сохраняется только метаинформация, необходимая для детерминированной работы.
- Доступом к очереди, включая нелокальный кластер, управляют `TQueueSource` и адаптер; process function получает уже прочитанное сообщение и не обращается к клиенту {{product-name}} напрямую.

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

- Так как в рамках примера нет какого-либо преобразования данных, то нам достаточно `NYT::NFlow::TSwiftPassthroughComputation`. Если преобразование понадобится, пользовательскую логику следует реализовать как `IProcessFunction` и запустить через `NYT::NFlow::TProcessFunctionSwiftMapComputation`.
- `NYT::NFlow::TSwiftPassthroughComputation` не материализует данные в {{product-name}}.
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
                "computation_class_name" = "NYT::NFlow::TProcessFunctionComputation";
                "processing_function" = "NYT::NFlow::NExample::TReducer";
                "group_by_schema" = [
                    {"name" = "hash"; "expression" = "farm_hash(value)"; "type" = "uint64";};
                    {"name" = "value"; "type" = "string";};
                ];
                "input_stream_ids" = ["event_a"; "event_b"; "event_c"; "event_d";];
                "output_stream_ids" = [];
                "external_state_managers" = {
                    "/state" = {
                        "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
                        "parameters" = {
                            "path" = "//path/to/state";
                        };
                    };
                };
            };
        };
    };
};
```

- Для описания логики используется process function `TReducer`, запущенная через `TProcessFunctionComputation`.
- Для работы со [стейтом](../../../../flow/concepts/glossary.md#state) используется `TSimpleExternalStateManager`, который предоставляет прямой доступ к таблице. `TReducer` хранит `TMutableStateKeyClient<TSimpleExternalState>` и привязывает его к `"/state"` в методе `Init(const IRuntimeInitContextPtr&)`.

{% code '/yt/yt/flow/examples/cpp/shuffle/lib/shuffle_functions.cpp' lang='cpp' lines='[BEGIN example_shuffle_reducer]-[END example_shuffle_reducer]' %}

