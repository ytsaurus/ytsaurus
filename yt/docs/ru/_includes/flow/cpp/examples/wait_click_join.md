# Джойн хитов, показов и кликов на временном окне ({{product-name}} Flow, C++)

## Постановка задачи

Пусть есть воображаемая система с тремя видами событий:
- hit - факт запроса пользователя. Содержит некий hit_payload с полезными данными
- action show - факт, что пользователь увидел ответ, содержит action_time
- action click - факт, что пользователь кликнул на увиденный ответ, содержит action_time
Все события содержат hit_id и hit_time

Для простоты предполагается, что в рамках пользовательского запроса обязательно должен присутствовать hit, не более одного show и не более одного click. Данные едут в двух топиках: hit и action.

На выходе будет построен джойн всех трех логов. В общем случае существует достаточно много вариантов джойна, но в данном случае будет рассмотрен один конкретный вариант:
1) Action может случиться через несколько часов после hit. Однако, данный [пайплайн](../../../../flow/concepts/glossary.md#pipeline) рассматривает только те `action`, для которых верно `action_time < hit_time + wait_for_actions`.
2) Система игнорирует все события, которые произошли уже после закрытия хита. Иными словами она недопускает какого-либо уточнения выпущенных ранее событий.
3) Хиты без показов игнорируются
4) К каждому show присоединяется информация о наличии клика и `hit_payload` с хита.

У п.1 и п.2 могут и другие варианты в зависимости от бизнес-целей. В статье [The Dataflow Model](https://static.googleusercontent.com/media/research.google.com/ru//pubs/archive/43864.pdf) достаточно неплохо рассмотрены различные варианты возможного поведения системы. Настоятельно рекомендуется ознакомится со статьей.

Код лежит [тут]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join).

## Чтение данных

Здесь все достаточно стандартно: так как данные уже лежат в необходимом формате во входной очереди, то вполне достаточно использовать связку `TSwiftPassthroughSourceComputation + TQueueSource`. Регистрируются `action_reader`, который генерирует поток `action`, и `hit_reader`, который генерирует поток `hit`.
Однако, для правильной работы пайплайна необходимо правильно настроить работу со временем. Для этого у обоих `SourceComputation` нужно заполнить `watermark_strategy`:
- Нужно заполнить `event_timestamp` соответствующим временем. Для этого мы используем `event_timestamp_assigner`. Для `action_reader` мы указываем колонку `action_time`, а для `hit_reader` &mdash; `hit_time`.
- Мы явным образом указываем `watermark_generator/out_of_orderness_bound` в `10s` - ключевой параметр для эвристики по оценке [вотермарка](../../../../flow/concepts/glossary.md#timestamps-and-watermarks). Значение указано лишь для тестов, для реальных задач нужно выбирать значение на основе свойств вычитываемого потока.
Детально про все настройки можно прочитать [тут](../../../../flow/concepts/spec.md#watermark-strategy).

Пример [спеки](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec) на примере `action_reader`:

```yson
"spec" = {
    "computations" = {
        "action_reader" = {
            "computation_class_name" = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation";
            "input_stream_ids" = [];
            "output_stream_ids" = ["action"];
            "watermark_strategy" = {
                "event_timestamp_assigner" = {
                    "column" = "action_time";
                };
                "watermark_generator" = {
                    "out_of_orderness_bound" = "10s";
                };
            };
            "sources" = {
                "source_stream" = {
                    "source_class_name" = "NYT::NFlow::TQueueSource";
                    "stream_id" = "injected_stream";
                    "parameters" = {
                    };
                };
            };
            "parameters" = {};
        };
    };
    "streams" = {
        "action" = {
            "schema" = [
                {name = "hit_id"; type = "string";};
                {name = "hit_time"; type = "uint64";};
                {name = "is_click"; type = "boolean";};
                {name = "action_time"; type = "uint64";};
            ];
        };
    };
};
```

Нужно учитывать, что наш генератор вотермарков эвристичен - а значит неизбежно будут ошибки. Иными словами - время от времени отдельные события будут нарушать правила вотермарка. Их обычно называют `late data`. В зависимости от требований конкретной системы, эти события можно обрабатывать некоторым особым образом. В рамках рассматриваемого пайплайна они полностью игнорируются (что, безусловно, приводит к потере некоторых данных).

## Джойн потоков

Для удобства джойн осуществляется по ключу `(hit_id, hit_time)`. В момент прихода первого события по данному ключу запускается таймер на `max_time == hit_time + wait_for_actions`. Система дождётся, пока все события с `event_time < max_time` будут обработаны (со скидкой на точность нашего `watermark`, конечно) и окончательно закроет соответствующий хит, сгенерировав необходимое `output` событие и удалив исходный профиль хита.
При этом данные, которые приехали после закрытия хита будут проигнорированы.

Для начала рассмотрим детальнее спеку джойна:
```yson
{
    "computations" = {
        "join" = {
            "computation_class_name" = "NYT::NFlow::TProcessFunctionComputation";
            "processing_function" = "NYT::NFlow::NExample::TJoinFunction";
            "processing_function_parameters" = {
                "wait_for_actions" = "10s";
            };
            "group_by_schema" = [
                {"name" = "hash"; "expression" = "farm_hash(hit_id)"; "type" = "uint64"};
                {"name" = "hit_id"; "type" = "string"};
                {"name" = "hit_time"; "type" = "uint64"};
            ];
            "input_stream_ids" = ["action"; "hit"];
            "output_stream_ids" = ["joined_action";];
            "timers" = {
                "timer" = {};
            };
            "sinks" = {
                "queue" = {
                    "sink_class_name" = "NYT::NFlow::TQueueSink";
                    "input_stream_ids" = ["joined_action"];
                    "parameters" = {
                    };
                };
            };
        };
    };
    streams = {
        "joined_action" = {
            "schema" = [
                {name = "hit_id"; type = "string";};
                {name = "hit_time"; type = "uint64";};
                {name = "is_click"; type = "boolean";};
                {name = "show_time"; type = "uint64";};
                {name = "click_time"; type = "uint64";};
                {name = "hit_payload"; type = "string";};
            ];
        };
    };
};
```

- В `group_by_schema` дополнительно к `hit_id` и `hit_time` указан `hash` &mdash; для правильной работы алгоритма [партиционирования](../../../../flow/concepts/glossary.md#partition)
- Для работы пайплайна нужен [таймер](../../../../flow/concepts/glossary.md#timer) для закрытия хита &mdash; поэтому регистрируется `timer` в `timers`. Мы не указываем дополнительных настроек, так как таймеры по умолчанию используют `event_time` и входные стримы.
- Для отправки `joined_event` в упорядоченную динтаблицу (которая может быть на другом кластере) используется асинхронный `TQueueSink`.

Сам джойн реализован как [process function](../../../../flow/cpp/process-functions.md) `TJoinFunction` (наследник `IProcessFunction` — обрабатывает сообщения и таймеры поэлементно), которую исполняет встроенный `TProcessFunctionComputation`; в спеке он задаётся через `processing_function`, а `wait_for_actions` передаётся в `processing_function_parameters`. Код рекомендуется читать в самом репозитории в силу его непрерывного улучшения. Ключевые идеи:
- В момент появления первого события по ключу создается таймер для его закрытия.
- Стейт каждого ключа описывается произвольным типом (в данном примере &mdash; `TYsonStruct`-наследником). Хранится такой объект во внутренних таблицах `Flow`. Доступ к стейту осуществляется через `TMutableStateKeyClient`, инициализируемый в `Init` через `IRuntimeInitContext::InitClient`.
- В ответ на каждое событие в этом профиле записывается необходимая информация с этого события.
- В ответ на таймер генерируется выходное событие (если есть необходимые данные), а сам профиль затирается вызовом `state.Clear()`.
- С помощью `context->GetInputEventWatermark()` получается текущий вотермарк и отбрасываются все поздно пришедшие события.

{% code '/yt/yt/flow/examples/cpp/wait_click_join/lib/wait_click_join_functions.cpp' lang='cpp' lines='[BEGIN join_process_message]-[END join_process_message]' keep-indents %}

{% code '/yt/yt/flow/examples/cpp/wait_click_join/lib/wait_click_join_functions.cpp' lang='cpp' lines='[BEGIN join_process_timer]-[END join_process_timer]' keep-indents %}

## Управление объектами в {{product-name}}{% if audience == "internal" %} (`YtSync`){% endif %} {#yt-sync}

В описанной выше воображаемой системе есть следующие объекты:
- `action_queue` и `hit_queue` &mdash; входные [очереди](../../../../user-guide/dynamic-tables/queues.md#data_model).
- `consumer` &mdash; [консьюмер](../../../../user-guide/dynamic-tables/queues.md#data_model) для чтения входных очередей и уведомления брокера очереди о том, какие сообщения полностью обработаны и больше не нужны системе.
- `output_queue` &mdash; выходная очередь с джойном входных логов.
- `producer` &mdash; продюсер для [exactly-once](../../../../flow/concepts/glossary.md#exactly-once) записи в выходную очередь.
- `state` &mdash; [таблица](../../../../user-guide/dynamic-tables/sorted-dynamic-tables.md) для хранения профиля ключа.
- `pipeline` &mdash; cам пайплайн, который представляет собой совокупность таблиц и файлов.

## См. также

- [Быстрый старт (C++)](../../../../flow/cpp/getting-started.md)
- [Таймеры](../../../../flow/concepts/timers.md)
- [Stateful processing](../../../../flow/concepts/stateful.md)

Все эти объекты необходимо создать в {{product-name}}, перед тем как [запускать работу пайплайна](../../../../flow/release/basic-rules.md#launch-flow).{% if audience == "internal" %} Для создания объектов можно воспользоваться библиотекой [YtSync]({{yt-sync-docs}}/). Она позволяет лаконично описать объекты и их отличия между разными [окружениями](../../../../flow/concepts/glossary.md#environment) и выполнять операции создания, обновления, [миграции](../../../../flow/concepts/glossary.md#migration) (в ряде случаев).{% endif %}

{% if audience == "internal" %}В данном примере продемонстрировано использование режима easy mode, подробную документацию по которому можно найти [здесь]({{yt-sync-docs}}/stages_specification).{% endif %}

Исходники примера с использованием `YtSync` лежат в [tools/yt_sync]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/tools/yt_sync):
- [queues.py]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/tools/yt_sync/queues.py) &mdash; описание очередей, консьюмеров и продюсеров.
- [tables.py]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/tools/yt_sync/tables.py) &mdash; описание таблицы профилей `state`.
- [pipelines.py]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/tools/yt_sync/pipelines.py) &mdash; описание пайплайна.
- [stages.py]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/tools/yt_sync/stages.py) &mdash; глобальные настройки для окружений.
- [\_\_main\_\_.py]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join/tools/yt_sync/__main__.py) &mdash; main программы, сводящийся к вызову одной библиотечной функции.
