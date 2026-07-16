# Объект Pipeline в {{product-name}}

В данном разделе описан Cypress-объект `pipeline` &mdash; единица деплоя {{product-name}} Flow. Здесь рассматриваются модель данных, способы создания, внутренние таблицы и инструменты управления.

Связанные разделы: [Pipeline в глоссарии](../../../flow/concepts/glossary.md#pipeline), [Spec, DynamicSpec и Config](../../../flow/concepts/spec.md), [Stateful-обработка](../../../flow/concepts/stateful.md), [Внутренние таблицы пайплайна](../../../flow/concepts/glossary.md#inner-pipeline-tables).

## Модель данных { #data_model }

*Пайплайном (pipeline)* в {{product-name}} называется специальный Cypress-объект типа `pipeline`. По устройству это map-узел &mdash; директория, в которой автоматически создаётся набор служебных [внутренних таблиц](#internal_tables), необходимых для работы Flow.

## Внутренние таблицы { #internal_tables }

<small>Таблица 1 &mdash; Внутренние таблицы пайплайна</small>

| Имя таблицы                         | Назначение                                                                                                                                                                                                                                                                                                        |
|-------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `input_messages`                    | Индекс входных сообщений [transform computation'ов](../../../flow/concepts/computation.md#ttransformcomputation), использующийся для дедупликации                                                                                                                                                                                        |
| `compact_input_messages`            | Компактный индекс входных сообщений. Используется по умолчанию для всех computation'ов, кроме случая, когда включён `experimental_enable_non_uint_key`; поведение переопределяется параметром `use_compact_input_messages` в [TComputationSpec](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TComputationSpec) |
| `compact_output_messages`           | Не используется                                                                                                                                                                                                                                                                                                   |
| `compact_partition_output_messages` | Выходные сообщения transform computation'ов, физически сгруппированные по партициям и чанкованные по `stream_id` / `chunk_id` для оптимального чтения |
| `states`                            | Пользовательские и служебные [стейты](../../../flow/concepts/stateful.md), сохраняемые по [ключу](../../../flow/concepts/glossary.md#key)                                                                                                                                                                                                                       |
| `partition_states`                  | Стейты сохраняемые по партиции                                                                                                                                                                                                                                                                                    |
| `timers`                            | [Таймеры](../../../flow/concepts/glossary.md#timer) пользовательского кода                                                                                                                                                                                                                                                               |
| `controller_logs`                   | Логи событий [Controller](../../../flow/concepts/glossary.md#controller) в категории PublicFlowController                                                                                                                                                                                                                                |
| `flow_state`                        | Текущее состояние Flow                                                                                                                                                                                                                                                                                            |
| `flow_state_obsolete`               | KV-хранилище Flow для именнованных объектов (spec, dynamic_spec...)                                                                                                                                                                                                                                               |
| `partition_transactions`            | Служебная таблица для безопасного ретрая транзакций                                                                                                                                                                                                                                                               |

После создания пайплайна они появляются под путём `<pipeline_path>/<table_name>` и автоматически монтируются.

{% note warning "Внимание" %}

Внутренние таблицы являются служебными и их структура может меняться между релизами Flow. Не следует читать или писать в них напрямую из пользовательского кода. Для чтения отладочных данных (например, логов контроллера) используйте `yt flow show-logs` и другие команды семейства `yt flow`.

{% endnote %}

## Создание пайплайна { #create }

{% if audience == "internal" %}

### Через YtSync { #yt-sync }

В Yandex-инфраструктуре единственный поддерживаемый способ создания пайплайна &mdash; [YtSync]({{yt-sync-docs}}/). Он обеспечивает декларативное описание, корректные физические атрибуты, миграции схем и единообразное управление сопутствующими сущностями (пользовательские таблицы, очереди, консьюмеры).

Достаточно описать пайплайн в `pipelines.py` через пресет `builtin:pipeline_preset`, и запустить сценарий `ensure`:

```bash
./yt_sync/yt_sync --scenario ensure --stage <stage> --commit
```

YtSync создаёт пайплайн и сопутствующие сущности (таблицы, очереди, консьюмеры) в едином декларативном описании.

{% else %}

### Через yt_sync_mini { #yt-sync-mini }

Рекомендуемый способ создания пайплайна в опенсорсе &mdash; Python-библиотека [`yt_sync_mini`]({{source-root}}/yt/yt/flow/library/python/yt_sync_mini). Она создаёт map-узел типа `pipeline`, все [внутренние таблицы](#internal_tables) с корректными схемами и физическими атрибутами и сразу монтирует их. Операция идемпотентна &mdash; повторный запуск над уже существующим пайплайном является no-op.

```python
import yt.wrapper as yt

from yt.yt.flow.library.python.yt_sync_mini import yt_sync_mini

client = yt.YtClient(proxy="<cluster>")
yt_sync_mini(client, "<pipeline_path>")
```

### Низкоуровневое создание Cypress-узла { #low-level-create }

Если нужен полный контроль над созданием узла и таблиц (например, чтобы интегрировать в существующую систему деплоя), пайплайн создаётся штатным механизмом `create` &mdash; так же, как и для других типов Cypress-объектов (table, map_node, queue_consumer и т. д.). При таком подходе пользователь сам отвечает за создание и монтирование внутренних таблиц с корректными схемами и атрибутами.

#### Через {{product-name}} CLI

```bash
yt --proxy <cluster> create pipeline <pipeline_path>
```

#### Через Python ({{product-name}} wrapper)

```python
import yt.wrapper as yt

client = yt.YtClient(proxy="<cluster>")
client.create(
    "pipeline",
    "<pipeline_path>"
)
```

#### Через C++ ({{product-name}} native client)

```cpp
#include <yt/yt/flow/lib/native_client/pipeline_init.h>

NYT::NApi::TCreateNodeOptions options;

auto nodeId = NYT::NFlow::CreatePipelineNode(client, pipelinePath, options);
```

{% endif %}

## External State { #external-state }

При смене схемы [внутренних таблиц](#internal_tables) в новой версии Flow апгрейд формата выполняется отдельной миграцией &mdash; см. [Внутренние таблицы пайплайна](../../../flow/concepts/glossary.md#inner-pipeline-tables) и [Базовые правила выкатки](../../../flow/release/basic-rules.md).

Если пайплайн использует [External State](../../../flow/concepts/stateful.md) (пользовательские таблицы за пределами узла), их создание и эволюция схем &mdash; ответственность пользователя. {% if audience == "internal" %}В Yandex-инфраструктуре для этого используется [YtSync]({{yt-sync-docs}}/).{% else %}Операции выполняются стандартными командами `yt create table ... --attributes '{dynamic=true; schema=...}'` и `yt mount-table` &mdash; см. примеры в разделе [Команда create](../../../user-guide/storage/cypress-example.md#create).{% endif %}

## См. также { #see_also }

- [Глоссарий: Pipeline](../../../flow/concepts/glossary.md#pipeline)
- [Внутренние таблицы пайплайна](../../../flow/concepts/glossary.md#inner-pipeline-tables)
- [Базовые правила выкатки пайплайна](../../../flow/release/basic-rules.md)
{% if audience != "internal" %}
- Опенсорс-пример bootstrap-скрипта: [`yt/yt/flow/examples/cpp/noop/yt_sync_mini`]({{source-root}}/yt/yt/flow/examples/cpp/noop/yt_sync_mini)
{% endif %}
