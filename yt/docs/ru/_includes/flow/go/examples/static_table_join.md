# Static Table Join в {{product-name}} Flow (Go)

Пример [пайплайна](../../../../flow/concepts/glossary.md#pipeline), который загружает статический справочник во внешний [стейт](../../../../flow/concepts/glossary.md#state), а затем обогащает события из очереди по общему ключу.

[Исходный код]({{source-root}}/yt/yt/flow/examples/go/static_table_join)

## Структура {#structure}

- `reference_reader` — нативный static-table source, публикующий строки справочника в стрим `reference`.
- `reference_loader` (`referenceLoader`) — Go transform-компьютейшен, который нормализует значение и записывает его во внешний стейт `/reference_state`.
- `event_reader` — нативный queue-source, публикующий стрим `event`.
- `enricher` (`enricher`) — Go transform-компьютейшен, который читает `/reference_state` через `external_state_joiners` и публикует результат в стрим `enriched`.

Менеджер и joiner ссылаются на одну таблицу стейта: первый заполняет её из статической таблицы, второй выполняет read-only lookup для входящих событий.

## `main.go` {#main-go}

Точка входа регистрирует три типизированных стрима и оба Go-компьютейшена.

{% code '/yt/yt/flow/examples/go/static_table_join/main.go' lang='go' %}

## `reference_loader.go` {#reference-loader-go}

Загрузчик декодирует строку справочника, открывает внешний стейт для её ключа и сохраняет нормализованное значение.

{% code '/yt/yt/flow/examples/go/static_table_join/reference_loader.go' lang='go' %}

## `enricher.go` {#enricher-go}

Обогатитель читает присоединённую строку стейта и создаёт выходное сообщение только для найденного ключа.

{% code '/yt/yt/flow/examples/go/static_table_join/enricher.go' lang='go' %}

## Ключевые паттерны {#key-patterns}

- `flow.OpenExternalState` и `ConvertFrom` используются на стадии загрузки для записи справочника.
- `flow.OpenJoinedExternalState` и `ConvertTo` используются на стадии обогащения для read-only lookup.
- Оба компьютейшена работают с типизированными структурами; преобразование payload выполняется только на входе и выходе обработчика.
- Схемы стримов выводятся из `yson`-тегов и регистрируются через `flow.NewYSONStream`.
