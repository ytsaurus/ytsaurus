# External State Join в {{product-name}} Flow (Go)

Пример [пайплайна](../../../../flow/concepts/glossary.md#pipeline), который обогащает события из очереди данными из read-only внешнего [стейта](../../../../flow/concepts/glossary.md#state). Таблица справочника подключена через `TSimpleExternalStateJoiner`, а Go-компьютейшен выполняет lookup по ключу события.

[Исходный код]({{source-root}}/yt/yt/flow/examples/go/external_state_join)

## Структура {#structure}

- `event_reader` — нативный queue-source, публикующий стрим `event`.
- `lookup_join` (`lookupJoin`) — Go transform-компьютейшен, сгруппированный по `key`. Он читает `/reference` через `external_state_joiners` и публикует обогащённое сообщение в стрим `enriched`.
- Queue-sink сохраняет сообщения из `enriched`.

Путь joiner-а в спеке может указывать на Cypress-ссылку. Это позволяет атомарно переключать справочник на новую версию таблицы без рестарта пайплайна.

## `main.go` {#main-go}

Точка входа регистрирует схемы входного и выходного стримов, добавляет компьютейшен и запускает пайплайн.

{% code '/yt/yt/flow/examples/go/external_state_join/main.go' lang='go' %}

## `lookup_join.go` {#lookup-join-go}

Компьютейшен декодирует типизированное входное сообщение, читает присоединённую строку стейта и создаёт типизированное выходное сообщение.

{% code '/yt/yt/flow/examples/go/external_state_join/lookup_join.go' lang='go' %}

## Ключевые паттерны {#key-patterns}

- `flow.OpenJoinedExternalState(rt, referenceStateName, msg)` открывает read-only стейт для ключа текущего сообщения.
- `flow.ErrStateNotRead` означает, что для ключа нет присоединённой строки; пример в этом случае не публикует результат.
- `ConvertTo` остаётся на границе SDK: бизнес-логика работает с `eventMessage`, `referenceState` и `enrichedMessage`, а не с сырыми строками wire-протокола.
- Joined external state не изменяется из компьютейшена: метод `ConvertFrom` для него не используется.
