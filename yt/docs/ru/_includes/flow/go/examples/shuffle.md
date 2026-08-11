# Shuffle в {{product-name}} Flow (Go)

Пример [пайплайна](../../../../flow/concepts/glossary.md#pipeline) из двух Go-[компьютейшенов](../../../../flow/concepts/glossary.md#stream-and-computation): source-компьютейшен парсит JSON и отправляет типизированные [сообщения](../../../../flow/concepts/glossary.md#message), transform-компьютейшен ведёт подсчёт событий во внешнем [стейте](../../../../flow/concepts/glossary.md#state). Между ними стоят нативные passthrough-компьютейшены, перегруппирующие событие по четырём разным ключам.

[Исходный код]({{source-root}}/yt/yt/flow/examples/go/shuffle)

## Структура {#structure}

- `reader` (source, `TSwiftOrderedSourceCompanionComputation`) — `eventMapper`: парсит JSON из колонки `data` и отправляет типизированное сообщение в [стрим](../../../../flow/concepts/glossary.md#stream-and-computation) `event`.
- `shuffle_a` … `shuffle_d` — нативные `TSwiftPassthroughComputation`, объявленные в [спеке](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec). Каждый перегруппирует поток по своему ключу (`key_a` … `key_d`) и публикует его в отдельный стрим `event_a` … `event_d`. Go-кода у них нет.
- `reducer` (transform, `TTransformCompanionComputation`) — `eventReducer`: подписан на все четыре стрима и считает число приходов значения во внешнем стейте `/shuffle-state`.

Событие, записанное в очередь один раз, доходит до редьюсера четырьмя разными путями, поэтому учитывается четыре раза. Компаньон обслуживает только концы пайплайна — `reader` и `reducer`.

## `main.go` {#main-go}

Точка входа: регистрация source- и transform-компьютейшенов одним вызовом `pipeline.Add`.

{% code '/yt/yt/flow/examples/go/shuffle/main.go' lang='go' %}

## `event_mapper.go` {#event-mapper-go}

JSON-структура, которую входная очередь несёт в колонке `data`. Четыре ключа независимы друг от друга — именно это даёт shuffle-стадиям то, по чему перегруппировывать:

{% code '/yt/yt/flow/examples/go/shuffle/event_mapper.go' lang='go' lines='[BEGIN event]-[END event]' %}

Source-функция, которая преобразует входную строку в `sourceMessage`, разбирает JSON в `event` и публикует типизированное сообщение: перегруппировать поток можно только по колонке стрима, поэтому распарсенные ключи выносятся в отдельные поля.

{% code '/yt/yt/flow/examples/go/shuffle/event_mapper.go' lang='go' lines='[BEGIN event_mapper]-[END event_mapper]' %}

## `event_reducer.go` {#event-reducer-go}

Transform-функция с внешним стейтом для подсчёта событий:

{% code '/yt/yt/flow/examples/go/shuffle/event_reducer.go' lang='go' lines='[BEGIN event_reducer]-[END event_reducer]' %}

## Ключевые паттерны {#key-patterns}

- Пайплайн из нескольких компьютейшенов: [сорс](../../../../flow/concepts/glossary.md#source) на `flow.NewRowSourceComputation` плюс трансформ на `flow.NewRowComputation`. Оба регистрируются одним `pipeline.Add`.
- Парсинг JSON и создание типизированного сообщения: `msg.ConvertTo(&input)` → `json.Unmarshal` → `flow.ConvertFrom(rt, event)` → `out.AddMessage(msg)`.
- Внешний стейт через `flow.OpenExternalState(rt, "/shuffle-state", msg)`: `ConvertTo` читает счётчик в структуру, `ConvertFrom` сохраняет обновление. Имя внешнего стейта — абсолютный путь, совпадающий с ключом в `external_state_managers` спеки.
- `state.ConvertTo` возвращает признак наличия строки: для ключа, по которому стейт ещё не записывался, счётчик начинается с нуля.
- Перегруппировка потока — задача нативных passthrough-компьютейшенов: одно и то же значение можно посчитать по нескольким разрезам, не написав ни строчки Go-кода для самих shuffle-стадий.
