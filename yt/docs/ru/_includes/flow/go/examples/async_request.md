# Async Request в {{product-name}} Flow (Go)

Пример [пайплайна](../../../../flow/concepts/glossary.md#pipeline), реализующего асинхронный поход во внешний сервис: один [компьютейшен](../../../../flow/concepts/glossary.md#stream-and-computation) превращает события в запросы и накапливает ответы во внешнем [стейте](../../../../flow/concepts/glossary.md#state), другой обслуживает запросы без стейта. Go-реализация того же сценария, что и [C++ пример](../../../../flow/cpp/examples/async_request.md).

[Исходный код]({{source-root}}/yt/yt/flow/examples/go/async_request)

## Структура {#structure}

Компаньон обслуживает два компьютейшена, `injector` остаётся нативным сорсом из [спеки](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec):

1. **`state`** (`stateKeeper`) — stateful-компьютейшен, сгруппированный по `key`, который:
   - принимает события из стрима `event` и порождает запрос в стрим `request` со случайным `request_id`;
   - принимает ответы из стрима `response` и складывает суммарную длину (`total_length`) во внешний стейт `/state`.

2. **`processor`** (`requestProcessor`) — stateless-компьютейшен, сгруппированный по `request_id`: принимает запросы из стрима `request` и сразу отвечает длиной строки запроса в стрим `response`.

Цикл `event → request → response → state` замыкается между двумя компьютейшенами. Событие отвечается запросом, а не сразу результатом, поэтому обслуживающая сторона никогда не задерживает обработку: ответ приходит позже, отдельным [сообщением](../../../../flow/concepts/glossary.md#message), и только тогда стейт ключа сдвигается.

## `main.go` {#main-go}

Точка входа: создание пайплайна и регистрация обоих компьютейшенов.

{% code '/yt/yt/flow/examples/go/async_request/main.go' lang='go' %}

## `state_keeper.go` {#state-keeper-go}

Маршрутизация входных стримов (`event` / `response`) и работа с внешним стейтом.

{% code '/yt/yt/flow/examples/go/async_request/state_keeper.go' lang='go' lines='[BEGIN state_keeper]-[END state_keeper]' %}

## `request_processor.go` {#request-processor-go}

Stateless-обработчик запросов: вычисляет длину строки запроса и возвращает ответ.

{% code '/yt/yt/flow/examples/go/async_request/request_processor.go' lang='go' lines='[BEGIN request_processor]-[END request_processor]' %}

## Ключевые паттерны {#key-patterns}

- **Маршрутизация по `msg.StreamID`**: `switch` по идентификатору входного стрима позволяет одному компьютейшену обрабатывать несколько входов с разной логикой. Неизвестный стрим — ошибка, а не молчаливое игнорирование.
- **Случайный `request_id`**: `rand.Uint64()` связывает запрос с ответом. Запрос несёт ключ исходного события, поэтому ответ, партиционированный по `request_id`, возвращается к тому стейту, которому принадлежит поход.
- **Внешний стейт** через `flow.OpenExternalState(rt, "/state", msg)`: строка преобразуется в `totalLengthState` через `ConvertTo`, изменяется как структура и сохраняется через `ConvertFrom`.
- **Stateless-компьютейшен**: `requestProcessor` не использует стейт и сгруппирован по `request_id`, а не по ключу события, поэтому запросы одного ключа расходятся по всем [партициям](../../../../flow/concepts/glossary.md#partition) и масштабируются независимо.
- **Зависимость стримов**: `streams_dependency` в спеке объявляет, что `request` порождается из `event` — воркер учитывает это при продвижении [вотермарков](../../../../flow/concepts/watermarks.md).
