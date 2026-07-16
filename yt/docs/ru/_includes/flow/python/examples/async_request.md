# Async Request в {{product-name}} Flow (Python)

Пример двухкомпонентного [пайплайна](../../../../flow/concepts/glossary.md#pipeline), реализующего асинхронную обработку запросов: один [компьютейшен](../../../../flow/concepts/glossary.md#stream-and-computation) маршрутизирует события в запросы и накапливает ответы, другой — выполняет вычисление без [стейта](../../../../flow/concepts/glossary.md#state). Python-реализация аналогичного [C++ примера](../../../../flow/cpp/examples/async_request.md).

[Исходный код]({{source-root}}/yt/yt/flow/examples/python/async_request)

## Структура

Пайплайн состоит из двух компьютейшенов:

1. **`state`** (`StateKeeperFunction`) — stateful-компьютейшен, который:
   - Принимает события из стрима `event` и порождает запросы в стрим `request` с уникальным `request_id`.
   - Принимает ответы из стрима `response` и накапливает суммарную длину (`total_length`) во внешнем [стейте](../../../../flow/concepts/glossary.md#state).

2. **`processor`** (`RequestProcessorFunction`) — stateless-компьютейшен, который принимает запросы из стрима `request` и немедленно возвращает ответ (длину строки запроса) в стрим `response`.

Цикл `event → request → response → state` замыкается между двумя компьютейшенами.

## `state_keeper_function.py`

Маршрутизация входящих стримов (`event` / `response`) и работа с внешним стейтом.

{% code '/yt/yt/flow/examples/python/async_request/state_keeper_function.py' lang='python' lines='[BEGIN state_keeper]-[END state_keeper]' %}

## `request_processor_function.py`

Stateless-обработчик запросов: вычисляет длину строки запроса и возвращает ответ.

{% code '/yt/yt/flow/examples/python/async_request/request_processor_function.py' lang='python' lines='[BEGIN request_processor]-[END request_processor]' %}

## `__main__.py`

Точка входа: создание пайплайна и регистрация обоих компьютейшенов.

{% code '/yt/yt/flow/examples/python/async_request/__main__.py' lang='python' lines='[BEGIN main]-[END main]' %}

## Ключевые паттерны

- **Маршрутизация по `stream_id`**: ветвление `if stream_id == "event" / "response"` позволяет одному компьютейшену обрабатывать несколько входных стримов с разной логикой.
- **Генерация уникального `request_id`**: `random.getrandbits(64)` обеспечивает корреляцию между запросом и ответом в асинхронном цикле.
- **External state** через `ctx.external_state("/state", message)`: паттерн `to_builder()` / `set()` для накопительного обновления внешнего стейта.
- **Stateless-компьютейшен**: `RequestProcessorFunction` не использует стейт — чистое преобразование запроса в ответ, что позволяет масштабировать его независимо.
- **Двухкомпонентный пайплайн**: `pipeline.add("state", ...)` и `pipeline.add("processor", ...)` регистрируют компьютейшены, стримы между которыми описываются в [спеке](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec).

## См. также

- [Быстрый старт (Python)](../../../../flow/python/getting-started.md)
- [Computation (Python)](../../../../flow/python/computation.md)
- [Stateful processing](../../../../flow/concepts/stateful.md)
- [Аналогичный пример на C++](../../../../flow/cpp/examples/async_request.md)
- [Аналогичный пример на Java](../../../../flow/java/examples/async_request.md)
