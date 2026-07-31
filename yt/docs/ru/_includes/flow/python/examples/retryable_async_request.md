# Retryable Async Request в {{product-name}} Flow (Python)

Расширение примера [AsyncRequest](../../../../flow/python/examples/async_request.md): обработчик запросов поддерживает автоматические повторные попытки через [таймеры](../../../../flow/concepts/glossary.md#timer). При неудаче запрос сохраняется во внутреннем [стейте](../../../../flow/concepts/glossary.md#state) и повторяется через фиксированный интервал, вплоть до `MAX_RETRIES` попыток.

[Исходный код]({{source-root}}/yt/yt/flow/examples/python/retryable_async_request)

## Структура

Пайплайн состоит из двух компьютейшенов:

1. **`state`** (`StateKeeperFunction`) — stateful-компьютейшен: маршрутизирует события в запросы и накапливает суммарную длину ответов во внешнем стейте (аналогично `async_request`).

2. **`processor`** (`RequestProcessorFunction`) — stateful-компьютейшен с логикой повторов:
   - При первой попытке сохраняет запрос во внутреннем стейте и пытается его обработать.
   - Если обработка не удалась — увеличивает счётчик неудач, сохраняет стейт и устанавливает таймер (`DELAY_SECONDS = 5`).
   - По таймеру повторяет попытку (до `MAX_RETRIES = 3`).
   - При успехе — отправляет ответ и очищает стейт.

## `request_processor_function.py`

Ключевой файл примера: реализует логику повторных попыток через `on_timer` и вспомогательный метод `_try_or_retry`.

{% code '/yt/yt/flow/examples/python/retryable_async_request/request_processor_function.py' lang='python' lines='[BEGIN request_processor]-[END request_processor]' %}

## `state_keeper_function.py`

Маршрутизация стримов и накопление результатов во внешнем стейте (идентично `async_request`).

{% code '/yt/yt/flow/examples/python/retryable_async_request/state_keeper_function.py' lang='python' lines='[BEGIN state_keeper]-[END state_keeper]' %}

## `__main__.py`

Точка входа: структура идентична `async_request`, отличается только импортируемыми классами.

{% code '/yt/yt/flow/examples/python/retryable_async_request/__main__.py' lang='python' lines='[BEGIN main]-[END main]' %}

## Ключевые паттерны

- **Retry через таймеры**: `output.add_timer(int(time.time()) + DELAY_SECONDS)` откладывает повторную попытку; `on_timer` читает стейт и повторяет `_try_or_retry` — стандартный паттерн реализации повторов в Flow без внешних очередей.
- **Счётчик попыток в стейте**: поле `failed_attempts` сохраняется вместе с данными запроса в `ctx.state("request-state", message)`, что гарантирует корректность при рестарте.
- **Детерминированная симуляция сбоев**: `_is_request_succeed(request_id, failed_attempts)` моделирует нестабильный внешний сервис; в реальном коде заменяется вызовом HTTP-клиента.
- **Очистка стейта при успехе**: `state.clear()` после успешной обработки предотвращает повторное срабатывание таймера на устаревших данных.
- **Разделение ответственности**: `StateKeeperFunction` не знает о повторах — вся логика retry инкапсулирована в `RequestProcessorFunction`, что упрощает замену стратегии повторов.

## См. также

- [Быстрый старт (Python)](../../../../flow/python/getting-started.md)
- [Computation (Python)](../../../../flow/python/computation.md)
- [Таймеры](../../../../flow/concepts/timers.md)
- [Stateful processing](../../../../flow/concepts/stateful.md)
- [AsyncRequest (Python)](../../../../flow/python/examples/async_request.md)
- [Аналогичный пример на C++](../../../../flow/cpp/examples/retryable_async_request.md)
- [Аналогичный пример на Java](../../../../flow/java/examples/retryable_async_request.md)
