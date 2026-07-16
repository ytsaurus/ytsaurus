# Shuffle в {{product-name}} Flow (Python)

Пример [пайплайна](../../../../flow/concepts/glossary.md#pipeline) из двух [компьютейшенов](../../../../flow/concepts/glossary.md#stream-and-computation): source-компьютейшен парсит JSON-данные и отправляет типизированные [сообщения](../../../../flow/concepts/glossary.md#message), transform-компьютейшен ведёт подсчёт событий во внешнем [стейте](../../../../flow/concepts/glossary.md#state).

[Исходный код]({{source-root}}/yt/yt/flow/examples/python/shuffle)

## Структура

- `reader` (source) -- `EventMapper`: парсит JSON из поля `data` и отправляет типизированные сообщения в [стрим](../../../../flow/concepts/glossary.md#stream-and-computation) `event`.
- `reducer` (transform) -- `EventReducer`: подсчитывает количество событий по ключу с использованием external state.

## `__main__.py`

{% code '/yt/yt/flow/examples/python/shuffle/__main__.py' lang='python' lines='[BEGIN main]-[END main]' %}

## `event_mapper.py`

Source-функция, которая парсит JSON из поля `data` входного сообщения и создаёт типизированное сообщение через `ctx.message_builder()`:

{% code '/yt/yt/flow/examples/python/shuffle/event_mapper.py' lang='python' lines='[BEGIN event_mapper]-[END event_mapper]' %}

## `event_reducer.py`

Transform-функция с external state для подсчёта событий:

{% code '/yt/yt/flow/examples/python/shuffle/event_reducer.py' lang='python' lines='[BEGIN event_reducer]-[END event_reducer]' %}

## Ключевые паттерны

- Пайплайн из нескольких компьютейшенов: source + transform.
- [Source](../../../../flow/concepts/glossary.md#source)-компьютейшен с `source=True` для чтения из внешнего источника.
- Парсинг JSON и создание типизированных сообщений через `ctx.message_builder()`.
- External state с паттерном `to_builder()` / `set()` / `finish()`.

## См. также

- [Быстрый старт (Python)](../../../../flow/python/getting-started.md)
- [Computation (Python)](../../../../flow/python/computation.md)
