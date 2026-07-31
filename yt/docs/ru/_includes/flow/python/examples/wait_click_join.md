# Wait Click Join в {{product-name}} Flow (Python)

Пример join-[пайплайна](../../../../flow/concepts/glossary.md#pipeline), объединяющего [потоки](../../../../flow/concepts/glossary.md#stream-and-computation) показов (hit) и действий (action) по ключу с использованием [таймеров](../../../../flow/concepts/glossary.md#timer) и external [state](../../../../flow/concepts/glossary.md#state). Python-реализация аналогичного [C++ примера](../../../../flow/cpp/examples/wait_click_join.md).

[Исходный код]({{source-root}}/yt/yt/flow/examples/python/wait_click_join)

## Структура

Пайплайн состоит из одного transform-компьютейшена `join`, который:
1. Получает сообщения из двух входных стримов: `hit` и `action`.
2. Накапливает данные во внешнем стейте.
3. Устанавливает таймер на окончание окна ожидания.
4. При срабатывании таймера генерирует join-результат или очищает стейт.

## `__main__.py`

{% code '/yt/yt/flow/examples/python/wait_click_join/__main__.py' lang='python' lines='[BEGIN main]-[END main]' %}

## `join_process_function.py`

Основная логика join-а. Функция обрабатывает два стрима и использует таймеры для оконной агрегации:

### `on_message`

{% code '/yt/yt/flow/examples/python/wait_click_join/join_process_function.py' lang='python' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

### `on_timer`

{% code '/yt/yt/flow/examples/python/wait_click_join/join_process_function.py' lang='python' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

## Ключевые паттерны

- **External state** с `ctx.external_state("/join-state", message)`: паттерн `to_builder()` / `set()` / `clear()` для накопления данных из нескольких стримов.
- **Таймеры** через `output.add_timer(max_time, hit_time)`: установка таймера с `trigger_timestamp` (когда сработает) и `event_timestamp` (привязка к событию).
- **Фильтрация запоздавших данных**: `message.event_timestamp < ctx.min_watermark` для отброса late data (подробнее о [вотермарках](../../../../flow/concepts/glossary.md#timestamps-and-watermarks)).
- **Параметры компьютейшена**: `ctx.parameters["wait_for_actions"]` для чтения конфигурации из спеки.
- **MessageBuilder**: `ctx.message_builder("joined_action")` для создания выходных сообщений с заданной схемой.
- **Обработка нескольких стримов**: ветвление по `message.stream_id` для различной логики обработки hit и action.

## См. также

- [Быстрый старт (Python)](../../../../flow/python/getting-started.md)
- [Таймеры](../../../../flow/concepts/timers.md)
- [Stateful processing](../../../../flow/concepts/stateful.md)
