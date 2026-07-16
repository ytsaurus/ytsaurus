# Internal State в {{product-name}} Flow (Python)

Internal State — механизм работы с внутренним [состоянием (стейтом)](../../../flow/concepts/glossary.md#state), хранящимся во внутренних таблицах Flow. В отличие от [External State](../../../flow/python/external-state.md), пользователю не нужно самостоятельно создавать таблицы — Flow управляет ими автоматически.

Подробнее про `StateAccessor` и общие принципы работы со стейтом: [StateAccessor](../../../flow/python/state-accessor.md).

Общие сведения о stateful-обработке описаны в разделе [Stateful processing](../../../flow/concepts/stateful.md).

## Обзор

Python SDK предоставляет три вида аксессоров для работы с Internal State, различающихся форматом сериализации. Для работы с внешним стейтом используется отдельный аксессор — [ExternalStateAccessor](../../../flow/python/external-state.md).

| Аксессор | Формат | Описание |
|----------|--------|----------|
| [YsonStateAccessor](#yson-state-accessor) | YSON (dict) | Сериализация Python dict через YSON |
| [RawStateAccessor](#raw-state-accessor) | `bytes` | Без сериализации (сырые байты) |
| [ProtoStateAccessor](#proto-state-accessor) | Protobuf | Сериализация через Protobuf |

Все аксессоры предоставляют единый набор методов: `get()`, `set(value)`, `clear()`, `get_or_default(default)`.

## YsonStateAccessor {#yson-state-accessor}

[Исходный код]({{source-root}}/yt/yt/flow/library/python/companion/context.py)

`YsonStateAccessor` использует YSON-сериализацию. Стейт хранится как Python dict, автоматически сериализуемый в YSON и обратно.

### Получение аксессора

```python
# Для сообщения
state = ctx.state("state-name", message)

# Для таймера
state = ctx.state("state-name", timer)
```

### Методы

| Метод | Возвращаемый тип | Описание |
|-------|-----------------|----------|
| `get()` | `dict` или `None` | Десериализовать и вернуть текущее значение |
| `set(value)` | — | Сериализовать и сохранить значение (dict или bytes) |
| `clear()` | — | Удалить стейт для текущего ключа |
| `get_or_default(default)` | `dict` | Вернуть текущее значение или `default` |

### Пример из WordCount

{% code '/yt/yt/flow/examples/python/word_count/word_count_mapper.py' lang='python' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

[Полный исходный код]({{source-root}}/yt/yt/flow/examples/python/word_count/word_count_mapper.py)

Здесь стейт привязан к ключу [сообщения](../../../flow/concepts/glossary.md#message) (определяемому через `group_by_schema` в [спеке](../../../flow/concepts/glossary.md#spec-and-dynamic-spec)). Для каждого уникального слова хранится свой независимый счётчик.

## RawStateAccessor {#raw-state-accessor}

[Исходный код]({{source-root}}/yt/yt/flow/library/python/companion/context.py)

`RawStateAccessor` работает с сырыми байтами без сериализации/десериализации.

### Получение аксессора

```python
# Для сообщения
state = ctx.raw_state("state-name", message)

# Для таймера
state = ctx.raw_state("state-name", timer)
```

### Методы

| Метод | Возвращаемый тип | Описание |
|-------|-----------------|----------|
| `get()` | `bytes` или `None` | Получить сырые байты |
| `set(value: bytes)` | — | Сохранить сырые байты |
| `clear()` | — | Удалить стейт для текущего ключа |
| `get_or_default(default: bytes)` | `bytes` | Вернуть текущее значение или `default` |

### Пример использования

```python
state = ctx.raw_state("raw-state", message)

data = state.get()
if data is not None:
    # Обработка сырых данных...
    pass

# Запись сырых данных
state.set(b"\x01\x02\x03")

# Очистка
state.clear()
```

## ProtoStateAccessor {#proto-state-accessor}

[Исходный код]({{source-root}}/yt/yt/flow/library/python/companion/context.py)

`ProtoStateAccessor` использует Protobuf-сериализацию. Стейт десериализуется в экземпляр указанного Protobuf-класса.

### Получение аксессора

```python
# Для сообщения
state = ctx.proto_state("state-name", message, TJoinState)

# Для таймера
state = ctx.proto_state("state-name", timer, TJoinState)
```

Третий аргумент — класс Protobuf-сообщения, используемый для десериализации.

### Методы

| Метод | Возвращаемый тип | Описание |
|-------|-----------------|----------|
| `get()` | Proto-объект или `None` | Десериализовать и вернуть значение |
| `set(value)` | — | Сериализовать и сохранить Proto-объект |
| `clear()` | — | Удалить стейт для текущего ключа |
| `get_or_default(default=None)` | Proto-объект | Вернуть значение, `default`, или пустой экземпляр Proto-класса |

{% note info %}

Метод `get_or_default()` без аргументов возвращает пустой экземпляр Proto-класса (эквивалент `ProtoClass()`). Это удобно для инициализации стейта при первом обращении.

{% endnote %}

{% if audience == "internal" %}

### Пример из lb_wait_click_join

{% note info %}

Proto-определение `TJoinState` находится в директории Java-примера (`yt/yt/flow/yandex/examples/java/lb_wait_click_join/proto`), т.к. proto-файлы являются общими для Java и Python: `from yt.yt.flow.yandex.examples.java.lb_wait_click_join.proto.message_pb2 import TJoinState`.

{% endnote %}

`JoinFunction.on_message`:

{% code '/yt/yt/flow/yandex/examples/python/lb_wait_click_join/join_function.py' lang='python' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

`JoinFunction.on_timer`:

{% code '/yt/yt/flow/yandex/examples/python/lb_wait_click_join/join_function.py' lang='python' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

{% endif %}

## Конфигурация в статической спеке {#static-spec}

Internal State не требует создания внешних таблиц. Стейты автоматически хранятся во внутренних таблицах Flow.

Имена внутренних стейтов должны быть объявлены в секции `internal_states` параметров [компьютейшена](../../../flow/concepts/glossary.md#stream-and-computation) в статической спеке:

{% code '/yt/yt/flow/examples/python/word_count/test/pipeline.yson' lang='yson' lines='19-36' %}

Имя стейта в коде (первый аргумент `ctx.state(...)`, `ctx.raw_state(...)` или `ctx.proto_state(...)`) должно совпадать с именем, объявленным в `internal_states`.

{% note warning %}

Если имя стейта не объявлено в `internal_states`, при обращении к нему через `ctx.state(...)` будет выброшено исключение `ValueError`.

{% endnote %}

## См. также

- [StateAccessor (Python)](../../../flow/python/state-accessor.md)
- [External State (Python)](../../../flow/python/external-state.md)
- [Работа со стейтами (Python)](../../../flow/python/state.md) — краткий обзор
- [Stateful processing (концепция)](../../../flow/concepts/stateful.md)
- [Internal State (Java)](../../../flow/java/internal-state.md)
- [Пример WordCount](../../../flow/python/examples/wordcount.md)
{% if audience == "internal" %}- [Пример lb_wait_click_join](../../../yandex-specific/flow/python/examples/lb_wait_click_join.md){% endif %}
