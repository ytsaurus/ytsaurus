# External State в {{product-name}} Flow (Python)

External State — механизм работы с внешним [состоянием (стейтом)](../../../flow/concepts/glossary.md#state), хранящимся во внешней динамической таблице {{product-name}}. Пользователь самостоятельно создаёт таблицу для хранения стейта на том же кластере, где развёрнут [пайплайн](../../../flow/concepts/glossary.md#pipeline).

Общие сведения о stateful-обработке описаны в разделе [Stateful processing](../../../flow/concepts/stateful.md).

## Обзор

External State в Python SDK представлен классом `ExternalStateAccessor`, который является подклассом `Payload` и предоставляет dict-like доступ к колонкам внешней динамической таблицы. Стейт привязан к ключу [сообщения](../../../flow/concepts/glossary.md#message) (`group_by_schema`). Подробнее про `StateAccessor` и работу со стейтом: [StateAccessor](../../../flow/python/state-accessor.md).

{% note info %}

Если нужен **только read-only** доступ к внешнему стейту (join по ключу с кэшированием по TTL, без модификации), на стороне фреймворка существует отдельный механизм — **External State Joiner**. На текущий момент joiner доступен только в C++ ([External State Joiner](../../../flow/cpp/state.md#external-state-joiner)) и объявляется в спеке компьютейшена в top-level секции `external_state_joiners` (на одном уровне с `external_state_managers`).

{% endnote %}

## Отличие от Internal State

| Характеристика | External State | Internal State |
|---|---|---|
| Хранение | Внешняя динамическая таблица | Внутренние таблицы Flow |
| Создание таблицы | Пользователь создаёт самостоятельно | Автоматически |
| Формат данных | Типизированный `Payload` (строка таблицы) | Произвольный (YSON, Protobuf, raw bytes) |
| Доступ из других систем | Да (сортированная динамическая таблица) | Нет |
| Схема | Определяется схемой таблицы | Определяется пользователем |

Подробнее о Internal State см. [Internal State](../../../flow/python/internal-state.md).

## Получение ExternalStateAccessor {#getting-accessor}

`ExternalStateAccessor` получается через `RuntimeContext` (`ctx`):

```python
# Для сообщения
state = ctx.external_state("/state-name", message)

# Для таймера
state = ctx.external_state("/state-name", timer)
```

Параметры:
- `"/state-name"` — строка с именем стейта из секции `external_state_managers` [статической спеки](../../../flow/concepts/glossary.md#spec-and-dynamic-spec). Имя обязательно начинается с `/` и совпадает с ключом в спеке.
- `message` / `timer` — сообщение или таймер, для [ключа](../../../flow/concepts/glossary.md#key) которого нужно получить стейт.

{% note warning %}

Имя external state валидируется: оно должно начинаться с `/`, не быть пустым, не оканчиваться на `/` и не содержать двух подряд `/`. Вызов `ctx.external_state("state-name", message)` (без ведущего `/`) бросит `ValueError`.

{% endnote %}

## ExternalStateAccessor как Payload {#accessor-as-payload}

[Исходный код]({{source-root}}/yt/yt/flow/library/python/companion/context.py)

`ExternalStateAccessor` наследует класс `Payload`, что позволяет читать значения колонок напрямую:

```python
state = ctx.external_state("/shuffle-state", message)

# Dict-like доступ
value = state["count"]          # Выбросит KeyError, если колонки нет
value = state.get("count")      # Вернёт None, если колонки нет
exists = "count" in state       # Проверка наличия
```

Класс `Payload` также предоставляет:
- `keys()` — список имён колонок с непустыми значениями.
- `to_dict()` — конвертация в обычный Python dict.

## Основные операции {#operations}

### Чтение стейта

```python
state = ctx.external_state("/join-state", message)

# Чтение значения колонки
hit_payload = state.get("hit_payload", str)
show_time = state.get("show_time")

# Dict-like доступ
try:
    value = state["hit_payload"]
except KeyError:
    value = None
```

### Запись стейта через PayloadBuilder

Для модификации стейта используется паттерн `to_builder()` / `set()` / `finish()`:

```python
state = ctx.external_state("/join-state", message)

# Получить builder с текущими значениями
builder = state.to_builder()

# Обновить нужные поля
builder.set("hit_payload", "some_value")
builder.set("show_time", 1234567890)

# Сохранить обновлённый стейт
state.set(builder.finish())
```

Метод `to_builder()` возвращает `PayloadBuilder`, предзаполненный текущими значениями стейта. Метод `builder.set(column, value)` возвращает сам builder (поддерживает цепочку вызовов). Метод `builder.finish()` создаёт новый `Payload` и сбрасывает builder.

### Очистка стейта

```python
state = ctx.external_state("/join-state", timer)

# Удаление строки из таблицы
state.clear()
```

{% note info %}

Пустой стейт соответствует отсутствию строки в таблице. Если строки нет, `state.get("column")` вернёт `None`. При вызове `clear()` строка будет удалена из таблицы.

{% endnote %}

## Конфигурация в статической спеке {#static-spec}

Для использования External State необходимо объявить external state manager в секции `external_state_managers` [компьютейшена](../../../flow/concepts/glossary.md#stream-and-computation) в статической спеке:

```yson
"computations" = {
    "reducer" = {
        "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
        "group_by_schema" = [
            {"name" = "hash"; "expression" = "farm_hash(key)"; "type" = "uint64"};
            {"name" = "key"; "type" = "string"};
        ];
        "input_stream_ids" = ["input"];
        "output_stream_ids" = ["output"];
        "external_state_managers" = {
            "/shuffle-state" = {
                "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
                "parameters" = {
                    "path" = "//path/to/state/table";
                };
            };
        };
        "parameters" = {};
    };
};
```

Ключевые поля:
- `external_state_managers` — секция верхнего уровня внутри `Computation` с описанием внешних state-менеджеров (раньше `parameters/external_states`).
- Ключ внутри `external_state_managers` (например, `"/shuffle-state"`) — имя стейта, используемое в Python-коде при вызове `ctx.external_state("/shuffle-state", message)`. Имя обязательно начинается с `/`.
- `external_state_manager_class_name` — имя зарегистрированного класса external state manager. Для типового сценария — `"NYT::NFlow::TSimpleExternalStateManager"`;{% if audience == "internal" %} для профилей BigRT — `"NYT::NFlow::NBigRTExtensions::TProfileManager<TUserProfile>"`.{% endif %} Подробнее см. в [C++ документации](../../../flow/cpp/state.md#external-state).
- `parameters.path` — путь к динамической таблице {{product-name}}, в которой хранится стейт.

## Создание таблицы для стейта

Таблица для External State должна быть создана заранее. Схема ключевых колонок таблицы должна совпадать с `group_by_schema` компьютейшена.

{% if audience == "internal" %}Для создания таблицы рекомендуется использовать [YtSync]({{yt-sync-docs}}/).{% endif %}

## Полный пример — EventReducer из Shuffle {#example}

Пример из [shuffle]({{source-root}}/yt/yt/flow/examples/python/shuffle):

```python
from yt.yt.flow.library.python.companion.computation import RowFunction


class EventReducer(RowFunction):
    """Подсчитывает количество событий для каждого ключа через external state."""

    def on_message(self, message, output, ctx):
        state = ctx.external_state("/shuffle-state", message)
        builder = state.to_builder()
        builder.set("count", (state.get("count") or 0) + 1)
        state.set(builder.finish())
```

[Полный исходный код]({{source-root}}/yt/yt/flow/examples/python/shuffle/event_reducer.py)

Паттерн работы:
1. Получить текущее состояние через `ctx.external_state(...)`.
2. Создать builder через `state.to_builder()`.
3. Обновить нужные поля через `builder.set(...)`.
4. Сохранить через `state.set(builder.finish())`.

## См. также

- [StateAccessor (Python)](../../../flow/python/state-accessor.md)
- [Internal State (Python)](../../../flow/python/internal-state.md)
- [Работа со стейтами (Python)](../../../flow/python/state.md) — краткий обзор
- [Stateful processing (концепция)](../../../flow/concepts/stateful.md)
- [External State (Java)](../../../flow/java/external-state.md)
- [Пример Shuffle](../../../flow/python/examples/shuffle.md)
