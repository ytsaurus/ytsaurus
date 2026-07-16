# StateAccessor в {{product-name}} Flow (Python)

StateAccessor — интерфейс для чтения, модификации и удаления значений [стейта](../../../flow/concepts/glossary.md#state).
Общие сведения о stateful-обработке описаны в разделе [Stateful processing](../../../flow/concepts/stateful.md).

## Принцип работы {#how-it-works}

[Стейт](../../../flow/concepts/glossary.md#state) во Flow хранится в [сортированных динамических таблицах](../../../user-guide/dynamic-tables/sorted-dynamic-tables.md).
В случае [внешнего стейта](../../../flow/python/external-state.md) эта таблица создаётся пользователем, в случае [внутреннего стейта](../../../flow/python/internal-state.md) таблицы создаются и управляются Flow автоматически.

Ключевые колонки в таблице стейта совпадают с `group_by_schema` того [компьютейшена](../../../flow/concepts/glossary.md#stream-and-computation), который использует этот стейт. Это означает, что стейт привязан к ключу [сообщения](../../../flow/concepts/glossary.md#message) — все сообщения с одинаковым ключом разделяют один стейт.

## Чтение и запись данных {#reading-and-writing-data}

Непосредственную работу с таблицей (чтение, запись, удаление данных) осуществляет [воркер](../../../flow/concepts/glossary.md#worker). При получении очередного батча сообщений воркер загружает значения стейтов для всех ключей в батче и отправляет их в [компаньон](../../../flow/concepts/companion.md) вместе с сообщениями и таймерами. Подробнее про [схему взаимодействия](../../../flow/concepts/companion.md#schema).

Запись новых значений в таблицу стейта осуществляется транзакционно в рамках [эпохи](../../../flow/concepts/glossary.md#epoch).

## Четыре типа аксессоров {#accessor-types}

Python SDK предоставляет четыре вида аксессоров для работы со стейтом:

| Аксессор | Формат | Получение | Описание |
|----------|--------|-----------|----------|
| [YsonStateAccessor](../../../flow/python/internal-state.md#yson-state-accessor) | YSON (dict) | `ctx.state(name, msg)` | Сериализация Python dict в YSON |
| [RawStateAccessor](../../../flow/python/internal-state.md#raw-state-accessor) | `bytes` | `ctx.raw_state(name, msg)` | Сырые байты без сериализации |
| [ProtoStateAccessor](../../../flow/python/internal-state.md#proto-state-accessor) | Protobuf | `ctx.proto_state(name, msg, ProtoClass)` | Сериализация через Protobuf |
| [ExternalStateAccessor](../../../flow/python/external-state.md) | Payload (строка таблицы) | `ctx.external_state("/name", msg)` | Типизированный доступ к внешней таблице |

Первые три аксессора работают с [внутренним стейтом](../../../flow/python/internal-state.md) (таблицы управляются Flow автоматически). `ExternalStateAccessor` работает с [внешним стейтом](../../../flow/python/external-state.md) (пользователь создаёт таблицу самостоятельно).

## Общий API {#common-api}

Все внутренние аксессоры (`YsonStateAccessor`, `RawStateAccessor`, `ProtoStateAccessor`) предоставляют единый набор методов:

| Метод | Описание |
|-------|----------|
| `get()` | Получить значение стейта (или `None`, если стейт отсутствует) |
| `set(value)` | Установить значение стейта |
| `clear()` | Удалить стейт для текущего ключа |
| `get_or_default(default)` | Получить значение или вернуть `default` |

## Получение аксессора {#getting-accessor}

Аксессор получается через `RuntimeContext` (`ctx`) внутри `on_message` или `on_timer`:

```python
class MyFunction(RowFunction):
    def on_message(self, message, output, ctx):
        # YSON (dict)
        yson_state = ctx.state("state-name", message)

        # Raw bytes
        raw_state = ctx.raw_state("state-name", message)

        # Protobuf
        proto_state = ctx.proto_state("state-name", message, MyProtoClass)

        # External (имя обязательно начинается с "/")
        ext_state = ctx.external_state("/state-name", message)

    def on_timer(self, timer, output, ctx):
        # Аналогично, но вместо message передаётся timer
        state = ctx.state("state-name", timer)
```

Параметры:
- `name` — строка с именем стейта, объявленным в [статической спеке](../../../flow/concepts/glossary.md#spec-and-dynamic-spec). Для внутренних стейтов — произвольная строка из `internal_states`; для внешних стейтов — ключ из `external_state_managers`, обязательно начинающийся с `/`.
- `message` / `timer` — сообщение или таймер, для [ключа](../../../flow/concepts/glossary.md#key) которого нужно получить стейт.
- `ProtoClass` (только для `proto_state`) — класс Protobuf-сообщения для десериализации.

## Когда использовать какой тип {#choosing-type}

| Ситуация | Рекомендуемый аксессор |
|----------|----------------------|
| Простой словарь с несколькими полями | `ctx.state()` (YSON) |
| Произвольные двоичные данные | `ctx.raw_state()` (Raw) |
| Структурированные данные с фиксированной схемой | `ctx.proto_state()` (Protobuf) |
| Данные, к которым нужен доступ из других систем | `ctx.external_state("/name", msg)` (External) |
| Данные, требующие пользовательской таблицы | `ctx.external_state("/name", msg)` (External) |

## См. также

- [Internal State (Python)](../../../flow/python/internal-state.md)
- [External State (Python)](../../../flow/python/external-state.md)
- [Работа со стейтами (Python)](../../../flow/python/state.md) — краткий обзор всех типов
- [Stateful processing (концепция)](../../../flow/concepts/stateful.md)
- [StateAccessor (Java)](../../../flow/java/state-accessor.md)
