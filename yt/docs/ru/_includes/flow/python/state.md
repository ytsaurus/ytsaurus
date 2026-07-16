# Работа со стейтами в {{product-name}} Flow (Python)

{% note info %}

Данная страница описывает Python API для работы со стейтами. Общие концепции стейтов описаны в разделе [Stateful-вычисления](../../../flow/concepts/stateful.md).

{% endnote %}

## YSON State {#yson-state}

Самый простой способ работы со стейтом -- YSON-формат. Стейт хранится как словарь Python, автоматически сериализуемый в YSON:

```python
state = ctx.state("state-name", message)
```

Возвращает `YsonStateAccessor` с методами:
- `get()` -- получить текущее значение (`dict` или `None`).
- `set(dict)` -- сохранить значение.
- `clear()` -- удалить стейт.
- `get_or_default(dict)` -- получить текущее значение или вернуть значение по умолчанию.

Пример из [WordCount](../../../flow/python/examples/wordcount.md):

{% code '/yt/yt/flow/examples/python/word_count/word_count_mapper.py' lang='python' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

Здесь стейт привязан к ключу сообщения (определяемому через `group_by_schema` в [спеке](../../../flow/concepts/glossary.md#spec-and-dynamic-spec)). Для каждого уникального ключа хранится свой независимый стейт.

## Raw State {#raw-state}

Для хранения стейта в виде сырых байтов:

```python
state = ctx.raw_state("state-name", message)
```

Возвращает `RawStateAccessor` с методами:
- `get()` -- получить значение (`bytes` или `None`).
- `set(bytes)` -- сохранить значение.
- `clear()` -- удалить стейт.
- `get_or_default(bytes)` -- получить значение или вернуть значение по умолчанию.

## Proto State {#proto-state}

Для хранения стейта в виде Protobuf-сообщения:

```python
state_accessor = ctx.proto_state("state-name", message, TJoinState)
```

Возвращает `ProtoStateAccessor` с методами:
- `get()` -- десериализовать и вернуть Protobuf-объект (или `None`).
- `set(proto_message)` -- сериализовать и сохранить.
- `clear()` -- удалить стейт.
- `get_or_default(default=None)` -- получить значение или вернуть default. Если default не указан, возвращает пустой экземпляр Proto-класса.

{% if audience == "internal" %}

Пример из [Logbroker WaitClickJoin](../../../yandex-specific/flow/python/examples/lb_wait_click_join.md) — `JoinFunction.on_message`. Класс `TJoinState` импортируется из общего для Java и Python proto-модуля `yt.yt.flow.yandex.examples.java.lb_wait_click_join.proto.message_pb2`:

{% code '/yt/yt/flow/yandex/examples/python/lb_wait_click_join/join_function.py' lang='python' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endif %}

## External State {#external-state}

Внешний стейт работает как Payload -- предоставляет dict-like доступ к полям:

```python
state = ctx.external_state("/state-name", message)
```

Имя стейта обязательно начинается с `/` и должно совпадать с ключом в `external_state_managers` статической спеки. Вызов `ctx.external_state("state-name", message)` (без ведущего `/`) бросит `ValueError`.

Возвращает `ExternalStateAccessor`, который одновременно является `Payload`:
- `state.get("field")` -- чтение поля.
- `state["field"]` -- dict-like чтение поля.
- `state.to_builder()` -- получить `PayloadBuilder` с текущими значениями.
- `state.set(payload)` -- сохранить новое значение (принимает Payload от `builder.finish()`).
- `state.clear()` -- удалить стейт.

Пример из [Shuffle](../../../flow/python/examples/shuffle.md) (EventReducer):

{% code '/yt/yt/flow/examples/python/shuffle/event_reducer.py' lang='python' lines='[BEGIN event_reducer]-[END event_reducer]' %}

Паттерн работы с external state:
1. Получить текущее состояние через `ctx.external_state(...)`.
2. Создать билдер через `state.to_builder()`.
3. Обновить нужные поля через `builder.set(...)`.
4. Сохранить через `state.set(builder.finish())`.

## Стейт в таймерах {#state-in-timers}

API для работы со стейтом в обработчике таймеров идентичен -- вместо `message` передаётся объект `timer`:

```python
def on_timer(self, timer, output, ctx):
    state = ctx.external_state("/join-state", timer)
    # Чтение стейта
    show_time = state.get("show_time")
    hit_payload = state.get("hit_payload")
    # Очистка стейта после обработки
    state.clear()
```

Пример из [WaitClickJoin](../../../flow/python/examples/wait_click_join.md) (JoinProcessFunction):

{% code '/yt/yt/flow/examples/python/wait_click_join/join_process_function.py' lang='python' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

## Привязка стейта к ключу {#group-by-schema}

Стейт привязывается к [ключу](../../../flow/concepts/glossary.md#key) сообщения, определяемому через `group_by_schema` в спеке компьютейшена. Все сообщения с одинаковым ключом разделяют один стейт. Подробнее о конфигурации ключей см. [Stateful-вычисления](../../../flow/concepts/stateful.md).

## Конфигурация стейтов в спеке {#spec-configuration}

Внутренние стейты (YSON, Raw, Proto) должны быть объявлены в параметрах [компьютейшена](../../../flow/concepts/glossary.md#stream-and-computation) в секции `internal_states`. Имя стейта в коде (первый аргумент `ctx.state(...)`) должно совпадать с именем, объявленным в спеке.

Внешние стейты (External) конфигурируются через секцию `external_state_managers` в спеке компьютейшена и имеют собственную схему, описывающую доступные поля. Ключ внутри `external_state_managers` (например, `"/shuffle-state"`) задаёт имя стейта, начинающееся с `/`; в поле `external_state_manager_class_name` указывается зарегистрированный класс менеджера (для типового сценария — `"NYT::NFlow::TSimpleExternalStateManager"`). Подробнее про спеку и доступные менеджеры см. в [External State](../../../flow/python/external-state.md#static-spec) и в [C++ документации](../../../flow/cpp/state.md#external-state).

## См. также

- [Stateful processing (концепция)](../../../flow/concepts/stateful.md)
- [Computation (Python)](../../../flow/python/computation.md)
- [Быстрый старт (Python)](../../../flow/python/getting-started.md)
