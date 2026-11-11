# External State в {{product-name}} Flow (Python)

External State — механизм работы с внешним [состоянием (стейтом)](../../../flow/concepts/glossary.md#state), хранящимся во внешней динамической таблице {{product-name}}. Пользователь самостоятельно создаёт таблицу для хранения стейта на том же кластере, где развёрнут [пайплайн](../../../flow/concepts/glossary.md#pipeline).

Для стейта в формате строк таблицы используйте `ctx.external_state(...)` и `ctx.joined_external_state(...)`. Если менеджер или joiner предоставляет стейт в формате protobuf, используйте [protobuf-аксессоры](#proto-external-state).

Общие сведения о stateful-обработке описаны в разделе [Stateful processing](../../../flow/concepts/stateful.md).

## Обзор {#overview}

В Python SDK External State в формате строк таблицы представлен классом `ExternalStateAccessor`, который является подклассом `Payload` и предоставляет dict-like доступ к колонкам внешней динамической таблицы. Стейт привязан к ключу [сообщения](../../../flow/concepts/glossary.md#message) (`group_by_schema`). Подробнее про `StateAccessor` и работу со стейтом: [StateAccessor](../../../flow/python/state-accessor.md).

Для read-only доступа по ключу используйте **External State Joiner**. Объявите его в top-level секции `external_state_joiners` компьютейшена, рядом с `external_state_managers`, и читайте через `ctx.joined_external_state(...)`. Подробности приведены в разделе [Read-only join](#external-state-joiner).

## Отличие от Internal State {#difference-from-internal-state}

#|
|| **Характеристика** | **External State в формате строк таблицы** | **Internal State** ||
|| Хранение | Внешняя динамическая таблица | Внутренние таблицы Flow ||
|| Создание таблицы | Пользователь создаёт самостоятельно | Автоматически ||
|| Формат данных | Типизированный `Payload` (строка таблицы) | Произвольный (YSON, Protobuf, raw bytes) ||
|| Доступ из других систем | Да (сортированная динамическая таблица) | Нет ||
|| Схема | Определяется схемой таблицы | Определяется пользователем ||
|#

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

### Чтение стейта {#reading-state}

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

### Запись стейта через PayloadBuilder {#writing-state}

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

### Очистка стейта {#clearing-state}

```python
state = ctx.external_state("/join-state", timer)

# Удаление строки из таблицы
state.clear()
```

{% note info %}

Пустой стейт соответствует отсутствию строки в таблице. Если строки нет, `state.get("column")` вернёт `None`. При вызове `clear()` строка будет удалена из таблицы.

{% endnote %}

## Внешний стейт в формате protobuf {#proto-external-state}

`ctx.proto_external_state(name, message_or_timer, proto_class)` возвращает `ProtoExternalStateAccessor`. `ctx.joined_proto_external_state(name, message_or_timer, proto_class)` возвращает тот же аксессор в режиме только для чтения. Передайте класс protobuf-сообщения в `proto_class` и сообщение или таймер с ключом. Имя должно соответствовать [правилам именования внешнего стейта](#getting-accessor) и записи в `external_state_managers` или, для присоединённого стейта, в `external_state_joiners`.

Настроенный менеджер или joiner должен предоставлять protobuf-стейт: его дескриптор для компаньона должен указывать формат `proto` и `proto_type`, равный `proto_class.DESCRIPTOR.full_name`. Выбор аксессора не меняет формат менеджера. `TSimpleExternalStateManager` и `TSimpleExternalStateJoiner` предоставляют стейт в формате строк таблицы и не подходят для protobuf-аксессоров. Конкретная реализация профильного стейта относится к внутреннему расширению; приведённая ниже конфигурация строкового стейта не включает внешний protobuf-стейт.

Аксессор поддерживает следующие операции:

#|
|| **Метод** | **Результат** ||
|| `get()` | Независимый снапшот protobuf-сообщения или `None`, если стейт отсутствует или сброшен. ||
|| `get_or_default()` | Снапшот значения или новый пустой экземпляр `proto_class`. Метод не принимает аргумент по умолчанию и не создаёт стейт. ||
|| `set(value)` | Сериализовать и сохранить снапшот с полной заменой значения. Поля, отсутствующие в `value`, очищаются. Последующие изменения `value` не меняют сохранённый снапшот. ||
|| `clear()` | Сбросить стейт. Последующий `get()` вернёт `None`. ||
|#

Изменения возвращённого сообщения сохраняются только после `set()`. Это отличается от [внутреннего стейта с живыми значениями](../../../flow/python/internal-state.md#in-place). Запись пустого protobuf-сообщения сохраняет его как значение, а `clear()` сбрасывает стейт. Для одного ключа действует последний явный вызов `set()` или `clear()`. Присоединённый стейт доступен только для чтения: `set()` и `clear()` выбрасывают `ReadOnlyExternalStateError`, а изменение возвращённого снапшота не влияет на стейт.

Неизвестное имя стейта, отсутствие ключа, неверный формат или несовпадение protobuf-типа приводят к `ValueError`. Если передать экземпляр сообщения вместо класса или значение неверного типа в `set()`, возникнет `TypeError`. Повреждённые сохранённые сообщения, сообщения без обязательных полей и ошибки сериализации приводят к `ValueError`. Ошибка сериализации сохраняет предыдущее значение стейта.

См. [реализацию аксессора]({{source-root}}/yt/yt/flow/library/python/companion/context.py) и [тесты внешнего protobuf-стейта]({{source-root}}/yt/yt/flow/library/python/companion/test/test_proto_external_state.py).

{% if audience == "internal" %}

### Настройка профильного стейта {#profile-state-setup}

Укажите `NYT::NFlow::NProfileState::TProfileStateManager` в `external_state_managers` или `NYT::NFlow::NProfileState::TProfileStateJoiner` в `external_state_joiners`. Бинарный файл воркера должен быть слинкован с библиотекой менеджера профильного стейта. Перед запуском создайте и смонтируйте таблицу с совместимой раскладкой аннотированного protobuf-сообщения; раскладка должна содержать корректную protobuf-привязку. Joiner профилей с переопределённой схемой ключа не поддерживает компаньоны.

Полная замена сообщения должна соответствовать раскладке: заданные в сообщении поля ключа должны совпадать с ключом стейта, а непривязанные поля верхнего уровня отклоняются. Согласуйте изменения схемы между читателями и писателями, поскольку замена очищает отсутствующие поля.

При вызове `Pipeline.add(...)` или `Pipeline.computation(...)` передайте соответствие имён стейтов классам сообщений через `proto_external_states` или `joined_proto_external_states`. Когда `Pipeline.run()` запускает пайплайн на хосте, runner заполняет `parameters.descriptor_layout.message_name` и самодостаточный `descriptor_set`, если источник дескрипторов отсутствует. Явно заданные `layout` и `descriptor_set` имеют приоритет. Если задан другой `message_name`, его разрешает пул дескрипторов воркера. Эти объявления не создают менеджер, joiner или таблицу: соответствующая запись уже должна быть в спеке.

Используйте настоящий [Python launcher]({{source-root}}/yt/yt/flow/yandex/extensions/profile_state/examples/python/wait_click_join_profile/__main__.py) вместе с его [спекой пайплайна]({{source-root}}/yt/yt/flow/yandex/extensions/profile_state/examples/python/wait_click_join_profile/pipeline.yson). Аннотации, поддерживаемые типы полей, схема таблицы, настройка кодеков и ограничения миграции описаны в [руководстве по профильному стейту]({{source-root}}/yt/yt/flow/yandex/extensions/profile_state/README.md). Выбор источника дескрипторов покрыт [тестами runner-а]({{source-root}}/yt/yt/flow/library/python/runner/tests/test_profile_state.py).

{% endif %}

## Конфигурация строкового стейта в статической спеке {#static-spec}

Для External State в формате строк таблицы объявите external state manager в секции `external_state_managers` [компьютейшена](../../../flow/concepts/glossary.md#stream-and-computation) в статической спеке:

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
- `external_state_manager_class_name` — имя зарегистрированного класса external state manager. Для стейта в формате строк таблицы используйте `"NYT::NFlow::TSimpleExternalStateManager"`.{% if audience == "internal" %} Для protobuf-профилей см. [настройку профильного стейта](#profile-state-setup).{% endif %} Подробнее см. в [C++ документации](../../../flow/cpp/state.md#external-state).
- `parameters.path` — путь к динамической таблице {{product-name}}, в которой хранится стейт.

## Read-only join {#external-state-joiner}

External State Joiner загружает строку, ключ которой совпадает с ключом текущего сообщения или таймера. Объявите `NYT::NFlow::TSimpleExternalStateJoiner` в `external_state_joiners`; имя стейта должно начинаться с `/` и совпадать с аргументом `ctx.joined_external_state`. См. спеки запускаемых примеров [External State Join]({{source-root}}/yt/yt/flow/examples/python/external_state_join/pipeline.yson) и [Static Table Join]({{source-root}}/yt/yt/flow/examples/python/static_table_join/pipeline.yson).

Аксессор поддерживает те же операции чтения, что и `ExternalStateAccessor`, но `set()` и `clear()` выбрасывают `ReadOnlyExternalStateError`. Если строка не найдена, возвращается пустой payload со схемой присоединённой таблицы, поэтому `get("column")` вернёт `None`; обработчики в примерах не создают для такого сообщения выходную запись. Неизвестное имя joiner-а приводит к `ValueError`.

{% code '/yt/yt/flow/examples/python/external_state_join/__main__.py' lang='python' lines='[BEGIN lookup_join]-[END lookup_join]' %}

Joiner использует отдельный TTL-кэш в каждой джобе. По умолчанию кэш отключён (`ttl = 0`), и в следующей эпохе ключи загружаются заново. Чтобы повторно использовать строки до истечения TTL, настройте `dynamic_spec.computations.<computation>.external_state_joiners.<name>.parameters.cache`. Присоединённые строки загружаются как неизменённый стейт и не отправляются воркеру как обновления.

См. [реализацию joiner-а]({{source-root}}/yt/yt/flow/library/cpp/computation/simple_external_state_manager.cpp), [реализацию TTL-кэша]({{source-root}}/yt/yt/flow/library/cpp/common/state_cache.cpp), [преобразование присоединённого стейта в Python]({{source-root}}/yt/yt/flow/library/python/companion/proto_mapper.py), [хранилище стейта]({{source-root}}/yt/yt/flow/library/python/companion/state.py) и [read-only-аксессор]({{source-root}}/yt/yt/flow/library/python/companion/context.py).

## Создание таблицы для стейта {#creating-table}

Таблица для External State должна быть создана заранее. Схема ключевых колонок таблицы должна совпадать с `group_by_schema` компьютейшена.

{% if audience == "internal" %}Для создания таблицы рекомендуется использовать [YtSync]({{yt-sync-docs}}/).{% endif %}

## Полный пример — EventReducer из Shuffle {#example}

Пример из [shuffle]({{source-root}}/yt/yt/flow/examples/python/shuffle):

{% code '/yt/yt/flow/examples/python/shuffle/event_reducer.py' lang='python' lines='[BEGIN event_reducer]-[END event_reducer]' %}

[Полный исходный код]({{source-root}}/yt/yt/flow/examples/python/shuffle/event_reducer.py)

Паттерн работы:
1. Получить текущее состояние через `ctx.external_state(...)`.
2. Создать builder через `state.to_builder()`.
3. Обновить нужные поля через `builder.set(...)`.
4. Сохранить через `state.set(builder.finish())`.

## См. также {#see-also}

- [StateAccessor (Python)](../../../flow/python/state-accessor.md)
- [Internal State (Python)](../../../flow/python/internal-state.md)
- [Работа со стейтами (Python)](../../../flow/python/state.md) — краткий обзор
- [Stateful processing (концепция)](../../../flow/concepts/stateful.md)
- [External State (Java)](../../../flow/java/external-state.md)
- [Пример Shuffle](../../../flow/python/examples/shuffle.md)
- [Пример Static Table Join](../../../flow/python/examples/static_table_join.md)
- [Пример External State Join](../../../flow/python/examples/external_state_join.md)
