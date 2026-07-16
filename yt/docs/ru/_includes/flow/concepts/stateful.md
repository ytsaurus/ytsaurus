# Stateful processing в {{product-name}} Flow

Stateful обработка — это обработка событий, включающая в себя read-modify-write работу со стейтами, хранящимися в {{product-name}}. Простейший пример: подсчёт статистик по входящим событиям в разрезе по [ключам](../../../flow/concepts/glossary.md#key) — нужно загрузить старое значение, обновить и записать обратно.

## Модель работы со стейтом {#model}

Доступ к стейту внутри [компьютейшена](../../../flow/concepts/glossary.md#stream-and-computation) строится из трёх сущностей:

- **Стейт (state)** — пользовательские данные, привязанные к [ключу](../../../flow/concepts/glossary.md#key) и хранящиеся в динамической таблице {{product-name}}.
- **Клиент стейта (state client)** — типизированный по типу стейта объект (`Client<TState>`), который компьютейшен заводит по одному на каждый именованный стейт и инициализирует в `DoInit`. Через клиент происходит доступ к стейту по ключу. Бывает **read-write** (работает и с [внутренним](#internal-state), и с [внешним](#external-state) стейтом) и **read-only** ([джойнер](#external-state-joiner) к внешнему стейту).
- **Аксессор (state accessor)** — то, что клиент отдаёт для конкретного ключа (по входному сообщению, таймеру или явному ключу): представление стейта того же типа `TState`. Аксессор ведёт себя как умный указатель на стейт: read-write-аксессор позволяет читать, изменять и очищать стейт, read-only — только читать.

{% note warning %}

Аксессор действителен только в пределах текущей [эпохи](../../../flow/concepts/glossary.md#epoch). Его нельзя сохранять в полях компьютейшена или переиспользовать между эпохами — на каждой эпохе стейт нужно получать через клиент заново.

{% endnote %}

## Типы стейтов {#state-types}

### Internal State {#internal-state}

Простейший способ работы со стейтом: таблицы создавать не нужно — Flow управляет ими автоматически. Данные подгружаются в начале [эпохи](../../../flow/concepts/glossary.md#epoch) и записываются при коммите. Доступ read-write — тем же клиентом, что и для внешнего стейта. Тип стейта может быть произвольным; единственное требование — сериализуемость в YSON (для сохранения между эпохами).

### External State {#external-state}

Стейт в пользовательской динамической таблице. Таблицы создаются и управляются пользователем{% if audience == "internal" %} (например, через [YtSync]({{yt-sync-docs}}/)){% endif %}. Доступ read-write — тем же клиентом, что и для внутреннего стейта, но бэкендом выступает **state manager** (`TSimpleExternalStateManager`{% if audience == "internal" %}, `NBigRTExtensions::TProfileManager`{% endif %}). Объявляется в спеке компьютейшена в top-level секции `external_state_managers`, реализация резолвится по `external_state_manager_class_name`. Поддерживает кэширование.

### External State Joiner {#external-state-joiner}

Read-only доступ к внешним стейтам через join по ключу — через **joiner** (`TSimpleExternalStateJoiner`{% if audience == "internal" %}, `NBigRTExtensions::TProfileJoiner`{% endif %}). Объявляется в спеке компьютейшена в top-level секции `external_state_joiners` (на одном уровне с `external_state_managers`), реализация резолвится по `external_state_joiner_class_name`. Поддерживает кэширование с TTL: загруженные стейты живут в общем [StateCache](#state-cache) и переподгружаются из YT только после истечения TTL либо вытеснения из кэша.

{% if audience == "internal" %}

{% note info %}

Для часто обновляемых protobuf-профилей у External State и External State Joiner есть специализированное расширение — [Serializable Profile](../../../yandex-specific/flow/extensions/serializable-profile.md). Оно даёт пару `NBigRTExtensions::TProfileManager` (read-write) и `NBigRTExtensions::TProfileJoiner` (read-only), параметризованных пользовательским типом профиля, с дельта-кодированием изменений и сжатием поверх обычной таблицы стейтов — экономит место и трафик по сравнению с записью полного профиля на каждое обновление.

{% endnote %}

{% endif %}

{% note warning %}

Таблицы, к которым даёт доступ read-write state manager, должны модифицироваться исключительно через него (или при [остановленном](../../../flow/concepts/glossary.md#start-stop-pause-pipeline) [пайплайне](../../../flow/concepts/glossary.md#pipeline)), так как он может использовать кэши.

{% endnote %}

{% note warning "Одна таблица — один писатель" %}

Писать в таблицу внешнего стейта должен ровно один компьютейшен: записи из разных [партиций](../../../flow/concepts/glossary.md#partition) и транзакций нарушают консистентность стейта. State manager владеет своей таблицей на запись: `TSimpleExternalStateManager`{% if audience == "internal" %}, как и `NBigRTExtensions::TProfileManager`,{% endif %} объявляет её как принадлежащую ему. Read-only доступ к стейту другого компьютейшена нужно делать через [external state joiner](#external-state-joiner) (`TSimpleExternalStateJoiner`{% if audience == "internal" %}, `NBigRTExtensions::TProfileJoiner`{% endif %}) либо сообщениями компьютейшену-писателю — джойнеры таблицу не захватывают. Это проверяется валидацией спеки по владению на запись: на каждую таблицу стейта должен приходиться ровно один писатель-владелец, а пайплайн, где одну таблицу захватывают на запись два менеджера, отклоняется с ошибкой `State table <путь> is claimed for writing by both ...`. Read-only потребители (джойнеры) таблицу на запись не захватывают, поэтому могут разделять её с писателем-владельцем.

{% endnote %}

{% note info %}

Для любого стейта пустое значение соответствует отсутствию строки в таблице. Если после модификации стейт пуст, соответствующая строка удаляется. По умолчанию пустота и очистка стейта определяются автоматически (сравнение со значением по умолчанию); тип стейта может переопределить это поведение.

{% endnote %}

## Хранение стейтов {#storage}

Стейты хранятся в динамических таблицах {{product-name}}. Простейший пример схемы:

#|
|| **name** | **type** | **sort_order** | **expression** ||
|| `hash` | `uint64` | `ascending` | `farm_hash(my_key)` ||
|| `my_key` | `string` | `ascending` | ||
|| `my_value_1` | `string` | | ||
|| `my_value_2` | `string` | | ||
|#

### Согласованность group_by_schema {#group-by-schema}

Из соображений корректности и производительности строго рекомендуется для [Computation](../../../flow/concepts/glossary.md#stream-and-computation) в качестве [group_by_schema](../../../flow/concepts/spec.md#computation) использовать схему из первых ключевых колонок динтаблицы со стейтами (строго префикс ключевых колонок). Тогда:
- с событиями по одному ключу будет работать только одна [партиция](../../../flow/concepts/glossary.md#partition) (корректность);
- одна партиция будет работать с ограниченным числом таблетов (производительность).

Пример `group_by_schema`, согласованного со схемой таблицы стейтов из примера выше:

#|
|| **name** | **type** | **expression** ||
|| `hash` | `uint64` | `farm_hash(my_key)` ||
|| `my_key` | `string` | ||
|#

## StateCache {#state-cache}

Flow предоставляет общий двухуровневый (uncompressed + compressed) LRU кэш для стейтов. Конфигурирование: `/dynamic_spec/job_tracker/state_cache`.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicStateCacheSpec.md) %}

## Реализация на разных языках

- **C++**: клиент `TMutableStateKeyClient<TState>` (read-write) или `TJoinedStateKeyClient<TState>` (read-only) отдаёт аксессор `TStateAccessor<TState>` / `TConstStateAccessor<TState>`; один и тот же key-client работает и с внутренним, и с внешним стейтом. [Подробнее →](../../../flow/cpp/state.md)
- **Java**: YsonStateAccessor, ProtoStateAccessor, ExternalStateAccessor. [Подробнее →](../../../flow/java/state.md)
- **Python**: ctx.state(), ctx.external_state(), ctx.proto_state(). [Подробнее →](../../../flow/python/state.md)

## См. также

- [Работа со стейтами (C++)](../../../flow/cpp/state.md)
- [Работа со стейтами (Java)](../../../flow/java/state.md)
- [Работа со стейтами (Python)](../../../flow/python/state.md)
