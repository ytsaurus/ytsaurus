# Работа со стейтами в {{product-name}} Flow (C++)

{% note info %}

На этой странице описаны особенности работы со стейтами на C++. Языконезависимое описание концепции см. в разделе [Stateful processing](../../../flow/concepts/stateful.md).

{% endnote %}

## Internal State {#internal-state}

Простейший способ хранить стейт внутри `Computation`. Данные автоматически подгружаются в начале [эпохи](../../../flow/concepts/glossary.md#epoch) и записываются при коммите. Не требует самостоятельного создания таблиц — Flow управляет ими автоматически.

### Использование

Для работы с Internal State необходимо:

В качестве типа стейта можно использовать **любой тип**, для которого определены функции сериализации и десериализации. По умолчанию поддерживаются все типы, для которых определены стандартные функции `Serialize`/`Deserialize` в YT (в том числе `TYsonStruct`-наследники); для произвольного типа достаточно предоставить их перегрузки.

По умолчанию стейт считается пустым, если он равен значению по умолчанию (`TMyState{}`), а очистка пересоздаёт его. Чтобы переопределить это поведение, тип стейта может унаследовать опциональные миксины из `NYT::NFlow`:

- `ICustomStateOps` — собственная логика `Clear()` и `IsEmpty()` (задаются парой);
- `ICustomYsonView` — собственное представление `ToYsonView()` для read-state-интроспекции.

Необходимо:

1. Завести поле `TMutableStateKeyClient<TMyState> MyStateClient_` в своём `TComputation`.
2. Переопределить `DoInit(IJobInitContextPtr initContext)` и вызвать в нём инициализацию `initContext->InitClient<TMyState>(MyStateClient_, "my_state")`. В качестве имени стоит взять уникальную в рамках `Computation` строку.
3. Для получения стейта по ключу использовать `MyStateClient_.GetState(message->Key)`. Возвращаемый аксессор `TStateAccessor<TMyState>` ведёт себя как умный указатель на `TMyState` (`state->...`, `*state`) и действителен только в пределах текущей эпохи — сохранять его в полях нельзя.
4. Для очистки стейта (удаления строки из таблицы) вызвать `state.Clear()` на аксессоре.

Если стейт пуст, соответствующая строчка будет удалена.

Пример:

```cpp
struct TMyState
    : public NYTree::TYsonStruct
{
    std::optional<ui64> SomeValue;

    REGISTER_YSON_STRUCT(TMyState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("some_value", &TThis::SomeValue)
            .Default();
    }
};

class TMyComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitClient(MyStateClient_, "my_state");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr output) override
    {
        auto state = MyStateClient_.GetState(message->Key);
        state->SomeValue = 42;
        // ...
    }

    void DoProcessTimer(
        const TTimer& timer,
        IOutputCollectorPtr output) override
    {
        auto state = MyStateClient_.GetState(timer.Key);
        // Очистить стейт (удалить строку из таблицы):
        state.Clear();
    }

private:
    TMutableStateKeyClient<TMyState> MyStateClient_;
};
```

### Сжатие

Internal State поддерживает сжатие данных. Оно настраивается в [DynamicSpec](../../../flow/concepts/spec.md) для каждого стейта по отдельности — в секции `state_manager` компьютейшена, в `overrides/<имя_стейта>/format`:

- `compress` — включить сжатие (по умолчанию `false`);
- `recode_probability` — вероятность перекодировать стейт в заданный формат при очередной обработке; обеспечивает постепенную миграцию стейтов после смены формата (по умолчанию `0.1`).

## Yson State Reader

{% note warning %}

Функциональность ещё не реализована.

{% endnote %}

## External State {#external-state}

External State — стейт, хранимый в пользовательской динамической таблице {{product-name}}. В отличие от Internal State, таблицы создаются и управляются пользователем{% if audience == "internal" %} (например, через [YtSync]({{yt-sync-docs}}/)){% endif %}.

External state manager объявляется в спеке `Computation` под уникальным именем в секции `external_state_managers` и подключается к `Computation` через типизированный клиент `TMutableStateKeyClient<TState>`. Имена в спеке должны начинаться с `/` (например, `"/state"`) и совпадают с именем, передаваемым в `InitExternalStateClient`. Реализация ищется в реестре по `external_state_manager_class_name`.

### Использование

Для работы с External State необходимо:

1. Завести поле `TMutableStateKeyClient<TState> StateClient_` в своём `TComputation`, где `TState` — тип стейта, который возвращает соответствующий external state manager (см. ниже про конкретные реализации).
2. Переопределить `DoInit(IJobInitContextPtr initContext)` и вызвать в нём `initContext->InitExternalStateClient(StateClient_, "/state")`. В качестве имени стоит взять уникальную в рамках `Computation` строку — это же имя должно фигурировать в спеке.
3. Для получения стейта по ключу использовать `StateClient_.GetState(message->Key)`. Возвращаемый аксессор `TStateAccessor<TState>` ведёт себя как умный указатель на `TState` и действителен только в пределах текущей эпохи.

Пример:

```cpp
class TMyComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr /*output*/) override
    {
        auto state = StateClient_.GetState(message->Key);
        i64 count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);
        TPayloadBuilder builder(state->Schema);
        builder.Set(count + 1, "count");
        state->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};
```

Спека `Computation` с подключённым `TSimpleExternalStateManager`:

```yson
"external_state_managers" = {
    "/state" = {
        "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
        "parameters" = {
            "path" = "//path/to/state/table";
        };
    };
};
```

### TSimpleExternalStateManager

`TSimpleExternalStateManager` — стандартная реализация external state manager'а. Работает с одной динамической таблицей, ключи которой совпадают с `group_by_schema`. `GetState` отдаёт аксессор поверх `TSimpleExternalState` с полями `Payload` и `Schema`; колонки достаются и записываются через `GetColumn[Value]<T>` / `TPayloadBuilder` по имени или индексу. Кэширование стейтов происходит автоматически через общий [StateCache](#state-cache).

Регистрировать `TSimpleExternalStateManager` в `register.cpp` не нужно: он уже зарегистрирован в самой библиотеке Flow.

Спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TSimpleExternalStateManagerSpec.md) %}

Динамическая спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicSimpleExternalStateManagerSpec.md) %}

Полный пример использования `TSimpleExternalStateManager` см. в разборе примера [word_count](../../../flow/cpp/examples/word_count.md).

{% if audience == "internal" %}

## NBigRTExtensions::TProfileManager {#profile-manager}

`TProfileManager<TProfile>` — external state manager, совместимый с профилями BigRT. Позволяет переиспользовать существующие профили BigRT в [пайплайнах](../../../flow/concepts/glossary.md#pipeline) Flow.

В отличие от `TSimpleExternalStateManager`, `TProfileManager` параметризован пользовательским типом профиля и поэтому должен быть зарегистрирован пользователем. В клиенте удобно использовать вспомогательный тип состояния `TProfileManagerState<TMyProfile>`, который позволяет указать только сам профиль (без алиаса менеджера). Для краткости рядом с типом состояния есть алиас клиента `NBigRTExtensions::TProfileMutableStateKeyClient<TMyProfile>` — полностью эквивалентен `TMutableStateKeyClient<NBigRTExtensions::TProfileManagerState<TMyProfile>>`. Пример:

```cpp
// header: подключаем клиента с типом стейта, привязанным к профилю.
class TMyComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    // ...

private:
    // Эквивалентно TMutableStateKeyClient<NBigRTExtensions::TProfileManagerState<TMyProfile>>.
    NBigRTExtensions::TProfileMutableStateKeyClient<TMyProfile> StateClient_;
};
```

```cpp
// register.cpp: регистрируем computation и сам менеджер.
YT_FLOW_DEFINE_COMPUTATION(TMyComputation);
YT_FLOW_DEFINE_EXTERNAL_STATE_MANAGER(NYT::NFlow::NBigRTExtensions::TProfileManager<TMyProfile>);
```

Спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NBigRTExtensions_TProfileManagerSpec.md) %}

Динамическая спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NBigRTExtensions_TDynamicProfileManagerSpec.md) %}

В спеке `Computation` имя класса задаётся `external_state_manager_class_name` и должно быть полностью квалифицированным именем того типа, который был передан в `YT_FLOW_DEFINE_EXTERNAL_STATE_MANAGER` (например, `NYT::NFlow::NBigRTExtensions::TProfileManager<NMyNamespace::TMyProfile>`).

Подробнее об этом расширении см. в разделе [Serializable Profile](../../../yandex-specific/flow/extensions/serializable-profile.md). О юнит-тестировании функций со стейтом Serializable Profile — в разделе [Тестирование](../../../flow/cpp/process-functions.md#profile-testing).

{% endif %}

## External State Joiner {#external-state-joiner}

External State Joiner предоставляет доступ только на чтение к внешним стейтам через join по ключу. Загруженные стейты кэшируются в общем StateCache с TTL: повторные обращения к тому же ключу до истечения TTL обслуживаются из кэша без обращения к YT.

External state joiner объявляется в спеке `Computation` под уникальным именем в секции `external_state_joiners` и подключается к `Computation` через типизированный клиент `TJoinedStateKeyClient<TState>`. Имена в спеке должны начинаться с `/` (например, `"/reference"`) и совпадать с именем, передаваемым в `InitExternalStateClient`. Реализация ищется в реестре по `external_state_joiner_class_name`.

### Использование

Аналогично external state manager'у:

```cpp
class TMyComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(StateReaderClient_, "/reference");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr /*output*/) override
    {
        auto state = StateReaderClient_.GetState(message->Key);
        // state — read-only аксессор TConstStateAccessor<TSimpleExternalState> (действителен в пределах эпохи).
    }

private:
    TJoinedStateKeyClient<TSimpleExternalState> StateReaderClient_;
};
```

Спека `Computation` с подключённым `TSimpleExternalStateJoiner`:

```yson
"external_state_joiners" = {
    "/reference" = {
        "external_state_joiner_class_name" = "NYT::NFlow::TSimpleExternalStateJoiner";
        "parameters" = {
            "path" = "//path/to/reference/table";
        };
    };
};
```

### TSimpleExternalStateJoiner {#simple-external-state-joiner}

`TSimpleExternalStateJoiner` — стандартная реализация external state joiner'а. По интерфейсу аналогична `TSimpleExternalStateManager`: читает строки одной динамической таблицы, ключи которой совпадают с `group_by_schema`. `GetState` отдаёт read-only аксессор поверх `TSimpleExternalState`, который содержит `TPayload` и схему value-колонок таблицы; колонки достаются через `GetColumn[Value]<T>` по имени или индексу.

Регистрировать `TSimpleExternalStateJoiner` в `register.cpp` не нужно: он уже зарегистрирован в самой библиотеке Flow.

Спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TSimpleExternalStateJoinerSpec.md) %}

Динамическая спека (TTL кэша задаётся через секцию `cache`):

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicSimpleExternalStateJoinerSpec.md) %}

{% if audience == "internal" %}

### NBigRTExtensions::TProfileJoiner {#profile-joiner}

`TProfileJoiner<TProfile>` — read-only joiner, совместимый с профилями BigRT. Регистрируется пользователем через `YT_FLOW_DEFINE_EXTERNAL_STATE_JOINER`:

```cpp
YT_FLOW_DEFINE_EXTERNAL_STATE_JOINER(NYT::NFlow::NBigRTExtensions::TProfileJoiner<TMyProfile>);
```

В `Computation` используется тот же вспомогательный тип состояния `TProfileManagerState<TMyProfile>`, что и для менеджера: `TJoinedStateKeyClient<NBigRTExtensions::TProfileManagerState<TMyProfile>>`. Сокращённый алиас — `NBigRTExtensions::TProfileJoinedStateKeyClient<TMyProfile>`; в коде пользователя удобнее использовать именно его.

Спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NBigRTExtensions_TProfileJoinerSpec.md) %}

Динамическая спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NBigRTExtensions_TDynamicProfileJoinerSpec.md) %}

Подробнее об этом расширении см. в разделе [Serializable Profile](../../../yandex-specific/flow/extensions/serializable-profile.md). О юнит-тестировании функций со стейтом Serializable Profile — в разделе [Тестирование](../../../flow/cpp/process-functions.md#profile-testing).

{% endif %}

## State Joiner {#state-joiner}

State Joiner предоставляет доступ только на чтение к **внутреннему** стейту другого `Computation`'а: один `Computation` может обогащаться стейтом, который накапливает другой `Computation`, в том числе когда ключ join'а не совпадает с собственной `group_by_schema`.

Joiner объявляется в спеке `Computation` под уникальным именем в секции `state_joiners` и подключается через типизированный клиент `TJoinedStateKeyClient<TState>`. Имена в спеке должны начинаться с `/` и совпадать с именем, передаваемым в `InitClient`. Отдельной реализации или регистрации не требуется — это встроенная возможность Flow.

В спеке указывается:

- `computation_id` — `Computation`, чей внутренний стейт читается;
- `state_name` — имя стейт-клиента целевого `Computation`'а (тот префикс, который он передал в `InitClient`, начинается с `/`);
- `join_on/key_schema_override` — какими колонками текущей строки задаётся ключ. Если не задана, используется собственная `group_by_schema` (join по тому же ключу);
- `join_on/key_provider_streams` — стримы, из сообщений и таймеров которых берутся ключи (`nullopt` — все входные стримы);
- `auto_preload` — если `true` (по умолчанию), фреймворк сам подгружает ключи перед каждым `DoProcess`; иначе `Computation` вызывает `PreloadKeyStates` сам.

{% note warning %}

Типы ключа проверяются на старте: `key_schema_override` (или собственная `group_by_schema`, если override не задан) должна по числу колонок и типам совпадать с `group_by_schema` целевого `Computation`'а — joiner вычисляет полный ключ целевого стейта. Имена колонок и выражения при этом могут отличаться.

На стороне пользователя остаётся (фреймворк это не проверяет):

- *соответствие значений* — какая колонка во что мапится, чтобы вычисленный ключ реально указывал на нужную строку чужого стейта;
- `TState` должен совпадать с типом стейта целевого `Computation`'а: joiner десериализует его строки так же, как `TMutableStateKeyClient<TState>` на стороне-владельце.

{% endnote %}

### Использование

```cpp
class TMyComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitClient(UpstreamClient_, "/upstream");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr /*output*/) override
    {
        auto state = UpstreamClient_.GetState(message);
        // state — read-only аксессор TConstStateAccessor<TUpstreamState> (действителен в пределах эпохи).
    }

private:
    TJoinedStateKeyClient<TUpstreamState> UpstreamClient_;
};
```

Спека `Computation` с подключённым joiner'ом по ключу, отличному от собственной `group_by_schema`:

```yson
"state_joiners" = {
    "/upstream" = {
        "computation_id" = "accumulator";
        "state_name" = "/total";
        "join_on" = {
            "key_schema_override" = [
                {name = "Hash"; expression = "farm_hash(UserId)"; type = "uint64"; required = %true;};
                {name = "UserId"; type = "string"; required = %true;};
            ];
        };
    };
};
```

По умолчанию кэш выключен (`ttl = 0`): joiner заново читает стейт целевого `Computation`'а на `SyncLastCommittedTimestamp` каждую эпоху, поэтому всегда видит свежие коммиты. Кэш с TTL включается в dynamic spec через секцию `cache` (загруженные стейты живут до истечения `ttl`, повторные чтения того же ключа обслуживаются без обращения к YT). Для `Swift`-вычислений с требованием строгой детерминированности кэш и чтение на `SyncLastCommittedTimestamp` нужно учитывать.

Спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TStateJoinerSpec.md) %}

Динамическая спека (TTL кэша задаётся через секцию `cache`):

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicStateJoinerSpec.md) %}

## StateCache {#state-cache}

Flow предоставляет общий двухуровневый (uncompressed + compressed) LRU кэш для стейтов. Конфигурирование осуществляется через `/dynamic_spec/job_tracker/state_cache`. Он используется в первую очередь для стейтов менеджеров; у joiner'ов кэш по умолчанию выключен, а данные грузятся на `SyncLastCommittedTimestamp`.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicStateCacheSpec.md) %}

Подробнее о согласованности `group_by_schema` и общих принципах работы со стейтами см. [Stateful processing](../../../flow/concepts/stateful.md).

## См. также

- [Stateful processing (концепция)](../../../flow/concepts/stateful.md)
- [Computation (C++)](../../../flow/cpp/computation.md)
- [Быстрый старт (C++)](../../../flow/cpp/getting-started.md)
