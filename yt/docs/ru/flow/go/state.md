# Работа со стейтами в {{product-name}} Flow (Go)

{% note info %}

Данная страница описывает Go API для работы со стейтами. Общие концепции стейтов описаны в разделе [Stateful-вычисления](../../flow/concepts/stateful.md).

{% endnote %}

Аксессор [стейта](../../flow/concepts/glossary.md#state) открывается свободной функцией `flow.OpenXxxState(rt, name, input)` с тремя аргументами:

- `rt` — `flow.Runtime`, второй аргумент любого обработчика (см. [Process Function](getting-started.md#process-function)).
- `name` — имя стейта, объявленное в [спеке](../../flow/concepts/glossary.md#spec-and-dynamic-spec) компьютейшена.
- `input` — вход, к [ключу](../../flow/concepts/glossary.md#key) которого привязывается аксессор: `flow.ExtendedMessage`, `flow.Timer` или `flow.Visit`. Все три реализуют интерфейс `flow.Input`, поэтому работа со стейтом в обработчике сообщения, таймера и визита выглядит одинаково.

Функции открытия — свободные, а не методы `Runtime`: YSON- и Proto-аксессоры параметризуются типом стейта, а методы в Go собственных типовых параметров не имеют.

Аксессор адресует стейт ровно одного ключа и живёт в пределах одного запроса. Воркеру уезжают только те записи, в которые компьютейшен писал: чтение стейта не отправляет его обратно.

## YSON State {#yson-state}

Самый простой способ работы со стейтом — YSON-формат. Стейт описывается обычной Go-структурой с тегами `yson`:

```go
state, err := flow.OpenYSONState[wordCountState](rt, "word-state", msg)
if err != nil {
    return err
}

if state.Empty() {
    state.Value().Word = word
}
state.Value().Count++
return nil
```

`flow.YSONState[T]` предоставляет `Empty()`, `Value() *T` и `Clear()`. `Value()` возвращает изменяемое значение, а для отсутствующего стейта создаёт zero value. Отдельный `Set` не нужен: после успешной обработки батча SDK сериализует изменения автоматически; при ошибке обработчика они отбрасываются.

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_state]-[END word_count_state]' %}

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

Стейт привязан к ключу сообщения, определяемому через `group_by_schema` в спеке компьютейшена. Для каждого уникального ключа хранится независимое значение.

## Raw State {#raw-state}

Для хранения стейта в виде сырых байтов:

```go
state, err := flow.OpenRawState(rt, "raw-state", msg)
```

Возвращает `flow.RawStateAccessor` с методами:

- `Get() ([]byte, bool)` — получить значение и признак его наличия.
- `Or(fallback []byte) []byte` — получить значение или `fallback`.
- `Set(data []byte) error` — сохранить значение.
- `Clear() error` — удалить стейт.

YSON- и Proto-аксессоры — обёртки над сырым: сам `RawStateAccessor` нужен, когда сериализацию компьютейшен выполняет самостоятельно.

## Proto State {#proto-state}

Для хранения стейта в виде Protobuf-сообщения:

```go
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", msg)
```

Тип стейта называется по значению (`TJoinState`), а аксессор работает с указателем на него (`*TJoinState`) — именно на указателе сгенерированный код реализует `proto.Message`.

Возвращает `flow.ProtoStateAccessor[T, PT]` с методами:

- `Get() (PT, bool, error)` — десериализовать и вернуть сообщение.
- `Or(fallback PT) (PT, error)` — вернуть сохранённое сообщение или `fallback`.
- `Set(value PT) error` — сериализовать и сохранить.
- `Clear() error` — удалить стейт.

```go
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", msg)
if err != nil {
    return err
}

window, err := state.Or(&TJoinState{})
if err != nil {
    return err
}
window.HitPayload = payload

return state.Set(window)
```

## External State {#external-state}

Внешний стейт — строка пользовательской динамической таблицы. В пользовательском коде она представляется обычной Go-структурой с тегами `yson`:

```go
state, err := flow.OpenExternalState(rt, "/shuffle-state", msg)
```

Имя стейта — абсолютный путь, совпадающий с ключом в секции `external_state_managers` статической спеки. Имя без ведущего `/` отвергается ошибкой `flow.ErrInvalidStateName`, необъявленное имя — ошибкой `flow.ErrUnknownState`.

Возвращает `flow.ExternalStateAccessor`. Основные операции:

- `ConvertTo(&value) (bool, error)` — заполнить структуру сохранённой строкой; `bool` отличает отсутствующую строку.
- `ConvertFrom(&value) error` — сохранить поля структуры в строку стейта.
- `Clear() error` — удалить строку.

Низкоуровневые `Get`, `Or`, `Builder`, `Set` и `Schema` нужны только для динамических схем и поколоночной обработки.

Пример из [Shuffle](examples/shuffle.md):

{% code '/yt/yt/flow/examples/go/shuffle/event_reducer.go' lang='go' lines='[BEGIN event_reducer]-[END event_reducer]' %}

Паттерн работы с внешним стейтом:

1. Открыть стейт через `flow.OpenExternalState(...)`.
2. Преобразовать строку в структуру через `state.ConvertTo(&value)`.
3. Изменить поля структуры и сохранить её через `state.ConvertFrom(&value)`.

Подробнее — в разделе [External State (Go)](external-state.md).

## Joined External State {#joined-external-state}

Если внешний стейт нужен только на чтение — например, для обогащения сообщений справочником, — компьютейшен объявляет его в секции `external_state_joiners` и открывает отдельной функцией:

```go
reference, err := flow.OpenJoinedExternalState(rt, "/reference", msg)
```

Возвращает `flow.JoinedExternalStateAccessor`. Строка читается в структуру через `ConvertTo(&value)`; записи у аксессора нет по устройству, потому что заджойненный стейт заполняется из запроса и никогда не едет обратно. Низкоуровневые `Get`, `Or` и `Schema` доступны для динамических схем.

Пространства имён не пересекаются: стейт, которым компьютейшен владеет, недоступен через `flow.OpenJoinedExternalState`, и наоборот.

{% code '/yt/yt/flow/examples/go/external_state_join/lookup_join.go' lang='go' lines='[BEGIN lookup_join]-[END lookup_join]' %}

[Исходный код примера]({{source-root}}/yt/yt/flow/examples/go/external_state_join)

{% note warning %}

Воркер джойнит только те ключи, для которых нашёл строки. Батч, не совпавший ни с одним ключом справочника, приходит вообще без заджойненного стейта, и открытие возвращает `flow.ErrStateNotRead` — стейта с таким именем запрос просто не принёс. Это не ошибка обработки: её следует отличать через `errors.Is` и трактовать как отсутствие данных.

{% endnote %}

## Стейт в таймерах {#state-in-timers}

API работы со стейтом в обработчике [таймеров](../../flow/concepts/glossary.md#timer) идентичен — вместо сообщения передаётся таймер:

```go
state, err := flow.OpenExternalState(rt, "/join-state", timer)
```

Пример из [WaitClickJoin](examples/wait_click_join.md) — окно закрывается по таймеру, и стейт очищается сразу после публикации результата:

{% code '/yt/yt/flow/examples/go/wait_click_join/join_function.go' lang='go' lines='[BEGIN on_timer]-[END on_timer]' %}

Стейт, очищенный в этом запросе, дальше читается как отсутствующий: компьютейшен видит стейт таким, каким он станет после ответа воркеру.

## Привязка стейта к ключу {#group-by-schema}

В `TTransformCompanionComputation` стейт привязывается к [ключу](../../flow/concepts/glossary.md#key) сообщения, определяемому через `group_by_schema` в спеке компьютейшена. Все сообщения с одинаковым ключом разделяют один стейт. Ключ, к которому привязан аксессор, берётся из входа — поэтому один обработчик не может случайно записать стейт чужого ключа.

В `TTransformOrderedSourceCompanionComputation` поле `group_by_schema` не поддерживается. Ключом внутреннего стейта служит ключ партиции источника, поэтому все сообщения одной партиции разделяют стейт. Подробнее о выборе класса для SourceComputation см. в разделе [Computation (Go)](computation.md#sourcecomputation).

Подробнее о конфигурации ключей см. [Stateful-вычисления](../../flow/concepts/stateful.md).

Схема ключа доступна в обработчике через `rt.KeySchema()`, а сам ключ — через поле `Key` входа (`msg.Key`, `timer.Key`, `visit.Key`).

## Конфигурация стейтов в спеке {#spec-configuration}

Стейт, не объявленный в спеке, открыть нельзя: функция открытия вернёт `flow.ErrUnknownState` со списком объявленных имён. Объявляются стейты в трёх разных местах описания [компьютейшена](../../flow/concepts/glossary.md#stream-and-computation):

- Внутренние стейты (YSON, Raw, Proto) — списком имён в `parameters.internal_states`. Имена произвольные, без ведущего `/`.
- Внешние стейты, которыми компьютейшен владеет, — в секции `external_state_managers`. Ключ секции задаёт имя стейта (абсолютный путь), а поле `external_state_manager_class_name` — зарегистрированный класс менеджера; для типового сценария это `"NYT::NFlow::TSimpleExternalStateManager"`.
- Внешние стейты, которые компьютейшен только читает, — в секции `external_state_joiners`, рядом с `external_state_managers`.

```yson
"mapper" = {
    "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
    "external_state_managers" = {
        "/shuffle-state" = {
            "external_state_manager_class_name" = "NYT::NFlow::TSimpleExternalStateManager";
            "parameters" = {
                "path" = "<cluster=cluster_name>//path/to/state";
            };
        };
    };
    "external_state_joiners" = {
        "/reference" = {
            "external_state_joiner_class_name" = "NYT::NFlow::TSimpleExternalStateJoiner";
            "parameters" = {
                "path" = "//path/to/current";
            };
        };
    };
    "parameters" = {
        "internal_states" = ["word-state"];
    };
};
```

Таблицы внутренних стейтов создаёт и обслуживает Flow, таблицу внешнего стейта пользователь создаёт сам. Подробнее — в разделах [Internal State (Go)](internal-state.md) и [External State (Go)](external-state.md).

## См. также

- [State Accessor (Go)](state-accessor.md)
- [Internal State (Go)](internal-state.md)
- [External State (Go)](external-state.md)
- [Computation (Go)](computation.md)
- [Stateful-вычисления](../../flow/concepts/stateful.md)
