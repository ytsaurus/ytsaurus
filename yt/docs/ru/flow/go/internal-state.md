# Internal State в {{product-name}} Flow (Go)

Internal State — механизм работы с внутренним [состоянием (стейтом)](../../flow/concepts/glossary.md#state), хранящимся во внутренних таблицах Flow. В отличие от [External State](external-state.md), пользователю не нужно самостоятельно создавать таблицы — Flow управляет ими автоматически.

Подробнее про аксессоры и общие принципы работы со стейтом: [State Accessor (Go)](state-accessor.md).

Общие сведения о stateful-обработке описаны в разделе [Stateful processing](../../flow/concepts/stateful.md).

## Обзор {#overview}

Go SDK предоставляет три вида аксессоров для работы с Internal State, различающихся форматом сериализации. Для работы с внешним стейтом используются отдельные аксессоры — [External State (Go)](external-state.md).

| Аксессор | Формат | Открывается функцией |
|---|---|---|
| [YSONState](#yson-state) | YSON | `flow.OpenYSONState[T]` |
| [RawStateAccessor](#raw-state-accessor) | `[]byte` | `flow.OpenRawState` |
| [ProtoState](#proto-state) | Protobuf | `flow.OpenProtoState[T]` |

`RawStateAccessor` читает и записывает сырые байты явно через `Get`, `Set` и `Clear`. `YSONState` и `ProtoState` предоставляют изменяемое значение: изменения, сделанные в нём, сериализуются автоматически после успешного завершения обработчиков батча — см. [Изменение значения на месте](#in-place).

Каждая из функций `flow.OpenXxxState` принимает три аргумента:

- `rt` — `flow.Runtime` обработчика.
- `name` — имя стейта. Имена внутренних стейтов не начинаются с `/` и должны быть объявлены в `parameters.internal_states` [спеки](../../flow/concepts/glossary.md#spec-and-dynamic-spec) [компьютейшена](../../flow/concepts/glossary.md#stream-and-computation), см. [Конфигурация в статической спеке](#static-spec).
- `input` — вход, к [ключу](../../flow/concepts/glossary.md#key) которого привязывается стейт. Подходит любое значение, реализующее `flow.Input`: `flow.ExtendedMessage`, `flow.Timer`, `flow.Visit`.

{% note info %}

Аксессор показывает стейт таким, каким он станет после ответа воркеру: стейт, которого не было во входном запросе, и стейт, очищенный в этом же вызове через `Clear`, читаются одинаково — как отсутствующий.

{% endnote %}

## Изменение значения на месте {#in-place}

Значение, которое возвращают `Value()`, `Get()` и `Or()`, живое: для каждого ключа оно декодируется один раз за батч, все аксессоры этого ключа возвращают одно и то же значение, а изменения, сделанные в нём, записываются в стейт по окончании батча без вызова `Set()`. Если значение не изменилось, запись не выполняется: стейт, который сериализуется в те же байты, с которыми пришёл, обратно не уезжает. Дефолт из `Value()` и `Or()` становится значением стейта и записывается, как после `Set()`, поэтому его можно сразу изменять:

```go
state, err := flow.OpenYSONState[wordCountState](rt, "word-state", msg)
if err != nil {
    return err
}
state.Value().Count++
```

`ProtoState.Set()` по-прежнему заменяет сообщение целиком, `Clear()` удаляет стейт. Пустое Protobuf-сообщение сериализуется в ноль байт, а ноль байт — это отсутствие стейта: дефолт `Or(&T{})` не записывается, а сообщение, у которого сняли все поля, и `Set(&T{})` стейт удаляют. `RawStateAccessor` изменяемого значения не отдаёт: `Get()` возвращает копию байтов, и записываются они только через `Set()`. Стейты [External State](external-state.md) на месте тоже не отслеживаются.

У одного ключа за батч только одно декодированное значение, поэтому открытие того же стейта и ключа с другим Go-типом возвращает ошибку. Если обработчик вернул ошибку, изменения на месте не попадают в ответ воркеру. Стейт, который Go сериализует не в те байты, с которыми он пришёл, при первом же чтении перезаписывается канонической записью — один раз на ключ.

Чтобы отследить изменения, прочитанное значение перекодируется по окончании батча. Если компьютейшен только читает стейт, используйте `ReadOnly()`: такое представление возвращает то же значение, но не отслеживает его, а `ReadOnlyYSONState.Value()` и `ReadOnlyProtoState.Or()` не создают стейт. Методов записи у него нет, поэтому запись через него не компилируется. Значение общее с изменяемым стейтом того же ключа: если тот же стейт где-то открыт и на запись, изменения, сделанные через представление, всё же уедут воркеру.

```go
count := state.ReadOnly().Value().Count
```

## YSONState {#yson-state}

[Исходный код]({{source-root}}/yt/go/flow/context.go)

`YSONState[T]` хранит стейт как YSON-сериализованное значение типа `T`. Типом может быть любая структура с тегами `yson`, а также map, slice или скаляр — всё, что понимает `yson.Marshal`.

### Получение стейта {#getting-yson-state}

```go
// Для сообщения
state, err := flow.OpenYSONState[wordCountState](rt, "word-state", msg)

// Для таймера
state, err := flow.OpenYSONState[wordCountState](rt, "word-state", timer)
```

Десериализация выполняется при открытии. Повторное открытие того же стейта и ключа в пределах запроса возвращает то же изменяемое значение.

### Методы {#yson-methods}

| Метод | Возвращаемый тип | Описание |
|---|---|---|
| `Empty()` | `bool` | Проверить, отсутствует ли значение |
| `Get()` | `(*T, bool)` | Получить изменяемое значение. Второй результат отличает сохранённый стейт от отсутствующего |
| `Value()` | `*T` | Получить изменяемое значение; для отсутствующего стейта создаётся zero value |
| `Clear()` | — | Удалить значение |
| `ReadOnly()` | `ReadOnlyYSONState[T]` | Представление только на чтение |

Изменения значения сериализуются автоматически после успешного завершения всех обработчиков батча. Если обработчик вернул ошибку, изменения YSON-стейта не попадают в ответ воркеру.

### Пример из WordCount {#yson-example}

Тип, который пайплайн хранит для одного слова:

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_state]-[END word_count_state]' %}

Обработчик сообщения:

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

[Полный исходный код]({{source-root}}/yt/yt/flow/examples/go/word_count/word_count_mapper.go)

Здесь стейт привязан к ключу [сообщения](../../flow/concepts/glossary.md#message). Для нового ключа `Empty()` возвращает `true`, а `Value()` создаёт пустой `wordCountState`. Присваивания полям сохраняются без отдельного `Set`.

Тот же стейт открывается и в обработчике [таймера](../../flow/concepts/glossary.md#timer). Так устроен [URL Downloader](examples/url_downloader.md): `OnMessage` накапливает батч, а `OnTimer` читает его и очищает через `Clear`.

## RawStateAccessor {#raw-state-accessor}

[Исходный код]({{source-root}}/yt/go/flow/context.go)

`RawStateAccessor` работает с сырыми байтами без сериализации и десериализации. Это аксессор, поверх которого построены остальные два, — берите его, когда формат стейта определяете вы сами.

### Получение аксессора {#getting-raw-accessor}

```go
// Для сообщения
state, err := flow.OpenRawState(rt, "raw-state", msg)

// Для таймера
state, err := flow.OpenRawState(rt, "raw-state", timer)
```

### Методы {#raw-methods}

| Метод | Возвращаемый тип | Описание |
|---|---|---|
| `Get()` | `([]byte, bool)` | Получить сырые байты. Второй результат отличает сохранённый стейт от отсутствующего |
| `Or(fallback []byte)` | `[]byte` | Вернуть текущее значение или `fallback`, если стейта нет |
| `Set(data []byte)` | `error` | Сохранить сырые байты; пустые байты удаляют стейт |
| `Clear()` | `error` | Удалить стейт для текущего ключа |

Методы `Get` и `Or` не возвращают ошибку: десериализовать здесь нечего.

### Пример использования {#raw-example}

```go
state, err := flow.OpenRawState(rt, "raw-state", msg)
if err != nil {
    return err
}

// Чтение сырых данных
if data, ok := state.Get(); ok {
    // Обработка сырых данных...
    _ = data
}

// Запись сырых данных
if err := state.Set([]byte{0x01, 0x02, 0x03}); err != nil {
    return err
}

// Очистка
return state.Clear()
```

## ProtoState {#proto-state}

[Исходный код]({{source-root}}/yt/go/flow/context.go)

`ProtoState` сериализует стейт через Protobuf. Тип Protobuf-сообщения указывается в значимой форме, а стейт отдаёт указатель на него: `flow.OpenProtoState[TJoinState]` возвращает стейт над `*TJoinState`.

### Получение стейта {#getting-proto-accessor}

```go
// Для сообщения
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", msg)

// Для таймера
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", timer)
```

Десериализация выполняется при открытии. Повторное открытие того же стейта и ключа в пределах запроса возвращает то же изменяемое сообщение.

### Методы {#proto-methods}

| Метод | Возвращаемый тип | Описание |
|---|---|---|
| `Empty()` | `bool` | Проверить, отсутствует ли значение |
| `Get()` | `(*T, bool)` | Получить изменяемое сообщение. Второй результат отличает сохранённый стейт от отсутствующего |
| `Or(fallback *T)` | `*T` | Вернуть текущее сообщение или записать и вернуть `fallback`, если стейта нет |
| `Set(value *T)` | `error` | Заменить значение стейта целиком; пустое сообщение удаляет стейт |
| `Clear()` | — | Удалить стейт для текущего ключа |
| `ReadOnly()` | `ReadOnlyProtoState[T, PT]` | Представление только на чтение |

{% note info %}

В отличие от Python, где `get_or_default()` без аргументов отдаёт пустой экземпляр Proto-класса, в Go значение по умолчанию задаётся явно — передайте `&T{}`, если хотите начать с пустого сообщения. Пустое сообщение сериализуется в ноль байт, а ноль байт означают отсутствие стейта: такое сообщение не записывается для ключа, у которого стейта нет, и удаляет стейт у ключа, у которого он есть. Учтите, что сообщение, все поля которого равны нулевым значениям, пустое именно в этом смысле.

{% endnote %}

### Пример использования {#proto-example}

```go
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", msg)
if err != nil {
    return err
}

window := state.Or(&TJoinState{})
window.ShowTime = showTime

return nil
```

## Конфигурация в статической спеке {#static-spec}

Internal State не требует создания внешних таблиц. Стейты автоматически хранятся во внутренних таблицах Flow.

Имена внутренних стейтов должны быть объявлены в секции `internal_states` параметров [компьютейшена](../../flow/concepts/glossary.md#stream-and-computation) в статической спеке:

{% code '/yt/yt/flow/examples/go/word_count/test/pipeline.yson' lang='yson' %}

Имя стейта в коде (второй аргумент `flow.OpenYSONState`, `flow.OpenRawState` или `flow.OpenProtoState`) должно совпадать с именем, объявленным в `internal_states`.

{% note warning %}

Если имя стейта не объявлено в `internal_states`, функция открытия вернёт ошибку, обёртывающую `flow.ErrUnknownState`; текст ошибки перечисляет объявленные имена. Возвращённая из обработчика ошибка прекращает обработку всего батча — воркер повторит запрос целиком.

{% endnote %}

## См. также

- [State Accessor (Go)](state-accessor.md)
- [External State (Go)](external-state.md)
- [Работа со стейтами (Go)](state.md) — краткий обзор
- [Stateful processing (концепция)](../../flow/concepts/stateful.md)
- [Примеры: Word Count (Go)](examples/wordcount.md)
