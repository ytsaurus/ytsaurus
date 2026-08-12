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
| [ProtoStateAccessor](#proto-state-accessor) | Protobuf | `flow.OpenProtoState[T]` |

`RawStateAccessor` и `ProtoStateAccessor` читают и записывают значения явно через `Get`, `Set` и `Clear`. `YSONState` предоставляет изменяемое значение: изменения, сделанные через `Value()`, сериализуются автоматически после успешного завершения обработчиков батча.

Каждая из функций `flow.OpenXxxState` принимает три аргумента:

- `rt` — `flow.Runtime` обработчика.
- `name` — имя стейта. Имена внутренних стейтов не начинаются с `/` и должны быть объявлены в `parameters.internal_states` [спеки](../../flow/concepts/glossary.md#spec-and-dynamic-spec) [компьютейшена](../../flow/concepts/glossary.md#stream-and-computation), см. [Конфигурация в статической спеке](#static-spec).
- `input` — вход, к [ключу](../../flow/concepts/glossary.md#key) которого привязывается стейт. Подходит любое значение, реализующее `flow.Input`: `flow.ExtendedMessage`, `flow.Timer`, `flow.Visit`.

{% note info %}

Аксессор показывает стейт таким, каким он станет после ответа воркеру: стейт, которого не было во входном запросе, и стейт, очищенный в этом же вызове через `Clear`, читаются одинаково — как отсутствующий.

{% endnote %}

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
| `Value()` | `*T` | Получить изменяемое значение; для отсутствующего стейта создаётся zero value |
| `Clear()` | — | Удалить значение |

Изменения из `Value()` сериализуются автоматически после успешного завершения всех обработчиков батча. Если обработчик вернул ошибку, изменения YSON-стейта не попадают в ответ воркеру.

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
| `Set(data []byte)` | `error` | Сохранить сырые байты |
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

## ProtoStateAccessor {#proto-state-accessor}

[Исходный код]({{source-root}}/yt/go/flow/context.go)

`ProtoStateAccessor` сериализует стейт через Protobuf. Тип Protobuf-сообщения указывается в значимой форме, а аксессор отдаёт указатель на него: `flow.OpenProtoState[TJoinState]` возвращает аксессор над `*TJoinState`.

### Получение аксессора {#getting-proto-accessor}

```go
// Для сообщения
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", msg)

// Для таймера
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", timer)
```

### Методы {#proto-methods}

| Метод | Возвращаемый тип | Описание |
|---|---|---|
| `Get()` | `(*T, bool, error)` | Десериализовать и вернуть значение. Второй результат отличает сохранённый стейт от отсутствующего и осмыслен только при `err == nil` |
| `Or(fallback *T)` | `(*T, error)` | Вернуть текущее значение или `fallback`, если стейта нет |
| `Set(value *T)` | `error` | Сериализовать и сохранить Proto-сообщение |
| `Clear()` | `error` | Удалить стейт для текущего ключа |

{% note info %}

В отличие от Python, где `get_or_default()` без аргументов отдаёт пустой экземпляр Proto-класса, в Go значение по умолчанию задаётся явно — передайте `&T{}`, если хотите начать с пустого сообщения.

{% endnote %}

### Пример использования {#proto-example}

```go
state, err := flow.OpenProtoState[TJoinState](rt, "join-state", msg)
if err != nil {
    return err
}

window, err := state.Or(&TJoinState{})
if err != nil {
    return err
}
window.ShowTime = showTime

return state.Set(window)
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
