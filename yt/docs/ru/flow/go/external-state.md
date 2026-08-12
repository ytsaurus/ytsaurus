# External State в {{product-name}} Flow (Go)

External State — механизм работы с внешним [состоянием (стейтом)](../../flow/concepts/glossary.md#state), хранящимся во внешней динамической таблице {{product-name}}. Пользователь самостоятельно создаёт таблицу для хранения стейта на том же кластере, где развёрнут [пайплайн](../../flow/concepts/glossary.md#pipeline).

Общие сведения о stateful-обработке описаны в разделе [Stateful processing](../../flow/concepts/stateful.md).

## Обзор {#overview}

External State в Go SDK представлен двумя аксессорами: `flow.ExternalStateAccessor` для стейта, которым компьютейшен **владеет**, и `flow.JoinedExternalStateAccessor` для стейта, который компьютейшен только **читает**. Оба преобразуют строку внешней динамической таблицы в пользовательскую Go-структуру с тегами `yson`. Стейт привязан к [ключу](../../flow/concepts/glossary.md#key) сообщения (`group_by_schema`).

| Аксессор | Секция спеки | Открывается функцией | Запись |
|---|---|---|---|
| [ExternalStateAccessor](#getting-accessor) | `external_state_managers` | `flow.OpenExternalState` | Есть |
| [JoinedExternalStateAccessor](#joined-external-state) | `external_state_joiners` | `flow.OpenJoinedExternalState` | Нет |

Владение и чтение живут в разных пространствах имён: стейт, объявленный в `external_state_managers`, недоступен через `flow.OpenJoinedExternalState`, и наоборот. Подробнее про аксессоры и общие принципы работы со стейтом: [State Accessor (Go)](state-accessor.md).

## Отличие от Internal State {#vs-internal-state}

| Характеристика | External State | Internal State |
|---|---|---|
| Хранение | Внешняя динамическая таблица | Внутренние таблицы Flow |
| Создание таблицы | Пользователь создаёт самостоятельно | Автоматически |
| Формат данных | Go-структура поверх строки таблицы | Произвольный (YSON, Protobuf, сырые байты) |
| Доступ из других систем | Да (сортированная динамическая таблица) | Нет |
| Схема | Определяется схемой таблицы | Определяется пользователем |
| Имя стейта | Абсолютный путь, начинается с `/` | Обычное имя без `/` |

Подробнее о внутреннем стейте см. [Internal State (Go)](internal-state.md).

## Получение аксессора {#getting-accessor}

[Исходный код]({{source-root}}/yt/go/flow/context.go)

`flow.ExternalStateAccessor` открывается через `flow.Runtime`:

```go
// Для сообщения
state, err := flow.OpenExternalState(rt, "/shuffle-state", msg)

// Для таймера
state, err := flow.OpenExternalState(rt, "/shuffle-state", timer)
```

Аргументы те же, что и у аксессоров внутреннего стейта:

- `rt` — `flow.Runtime` обработчика.
- `name` — имя стейта из секции `external_state_managers` [статической спеки](../../flow/concepts/glossary.md#spec-and-dynamic-spec) (в примере — `"/shuffle-state"`). Имя обязательно начинается с `/` и совпадает с ключом в спеке.
- `input` — вход, к ключу которого привязывается стейт: `flow.ExtendedMessage`, `flow.Timer` или `flow.Visit` (всё, что реализует `flow.Input`).

{% note warning %}

Имя внешнего стейта валидируется: оно должно начинаться с `/`, не быть пустым, не быть самим корнем `/`, не оканчиваться на `/` и не содержать двух подряд идущих `/`. Нарушение формы даёт ошибку, обёртывающую `flow.ErrInvalidStateName`; имя правильной формы, но не объявленное в спеке, — ошибку, обёртывающую `flow.ErrUnknownState`.

{% endnote %}

В отличие от внутреннего стейта, внешний нельзя создать на пустом месте: схема строк известна из таблицы стейта, а не из спеки, поэтому она приходит вместе с запросом. Если запрос не принёс стейт с таким именем, открытие вернёт ошибку, обёртывающую `flow.ErrStateNotRead`. Для стейта, которым компьютейшен владеет, воркер отдаёт строку по каждому ключу батча (пустую для ключей, которым ещё ничего не сохраняли), так что на практике эта ошибка встречается у [приджойненного стейта](#joined-external-state).

## Основные операции {#operations}

### Чтение и запись типизированного стейта {#read}

Опишите используемые колонки структурой. Указатели позволяют отличить отсутствующую колонку от её zero value:

```go
type joinState struct {
    ShowTime  *uint64 `yson:"show_time"`
    ClickTime *uint64 `yson:"click_time"`
}
```

`ConvertTo` читает строку в структуру, а второй результат сообщает, существовала ли строка:

```go
state, err := flow.OpenExternalState(rt, "/join-state", msg)
if err != nil {
    return err
}

var window joinState
_, err = state.ConvertTo(&window)
if err != nil {
    return err
}
```

После изменения структура сохраняется обратным преобразованием:

```go
window.ShowTime = &showTime
return state.ConvertFrom(&window)
```

`ConvertFrom` обновляет поля структуры поверх текущей строки, поэтому не представленные в структуре колонки сохраняются. Для динамических схем остаются низкоуровневые `Get`, `Or`, `Builder`, `Set` и `Schema`.

### Очистка стейта {#clear}

```go
state, err := flow.OpenExternalState(rt, "/join-state", timer)
if err != nil {
    return err
}

// Удаление строки из таблицы
return state.Clear()
```

{% note info %}

Пустой стейт соответствует отсутствию строки в таблице: `ConvertTo` для такого ключа вернёт `false` первым результатом. Обратно воркеру уезжают только те строки, которые компьютейшен изменил через `Set` или `Clear`, — протокол стейта передаёт дельту, а не весь стейт целиком.

{% endnote %}

## Конфигурация в статической спеке {#static-spec}

Для использования External State необходимо объявить external state manager в секции `external_state_managers` [компьютейшена](../../flow/concepts/glossary.md#stream-and-computation) в статической спеке. Пример из [static_table_join]({{source-root}}/yt/yt/flow/examples/go/static_table_join), где компьютейшен `reference_loader` владеет справочником:

{% code '/yt/yt/flow/examples/go/static_table_join/test/pipeline.yson' lang='yson' %}

Ключевые поля:

- `external_state_managers` — секция верхнего уровня внутри компьютейшена с описанием внешних state-менеджеров.
- Ключ внутри `external_state_managers` (здесь `"/reference_state"`) — имя стейта, используемое в Go-коде при вызове `flow.OpenExternalState(rt, "/reference_state", msg)`. Имя обязательно начинается с `/`.
- `external_state_manager_class_name` — имя зарегистрированного класса external state manager. Для типового сценария — `"NYT::NFlow::TSimpleExternalStateManager"`; для профилей BigRT — `"NYT::NFlow::NBigRTExtensions::TProfileManager<TUserProfile>"`. Подробнее см. в [C++ документации](../../flow/cpp/state.md#external-state).
- `parameters.path` — путь к динамической таблице {{product-name}}, в которой хранится стейт.

## Создание таблицы для стейта {#state-table}

Таблица для External State должна быть создана заранее. Ключевые колонки таблицы должны совпадать с `group_by_schema` компьютейшена: именно по ключу сообщения воркер находит строку стейта.

Для создания таблицы рекомендуется использовать [YtSync]({{yt-sync-docs}}/). Описание таблицы стейта из [Shuffle](examples/shuffle.md), чей компьютейшен `reducer` группирует сообщения по `farm_hash(value), value`:

{% code '/yt/yt/flow/examples/go/shuffle/test/yt_sync.py' lang='python' lines='[BEGIN yt_sync_tables]-[END yt_sync_tables]' %}

## Полный пример — eventReducer из Shuffle {#example}

{% code '/yt/yt/flow/examples/go/shuffle/event_reducer.go' lang='go' lines='[BEGIN event_reducer]-[END event_reducer]' %}

[Полный исходный код]({{source-root}}/yt/yt/flow/examples/go/shuffle/event_reducer.go)

Паттерн работы:

1. Открыть стейт через `flow.OpenExternalState(...)`.
2. Прочитать его в структуру через `state.ConvertTo(&value)`.
3. Изменить поля и сохранить структуру через `state.ConvertFrom(&value)`.

## Joined External State {#joined-external-state}

Если внешний стейт нужен **только для чтения** — например, чтобы обогатить события справочником, который наполняет другой компьютейшен, — используется приджойненный стейт. На стороне фреймворка его обслуживает [External State Joiner](../../flow/cpp/state.md#external-state-joiner), читающий таблицу с кэшированием по TTL; в спеке он объявляется в секции `external_state_joiners` (на одном уровне с `external_state_managers`).

В Go такой стейт представлен отдельным типом `flow.JoinedExternalStateAccessor`, у которого нет ни `Set`, ни `Clear`: джойнер никогда не пишет обратно, и ответ, заявивший обратное, был бы отвергнут [воркером](../../flow/concepts/glossary.md#worker).

### Получение аксессора {#getting-joined-accessor}

```go
reference, err := flow.OpenJoinedExternalState(rt, "/reference_state", msg)
```

| Метод | Возвращаемый тип | Описание |
|---|---|---|
| `ConvertTo(&value)` | `(bool, error)` | Заполнить структуру приджойненной строкой; `bool` сообщает, была ли она найдена |

Для динамических схем доступны низкоуровневые `Get`, `Or` и `Schema`.

{% note warning %}

Воркер джойнит только те ключи, для которых нашёл строки. Поэтому батч, не совпавший ни по одному ключу, приходит вообще без приджойненного стейта, и `flow.OpenJoinedExternalState` вернёт ошибку, обёртывающую `flow.ErrStateNotRead`. Это не сбой: обрабатывайте её как «обогащать нечем», а не как ошибку компьютейшена.

{% endnote %}

### Конфигурация в статической спеке {#joined-static-spec}

Компьютейшен `enricher` из примера [static_table_join]({{source-root}}/yt/yt/flow/examples/go/static_table_join) читает тот самый справочник, которым владеет `reference_loader` из [секции выше](#static-spec):

{% code '/yt/yt/flow/examples/go/static_table_join/test/pipeline.yson' lang='yson' %}

Поля `external_state_joiners` повторяют поля `external_state_managers` с точностью до имени класса: `external_state_joiner_class_name` вместо `external_state_manager_class_name`. Путь `parameters.path` резолвится при каждом обращении, поэтому если направить его на симлинк, то переключение симлинка подменяет весь справочник под работающим пайплайном, без рестарта.

### Пример {#joined-example}

{% code '/yt/yt/flow/examples/go/static_table_join/enricher.go' lang='go' lines='[BEGIN enricher]-[END enricher]' %}

[Полный исходный код]({{source-root}}/yt/yt/flow/examples/go/static_table_join/enricher.go)

Тот же приём, но со справочником, который наполняется не пайплайном, а внешним процессом, разобран в примере [external_state_join]({{source-root}}/yt/yt/flow/examples/go/external_state_join): там `parameters.path` указывает на симлинк, и подмена симлинка меняет справочник целиком.

## См. также

- [State Accessor (Go)](state-accessor.md)
- [Internal State (Go)](internal-state.md)
- [Работа со стейтами (Go)](state.md) — краткий обзор
- [Stateful processing (концепция)](../../flow/concepts/stateful.md)
- [Примеры: Shuffle (Go)](examples/shuffle.md)
