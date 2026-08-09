# StateAccessor в {{product-name}} Flow (Go)

StateAccessor — интерфейс для чтения, модификации и удаления значений [стейта](../../flow/concepts/glossary.md#state).
Общие сведения о stateful-обработке описаны в разделе [Stateful-вычисления](../../flow/concepts/stateful.md).

## Принцип работы {#how-it-works}

[Стейт](../../flow/concepts/glossary.md#state) во Flow хранится в [сортированных динамических таблицах](../../user-guide/dynamic-tables/sorted-dynamic-tables.md).
В случае [внешнего стейта](external-state.md) эта таблица создаётся пользователем, в случае [внутреннего стейта](internal-state.md) таблицы создаются и управляются Flow автоматически.

Для `TTransformCompanionComputation` ключевые колонки в таблице стейта совпадают с `group_by_schema` [компьютейшена](../../flow/concepts/glossary.md#stream-and-computation). Для внутреннего стейта `TTransformOrderedSourceCompanionComputation` ключом служит ключ партиции источника: `group_by_schema` в таком SourceComputation не поддерживается. Во всех случаях сообщения с одинаковым ключом разделяют один стейт.

Аксессор в Go — это значение, связывающее две вещи: держатель стейта с определённым именем и ключ конкретного входа. Держатели живут в `flow.Runtime` и доступны напрямую (`rt.InternalState(name)`, `rt.ExternalState(name)`, `rt.JoinedExternalState(name)`), но обычному компьютейшену они не нужны: аксессор — это и есть удобный вид на стейт одного ключа.

## Чтение и запись данных {#reading-and-writing-data}

Непосредственную работу с таблицей (чтение, запись, удаление данных) осуществляет [воркер](../../flow/concepts/glossary.md#worker). При получении очередного батча сообщений воркер загружает значения стейтов для всех ключей в батче и отправляет их в [компаньон](../../flow/concepts/companion.md) вместе с сообщениями и таймерами. Подробнее про [схему взаимодействия](../../flow/concepts/companion.md#schema).

Запись новых значений в таблицу стейта осуществляется транзакционно в рамках [эпохи](../../flow/concepts/glossary.md#epoch).

Обратно воркеру уезжает не весь стейт, а дельта: только изменённые записи. Для Raw-, Proto- и External-стейтов запись выполняется через `Set` или `Clear`; для YSON-стейта — изменением значения из `Value()` или вызовом `Clear`. Простое чтение ничего не отправляет.

`Clear` не стирает запись из аксессора, а помечает её удалённой, и удаление доезжает до воркера именно в таком виде. Для компьютейшена разницы нет: стейт, которого запрос не принёс, и стейт, очищенный в этом запросе, одинаково читаются как отсутствующий — компьютейшен видит стейт таким, каким он станет после ответа.

{% note warning %}

`flow.Runtime` и все открытые из него аксессоры принадлежат горутине, обслуживающей запрос, и не рассчитаны на конкурентное использование. Если обработчик распараллеливает работу, стейт следует читать и писать в той же горутине, в которой обработчик был вызван. Правила запуска дочерних горутин описаны в разделе [«Горутины в обработчике»](computation.md#goroutines).

{% endnote %}

## Виды аксессоров {#accessor-types}

Go SDK предоставляет пять видов аксессоров:

| Аксессор | Формат | Открытие | Описание |
|----------|--------|----------|----------|
| [RawStateAccessor](internal-state.md#raw-state-accessor) | `[]byte` | `flow.OpenRawState(rt, name, input)` | Сырые байты без сериализации |
| [YSONState](internal-state.md#yson-state) | YSON | `flow.OpenYSONState[T](rt, name, input)` | Сериализация Go-значения в YSON |
| [ProtoStateAccessor](internal-state.md#proto-state-accessor) | Protobuf | `flow.OpenProtoState[T](rt, name, input)` | Сериализация через Protobuf |
| [ExternalStateAccessor](external-state.md) | Go-структура (строка таблицы) | `flow.OpenExternalState(rt, "/name", input)` | Чтение и запись строки внешней таблицы |
| [JoinedExternalStateAccessor](external-state.md) | Go-структура | `flow.OpenJoinedExternalState(rt, "/name", input)` | Read-only доступ к таблице чужого стейта |

Первые три работают с [внутренним стейтом](internal-state.md), таблицами которого управляет Flow. `ProtoStateAccessor` сериализует явные записи поверх `RawStateAccessor`. `YSONState` держит изменяемое значение и сохраняет его автоматически после успешного батча.

`ExternalStateAccessor` и `JoinedExternalStateAccessor` работают с [внешним стейтом](external-state.md) — динамической таблицей, которую пользователь создаёт сам. Различаются они правами: первый доступен компьютейшену, который стейтом владеет, второй — компьютейшену, который его только читает.

## API внутренних стейтов {#common-api}

Raw- и Proto-аксессоры используют явные операции чтения и записи:

| Метод | `RawStateAccessor` | `ProtoStateAccessor[T, PT]` |
|-------|--------------------|-----------------------------|
| `Get()` | `([]byte, bool)` | `(PT, bool, error)` |
| `Or(fallback)` | `[]byte` | `(PT, error)` |
| `Set(value)` | `error` | `error` |
| `Clear()` | `error` | `error` |

YSON-стейт изменяется на месте:

| Метод | Тип результата | Описание |
|-------|----------------|----------|
| `Empty()` | `bool` | Значение отсутствует |
| `Value()` | `*T` | Изменяемое значение; создаёт zero value при отсутствии |
| `Clear()` | — | Удалить значение |

Десериализация YSON выполняется в `OpenYSONState`. Изменения из `Value()` сохраняются автоматически только после успешного завершения обработчиков батча.

`ExternalStateAccessor` преобразует строку таблицы в Go-структуру:

| Метод | Тип результата | Описание |
|-------|----------------|----------|
| `ConvertTo(&value)` | `(bool, error)` | Прочитать строку в структуру; `bool` отличает отсутствующую строку |
| `ConvertFrom(&value)` | `error` | Сохранить поля структуры в строку |
| `Clear()` | `error` | Удалить строку |

`JoinedExternalStateAccessor` предоставляет `ConvertTo(&value)`, но не запись и не очистку. Для динамических схем у обоих аксессоров остаются низкоуровневые `Get`, `Or` и `Schema`, а у владельца — `Builder` и `Set`.

## Получение аксессора {#getting-accessor}

Аксессор открывается свободной функцией внутри `OnMessage`, `OnTimer` или `OnVisit`:

```go
func (*myFunction) OnMessage(
    ctx context.Context,
    rt flow.Runtime,
    msg flow.ExtendedMessage,
    out flow.OutputCollector,
) error {
    // YSON
    ysonState, err := flow.OpenYSONState[myState](rt, "state-name", msg)

    // Сырые байты
    rawState, err := flow.OpenRawState(rt, "state-name", msg)

    // Protobuf
    protoState, err := flow.OpenProtoState[TMyState](rt, "state-name", msg)

    // Внешний стейт: имя обязательно начинается с "/"
    extState, err := flow.OpenExternalState(rt, "/state-name", msg)

    // Внешний стейт только на чтение
    joined, err := flow.OpenJoinedExternalState(rt, "/reference", msg)
}

func (*myFunction) OnTimer(
    ctx context.Context,
    rt flow.Runtime,
    timer flow.Timer,
    out flow.OutputCollector,
) error {
    // То же самое, но вместо сообщения передаётся таймер
    state, err := flow.OpenYSONState[myState](rt, "state-name", timer)
}
```

Параметры:

- `rt` — `flow.Runtime`, второй аргумент обработчика.
- `name` — имя стейта, объявленное в [статической спеке](../../flow/concepts/glossary.md#spec-and-dynamic-spec). Для внутренних стейтов — произвольная строка из `internal_states`; для внешних — ключ из `external_state_managers` или `external_state_joiners`, обязательно начинающийся с `/`.
- `input` — вход, к [ключу](../../flow/concepts/glossary.md#key) которого привязывается аксессор. Подходит любое значение, реализующее `flow.Input`: `flow.ExtendedMessage`, `flow.Timer` и `flow.Visit`.

Типовой параметр указывается только у YSON- и Proto-аксессоров и задаёт тип стейта: `flow.OpenProtoState[TMyState]` выдаёт аксессор над `*TMyState`. Именно из-за этого функции открытия свободные, а не методы `Runtime`: собственных типовых параметров у методов в Go нет.

Открытие возвращает ошибку, если имя стейта не подходит:

| Ошибка | Причина |
|--------|---------|
| `flow.ErrUnknownState` | Имя не объявлено в спеке компьютейшена |
| `flow.ErrInvalidStateName` | Имя внешнего стейта не является абсолютным путём |
| `flow.ErrStateNotRead` | Запрос не принёс внешнего стейта с таким именем |
| `flow.ErrNoStateSchema` | Внешний стейт пришёл без схемы своих строк |

{% note info %}

`flow.ErrStateNotRead` при открытии заджойненного стейта — штатная ситуация, а не сбой: воркер джойнит только те ключи, для которых нашёл строки, поэтому батч, не совпавший ни с одной строкой справочника, приходит без такого стейта. Отличить эту ошибку следует через `errors.Is` и трактовать как отсутствие данных.

{% endnote %}

## Когда использовать какой тип {#choosing-type}

| Ситуация | Рекомендуемый аксессор |
|----------|------------------------|
| Структура из нескольких полей, схема которой меняется вместе с кодом | `flow.OpenYSONState[T]` (YSON) |
| Произвольные двоичные данные или собственная сериализация | `flow.OpenRawState` (Raw) |
| Структурированные данные с фиксированной схемой, общей с другими языками | `flow.OpenProtoState[T]` (Protobuf) |
| Данные, к которым нужен доступ из других систем | `flow.OpenExternalState` (External) |
| Данные, требующие пользовательской таблицы | `flow.OpenExternalState` (External) |
| Справочник, который компьютейшен только читает | `flow.OpenJoinedExternalState` (Joined) |

## См. также

- [Internal State (Go)](internal-state.md)
- [External State (Go)](external-state.md)
- [Работа со стейтами (Go)](state.md) — краткий обзор всех типов
- [Computation (Go)](computation.md)
- [Stateful-вычисления](../../flow/concepts/stateful.md)
