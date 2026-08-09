# Computation в {{product-name}} Flow (Go)

{% note info %}

На этой странице описаны Go-специфичные детали работы с компьютейшенами. Общие концепции описаны в разделе [Computation](../../flow/concepts/computation.md).

{% endnote %}

## Типы Computation {#computation-types}

Во Flow есть два вида `Computation`: [`Swift`](../../flow/concepts/glossary.md#swift) и `Transform`. От их выбора зависит способ обеспечения exactly-once гарантий и то, какие преобразования возможно реализовать с их применением.

| Тип | Способ обеспечения гарантий | Применение |
|-----|-----------------------------|------------|
| `Swift`| Код преобразования детерминирован, при необходимости будет вызываться повторно | Stateless преобразования |
| `Transform` | Результат работы обязательно сохраняется в YT, поэтому нет требований на какую-либо детерминированность преобразований | Stateful преобразования [Подробнее](../../flow/concepts/stateful.md) |

При использовании [компаньона](../../flow/concepts/glossary.md#companion) выбор `Swift` или `Transform` осуществляется через указание `computation_class_name` в статической [спеке](../../flow/concepts/glossary.md#spec-and-dynamic-spec):

- `NYT::NFlow::NCompanion::TTransformCompanionComputation` — для `Transform`.
- `NYT::NFlow::NCompanion::TSwiftMapCompanionComputation` — для `Swift`.
- `NYT::NFlow::NCompanion::TSwiftOrderedSourceCompanionComputation` — для `Swift`-сорса.
- `NYT::NFlow::NCompanion::TTransformOrderedSourceCompanionComputation` — для `Transform`-сорса.

На стороне Go выбор конструктора отвечает не за `Swift` против `Transform`, а за то, чем компьютейшен объявляется [воркеру](../../flow/concepts/glossary.md#worker): сорсом или трансформом. `Swift`- и `Transform`-компьютейшены создаются одними и теми же конструкторами, а различает их `computation_class_name` в спеке.

| Конструктор | Тип, сообщаемый воркеру | `computation_class_name` в спеке |
|-------------|-------------------------|----------------------------------|
| `flow.NewRowComputation(id, fn)` | `Transform` | `TTransformCompanionComputation` или `TSwiftMapCompanionComputation` |
| `flow.NewBatchComputation(id, fn)` | `Transform` | `TTransformCompanionComputation` или `TSwiftMapCompanionComputation` |
| `flow.NewRowSourceComputation(id, fn)` | `Source` | `TSwiftOrderedSourceCompanionComputation` или `TTransformOrderedSourceCompanionComputation` |
| `flow.NewBatchSourceComputation(id, fn)` | `Source` | `TSwiftOrderedSourceCompanionComputation` или `TTransformOrderedSourceCompanionComputation` |

Для сорса `TSwiftOrderedSourceCompanionComputation` подходит только для детерминированной обработки без пользовательского стейта. Если SourceComputation использует [внутренний стейт](state.md) или недетерминированную логику, в спеке указывают `TTransformOrderedSourceCompanionComputation`: воркер материализует выход и фиксирует его вместе со стейтом и смещением источника. Ключ внутреннего стейта в таком компьютейшене — ключ партиции источника.

## Создание Computation {#computation}

Компьютейшен создаётся конструктором и регистрируется в `flow.Pipeline` через `pipeline.Add`. Пример из [Shuffle](examples/shuffle.md), где компаньон обслуживает оба конца пайплайна — сорс и трансформ:

{% code '/yt/yt/flow/examples/go/shuffle/main.go' lang='go' %}

У конструкторов два обязательных параметра:

| Параметр | Обязательный | Описание |
|----------|:---:|----------|
| `id` | Да | Идентификатор компьютейшена, совпадающий с ключом в `computations` статической спеки |
| `fn` | Да | Значение с логикой обработки: `flow.RowFunction` или `flow.BatchFunction` |

{% note warning %}

`fn == nil` недопустим: конструктор паникует на месте. Компьютейшен без функции обработки провалил бы каждый батч, который ему пришлют, а сообщить о такой ошибке по протоколу уже некуда.

Если нужен [passthrough](../../flow/concepts/glossary.md#passthrough) — не регистрируйте компьютейшен в Go вовсе, а в статической спеке укажите C++-класс passthrough в `computation_class_name` (см. [Passthrough Computation](../../flow/concepts/computation.md#passthrough)).

{% endnote %}

В статической спеке создаётся Computation с таким же `id` (в данном примере `mapper`):

```yson
"mapper" = {
    "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
    "group_by_schema" = [
        ...
    ];
    "input_stream_ids" = [...];
    "output_stream_ids" = [...];
    "required_resource_ids" = {
        "CompanionManager" = {
            "worker" = true;
            "controller" = false;
        };
    };
    "parameters" = {
        ...
    };
};
```

Подробнее про спеку в разделе [Spec, DynamicSpec и Config](../../flow/concepts/spec.md).

## SourceComputation {#sourcecomputation}

`SourceComputation` — вершина в графе [пайплайна](../../flow/concepts/glossary.md#pipeline), осуществляющая чтение данных из внешних источников. На стороне воркера ей соответствует [TSwiftOrderedSourceComputation](../../flow/concepts/computation.md#tswiftorderedsourcecomputation) или [TTransformOrderedSourceComputation](../../flow/concepts/computation.md#ttransformorderedsourcecomputation).

В Go сорс создаётся конструкторами `flow.NewRowSourceComputation` и `flow.NewBatchSourceComputation`. Интерфейс функции обработки у сорса тот же, что и у трансформа: сорс отличается от трансформа только тем, каким он объявляется воркеру.

### Создание SourceComputation {#creating-sourcecomputation}

```go
pipeline.Add(flow.NewRowSourceComputation("reader", &eventMapper{}))
```

Для passthrough-сорса не используйте Go — укажите в спеке `NYT::NFlow::TSwiftPassthroughOrderedSourceComputation` в `computation_class_name` и оставьте компьютейшен незарегистрированным в Go-компаньоне. Подробнее — [Passthrough Computation](../../flow/concepts/computation.md#passthrough).

### Взаимодействие с Worker {#companion-info}

При инициализации [воркер](../../flow/concepts/glossary.md#worker) запрашивает у Go-компаньона список зарегистрированных компьютейшенов вместе с их типом (`Source` или `Transform`). Список фиксируется на старте gRPC-сервера: компьютейшен, добавленный в `Pipeline` после старта, воркеру уже не виден.

Дальше воркер адресует компьютейшен по его `id` и присылает батчи входов. Стримы, в которые сорс поставляет сообщения, воркер сообщает не один раз на джобу, а на каждый запрос, поэтому `rt.StreamSpecs()` и `rt.MessageBuilder(...)` в сорсе всегда описывают текущий запрос.

## Process Function {#process-function}

Бизнес-логика обработки данных реализуется через Process Function. Необходимо выбрать один из двух интерфейсов: [`flow.RowFunction`]({{source-root}}/yt/go/flow/computation.go) или [`flow.BatchFunction`]({{source-root}}/yt/go/flow/computation.go).

{% note info %}

Использование `RowFunction` или `BatchFunction` — исключительно вопрос бизнес-логики. `RowFunction` не добавляет накладных расходов на обработку данных относительно использования `BatchFunction` благодаря тому, что Flow внутри себя осуществляет передачу данных батчами.

{% endnote %}

Каждый обработчик получает четыре аргумента:

| Аргумент | Описание |
| --- | --- |
| `ctx context.Context` | Контекст запроса, в котором пришёл вход. У него есть дедлайн, и он отменяется, когда воркер отказывается от батча |
| `rt flow.Runtime` | Доступ к [стейтам](state.md), параметрам компьютейшена, [вотермаркам](../../flow/concepts/watermarks.md) и схемам стримов |
| `msg` / `timer` / `visit` | Обрабатываемый вход вместе с [ключом](../../flow/concepts/glossary.md#key), по которому он сгруппирован |
| `out flow.OutputCollector` | Сбор выходных сообщений и постановка [таймеров](../../flow/concepts/glossary.md#timer) |

Возвращённая обработчиком ошибка прекращает обработку остатка батча: воркер повторит запрос целиком, поэтому частичный ответ привёл бы к повторному учёту уже обработанных входов. Компаньон возвращает воркеру gRPC-ошибку `INTERNAL` с исходным текстом и по умолчанию пишет её в `stderr`; логгер можно заменить опцией `flow.WithLogger` (см. [Node companion](getting-started.md#node-companion)).

### RowFunction {#row-function}

`flow.RowFunction` получает [сообщения](../../flow/concepts/glossary.md#message) по одному. Интерфейс объявляет единственный метод `OnMessage`.

#### Типизированные YSON-сообщения {#typed-yson-messages}

Аналог `TYsonMessage` из C++ — Go-структура со встроенным `flow.YSONMessage`. Поля payload описываются тегами `yson`:

```go
type numberMessage struct {
    flow.YSONMessage

    Number int64 `yson:"number"`
}

type doubledMessage struct {
    flow.YSONMessage

    NumberX2 int64 `yson:"number_x2"`
}
```

Структуры одновременно задают API обработчика и схемы стримов. Раннер добавляет выведенные схемы в спеку перед запуском `flow_server`:

```go
pipeline.AddStreams(
    flow.NewYSONStream[numberMessage]("numbers"),
    flow.NewYSONStream[doubledMessage]("x2_numbers"),
)
```

В обработчике вход декодируется в структуру, а выход создаётся и кодируется без прямой работы с `Payload` и `MessageBuilder`:

```go
type x2Mapper struct{}

var _ flow.RowFunction = (*x2Mapper)(nil)

func (*x2Mapper) OnMessage(
    ctx context.Context,
    rt flow.Runtime,
    msg flow.ExtendedMessage,
    out flow.OutputCollector,
) error {
    var input numberMessage
    if err := msg.ConvertTo(&input); err != nil {
        return err
    }

    output := flow.NewYSONMessage[doubledMessage]("x2_numbers")
    output.NumberX2 = input.Number * 2

    encoded, err := flow.ConvertFrom(rt, output)
    if err != nil {
        return err
    }
    out.AddMessage(encoded)
    return nil
}
```

`msg.ConvertTo(&input)` переносит в `input.Meta` идентификатор стрима, таймстемпы и ID входа. Ключ сообщения, таймера или визита преобразуется тем же методом: `msg.Key.ConvertTo(&key)`. `flow.NewYSONMessage` задаёт output-stream; event- и system-timestamp при необходимости меняются через `output.Meta` до вызова `flow.ConvertFrom`.

Низкоуровневые `flow.Payload`, `flow.PayloadBuilder` и `rt.MessageBuilder` остаются доступны для динамических схем и поколоночной обработки.

#### Пример stateful-функции {#stateful-example}

Функция из [WordCount](examples/wordcount.md) считает вхождения каждого слова в [стейте](../../flow/concepts/glossary.md#state) ключа:

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

### Опциональные обработчики {#optional-handlers}

[Таймеры](../../flow/concepts/glossary.md#timer) и визиты [key-visitor-стримов](../../flow/concepts/key_visitor.md) обрабатываются отдельными интерфейсами, объявленными на том же типе:

| Интерфейс | Метод | Вход |
| --- | --- | --- |
| `flow.RowTimerFunction` | `OnTimer(ctx, rt, timer, out)` | `flow.Timer` |
| `flow.RowVisitFunction` | `OnVisit(ctx, rt, visit, out)` | `flow.Visit` |

Так в Go выражается опциональность обработчика: компьютейшен объявляет только те методы, которые ему нужны. Воркер доставляет таймеры и визиты согласно спеке компьютейшена, а Go SDK пропускает те, для которых соответствующий обработчик не реализован. Пользовательские структуры реализуют эти интерфейсы указателем, чтобы не копировать значение при вызове методов; проверка `var _ flow.RowFunction = (*myFunction)(nil)` фиксирует контракт во время компиляции.

```go
type urlDownloadFunction struct{}

var (
    _ flow.RowFunction      = (*urlDownloadFunction)(nil)
    _ flow.RowTimerFunction = (*urlDownloadFunction)(nil)
)

// Обязательный обработчик: тип реализует flow.RowFunction.
func (*urlDownloadFunction) OnMessage(
    ctx context.Context,
    rt flow.Runtime,
    msg flow.ExtendedMessage,
    out flow.OutputCollector,
) error {
    // ...
    out.AddTimer(flow.TimerRequest{TriggerTimestamp: uint64(time.Now().Add(flushDelay).Unix())})
    return nil
}

// Объявление OnTimer на том же типе добавляет обработку таймеров.
func (*urlDownloadFunction) OnTimer(
    ctx context.Context,
    rt flow.Runtime,
    timer flow.Timer,
    out flow.OutputCollector,
) error {
    // ...
    return nil
}
```

Полные примеры — [URL Downloader](examples/url_downloader.md) и [Wait Click Join](examples/wait_click_join.md).

### BatchFunction {#batch-function}

`flow.BatchFunction` получает весь батч сообщений, пришедших от [воркера](../../flow/concepts/glossary.md#worker), одним вызовом метода `OnMessages`. Таймеры и визиты обрабатываются интерфейсами `flow.BatchTimerFunction` (`OnTimers`) и `flow.BatchVisitFunction` (`OnVisits`).

#### Пример batch-функции {#batch-example}

```go
type x2BatchMapper struct{}

var _ flow.BatchFunction = (*x2BatchMapper)(nil)

func (*x2BatchMapper) OnMessages(
    ctx context.Context,
    rt flow.Runtime,
    msgs []flow.ExtendedMessage,
    out flow.OutputCollector,
) error {
    for _, msg := range msgs {
        var input numberMessage
        if err := msg.ConvertTo(&input); err != nil {
            return err
        }

        output := flow.NewYSONMessage[doubledMessage]("x2_numbers")
        output.NumberX2 = input.Number * 2
        encoded, err := flow.ConvertFrom(rt, output)
        if err != nil {
            return err
        }
        out.AddMessage(encoded)
    }
    return nil
}
```

В отличие от `RowFunction`, выход batch-функции относится ко всему батчу целиком: [lineage](../../flow/concepts/lineage.md) выходных сообщений составляют идентификаторы всех входов батча, а не одного. Row-функция вызывается по одному входу, и её выход относится именно к нему.

### Функции без собственного типа {#function-adapters}

Компьютейшену, которому не нужны ни собственные поля, ни обработчики таймеров и визитов, объявлять тип необязательно: обычная функция передаётся через адаптеры `flow.RowFunc` и `flow.BatchFunc`.

```go
pipeline.Add(flow.NewRowComputation("mapper", flow.RowFunc(
    func(
        ctx context.Context,
        rt flow.Runtime,
        msg flow.ExtendedMessage,
        out flow.OutputCollector,
    ) error {
        return nil
    },
)))
```

## Фильтрация сообщений {#message-filtering}

Для фильтрации сообщений в source-компьютейшенах используется per-message-флаг [distribute](distribute.md): сообщение эмитится из Process Function вызовом `out.AddUndistributedMessage(msg)` и не публикуется дальше по графу, но учитывается при оценке [watermark](../../flow/concepts/watermarks.md).

Флаг читается воркером только на пути сорса. Трансформ публикует сообщение независимо от флага, поэтому в трансформе фильтрация — это просто не вызывать `out.AddMessage`.

## Регистрация в Pipeline {#pipeline-registration}

Все компьютейшены регистрируются через `pipeline.Add`, который принимает их переменным числом аргументов:

```go
pipeline := flow.NewPipeline()

pipeline.Add(
    // Transform-компьютейшен
    flow.NewRowComputation("reducer", &eventReducer{}),
    // Source-компьютейшен
    flow.NewRowSourceComputation("reader", &eventMapper{}),
)
```

`Pipeline` собирается из одной горутины и после этого передаётся в `pipeline.Run()`.

{% note warning %}

Каждый Computation должен иметь уникальный идентификатор, соответствующий идентификаторам в статической спеке. Регистрация двух компьютейшенов с одним `id` приводит к ошибке `flow.ErrDuplicateComputation` при построении сервера и невозможности старта компаньона.

{% endnote %}

Одно значение `Computation` — а значит и одна связанная с ним функция — обслуживает все запросы к этому идентификатору. Воркер обрабатывает партиции компьютейшена параллельно, и каждый запрос обслуживается собственной горутиной, поэтому функция, которая хранит состояние между вызовами, синхронизирует его сама.

## Горутины в обработчике {#goroutines}

Обработчик уже выполняется в отдельной горутине. Если внутри него требуется дополнительный параллелизм, запускайте дочерние горутины через `flow.Go(ctx, fn)`, а не оператором `go`: так компаньон сохраняет привязку потреблённых CPU и памяти к текущей джобе.

`flow.Go` только запускает функцию. Обработчик сам дожидается всех дочерних горутин, собирает их ошибки и завершает их до своего возврата. Fire-and-forget-работа недопустима: после возврата контекст запроса отменяется, а результат уже нельзя добавить в ответ воркеру.

```go
results := make(chan result, len(requests))
var wg sync.WaitGroup

for index, request := range requests {
    wg.Add(1)
    flow.Go(ctx, func(ctx context.Context) {
        defer wg.Done()
        value, err := callService(ctx, request)
        results <- result{index: index, value: value, err: err}
    })
}

wg.Wait()
close(results)
```

`flow.Runtime`, аксессоры стейта и `OutputCollector` не рассчитаны на конкурентное использование. В дочерних горутинах следует выполнять только независимую бизнес-логику или I/O, а стейт читать и менять и выходные сообщения собирать в исходной горутине обработчика после `wg.Wait()`.

## Runtime {#runtime}

[Исходный код]({{source-root}}/yt/go/flow/context.go)

`flow.Runtime` (`rt`) предоставляет доступ к контексту выполнения компьютейшена:

| Метод | Описание |
| --- | --- |
| `rt.MessageBuilder(streamID)` | Создать `MessageBuilder` для указанного output-[стрима](../../flow/concepts/glossary.md#stream-and-computation) |
| `rt.Parameters()` | Параметры компьютейшена из статической спеки |
| `rt.DynamicParameters()` | Параметры компьютейшена из динамической спеки |
| `rt.KeySchema()` | Схема [ключа](../../flow/concepts/glossary.md#key), по которому сгруппирован батч |
| `rt.StreamSpecs()` | Стримы компьютейшена и их схемы |
| `rt.MinWatermark()` | Минимальный [вотермарк](../../flow/concepts/glossary.md#timestamps-and-watermarks) по всем входным стримам |
| `rt.Watermark(streamID)` | [Вотермарк](../../flow/concepts/glossary.md#timestamps-and-watermarks) конкретного стрима |
| `rt.InternalState(name)` | Холдер внутреннего [стейта](../../flow/concepts/glossary.md#state) |
| `rt.ExternalState(name)` | Холдер внешнего стейта, которым владеет компьютейшен |
| `rt.JoinedExternalState(name)` | Холдер присоединённого внешнего стейта (только на чтение) |

Холдеры — низкоуровневый интерфейс: в пользовательском коде стейт ключа открывается аксессорами `flow.OpenYSONState`, `flow.OpenProtoState`, `flow.OpenRawState` и `flow.OpenExternalState`. Подробнее — в разделах [Работа со стейтами (Go)](state.md) и [State Accessor (Go)](state-accessor.md).

### Низкоуровневый MessageBuilder {#message-builder}

Для динамических схем выходное сообщение можно создать через `MessageBuilder`:

```go
builder, err := rt.MessageBuilder("stream_id")
if err != nil {
    return err
}

msg, err := builder.Set("field_name", value).Finish()
if err != nil {
    return err
}

out.AddMessage(msg)
```

Метод `Finish()` возвращает готовое `flow.Message`, не изменяя билдер. Идентификатор `stream_id` должен присутствовать в списке `output_stream_ids` в статической [спеке](../../flow/concepts/glossary.md#spec-and-dynamic-spec) компьютейшена, иначе `rt.MessageBuilder` вернёт `flow.ErrUnknownStream`.

Билдер типизирован схемой стрима: `Set` приводит переданное значение к wire-типу колонки. Строка собирается по колонке за раз, поэтому `Set` возвращает сам билдер, а не ошибку: первое отвергнутое значение запоминается, последующие `Set` не делают ничего, и ошибка возвращается из `Finish()` — `flow.ErrTypeMismatch`, если значение в колонку не укладывается, и `flow.ErrColumnNotFound`, если колонки в схеме нет. Значения `any`- и composite-колонок сериализуются в YSON, а `[]byte` в такую колонку записывается как уже сериализованный YSON.

Целая строка пишется одним вызовом `builder.SetStruct(v)`: колонки берутся из yson-тегов структуры `v` — тех же, по которым сериализуются [стейты](state.md). Колонка, которой нет в схеме стрима, отвергается как ошибка. Обратная операция — `payload.ConvertTo(&v)`: она заполняет поля структуры из одноимённых колонок, оставляя как есть те, которых в строке нет.

Дополнительно доступны `builder.SetEventTimestamp(ts)` и `builder.SetSystemTimestamp(ts)`. Оба поля по умолчанию заполняет воркер; `SetSystemTimestamp` в пользовательском коде обычно не нужен.

### Параметры компьютейшена {#parameters}

`flow.Parameters` — параметры из спеки, оставленные несериализованными: как выглядит конфигурация компьютейшена, знает только он сам.

```go
var waitForActions bool
if err := rt.Parameters().Get("wait_for_actions", &waitForActions); err != nil {
    return err
}
```

`Get(name, dst)` десериализует параметр из YSON в `dst` и возвращает `flow.ErrParameterNotFound`, если параметра нет. Наличие параметра проверяется методом `Has(name)`, список заданных имён возвращает `Names()`.

### Вотермарки {#watermarks}

```go
// Минимальный вотермарк по всем входным стримам
minWatermark := rt.MinWatermark()

// Вотермарк конкретного стрима
watermark, ok := rt.Watermark("stream_id")
```

`rt.Watermark` возвращает вторым значением признак того, что запрос сообщил вотермарк этого стрима. `rt.MinWatermark()` равен нулю, если запрос не сообщил ни одного вотермарка: событийное время ещё не сдвинулось.

## OutputCollector {#output-collector}

[Исходный код]({{source-root}}/yt/go/flow/output.go)

`flow.OutputCollector` используется для отправки результатов обработки:

| Метод | Описание |
| --- | --- |
| `out.AddMessage(msg)` | Добавить выходное сообщение (значение `flow.Message`, полученное через `builder.Finish()`) |
| `out.AddUndistributedMessage(msg)` | Добавить source-сообщение с `distribute = false` |
| `out.AddTimer(timer)` | Поставить [таймер](../../flow/concepts/glossary.md#timer) на обрабатываемый ключ |
| `out.WithParentIDs(parentIDs...)` | Вернуть коллектор, пишущий в отдельную группу с указанной [родословной](../../flow/concepts/lineage.md) |

Пример создания выходного сообщения и таймера:

```go
func (*myFunction) OnMessage(
    ctx context.Context,
    rt flow.Runtime,
    msg flow.ExtendedMessage,
    out flow.OutputCollector,
) error {
    output := flow.NewYSONMessage[outputMessage]("output_stream")
    output.Field = value
    encoded, err := flow.ConvertFrom(rt, output)
    if err != nil {
        return err
    }
    out.AddMessage(encoded)

    // Создание таймера
    out.AddTimer(flow.TimerRequest{TriggerTimestamp: 1000, EventTimestamp: 500})
    return nil
}
```

Поле `StreamID` в `flow.TimerRequest` выбирает стрим таймеров; пустое значение означает единственный стрим таймеров пайплайна.

`OutputCollector` не рассчитан на конкурентное использование: коллектор принадлежит горутине, обслуживающей запрос.

## ExtendedMessage {#extended-message}

Входящее [сообщение](../../flow/concepts/glossary.md#message) (`flow.ExtendedMessage`) содержит:

- `msg.ConvertTo(&value)` — преобразование payload сообщения в структуру со встроенным `flow.YSONMessage`.
- `msg.Key` — [ключ](../../flow/concepts/glossary.md#key) сообщения из `group_by_schema`; структура ключа заполняется через `msg.Key.ConvertTo(&key)`.
- `msg.StreamID` — идентификатор входного [стрима](../../flow/concepts/glossary.md#stream-and-computation) (`string`).
- `msg.EventTimestamp` — event timestamp сообщения (`uint64`).
- `msg.SystemTimestamp` — время создания сообщения (`uint64`).
- `msg.ID` — идентификатор сообщения, присвоенный воркером (`string`).

Для динамических схем можно работать с `msg.Payload` напрямую. Этот низкоуровневый API предоставляет аксессоры `Int64`, `Uint64`, `Float64`, `Bool`, `String`, `Bytes`, `Any(column, dst)`, `Has(column)` и `Columns()`.

## Timer {#timer}

Значение [таймера](../../flow/concepts/glossary.md#timer) (`flow.Timer`) содержит:

- `timer.Key` — [ключ](../../flow/concepts/glossary.md#key) таймера: `timer.Key.String("host")`.
- `timer.StreamID` — идентификатор стрима таймера (`string`).
- `timer.TriggerTimestamp` — время срабатывания (`uint64`).
- `timer.EventTimestamp` — event timestamp (`uint64`).

Визит `flow.Visit` устроен так же, но без времени срабатывания: он несёт `Key`, `StreamID` и таймстемпы. Подробнее — [Key Visitor Streams](../../flow/concepts/key_visitor.md).

## Конфигурация ресурса CompanionManager {#companion-manager}

Для запуска Go-компаньона необходимо объявить ресурс `CompanionManager` в статической спеке:

```yson
"CompanionManager" = {
    "resource_class_name" = "NYT::NFlow::NCompanion::TCompanionManager";
    "parameters" = {
        "entrypoint" = {
            "executable" = "./go_companion";
        };
    };
    "dependencies" = {};
};
```

Параметр `resource_class_name` указывает на класс ресурса, который будет осуществлять запуск компаньона.
В случае Go-компаньона `resource_class_name` всегда должен быть `NYT::NFlow::NCompanion::TCompanionManager`.

Процесс компаньона описывается параметром `entrypoint` (`executable`, `args`, `env`); воркер сам запускает компаньон и следит за его жизненным циклом. При [запуске пайплайна с хоста](getting-started.md#launch) через `pipeline.Run()` заполнять `entrypoint` вручную не нужно: Go-бинарь сам прописывает `entrypoint = {"executable" = "./go_companion"}`, а `flow_server` доставляет бинарь в джобу под этим именем.

Параметр `companion_process_count` Go-компаньоном принимается и валидируется, но ничего не задаёт: пре-форк нужен Python из-за GIL, а Go-компаньон обслуживает запросы конкурентно горутинами. Подробнее — [Параллелизм компаньона](getting-started.md#companion-process-count).

Подробнее про спеку в разделе [Spec, DynamicSpec и Config](../../flow/concepts/spec.md).

## См. также

- [Computation (концепция)](../../flow/concepts/computation.md)
- [Быстрый старт (Go)](getting-started.md)
- [Работа со стейтами (Go)](state.md)
- [Флаг distribute (Go)](distribute.md)
- [Companion](../../flow/concepts/companion.md)
