# Тестирование в {{product-name}} Flow (Go)

{% note info %}

Данная страница описывает **юнит-тестирование** [компьютейшенов](../../flow/concepts/glossary.md#stream-and-computation) Go-[пайплайна](../../flow/concepts/glossary.md#pipeline) через харнесс `flowtest`, а также **интеграционное тестирование** полного пайплайна через `FlowTestGoBase`.

{% endnote %}

## Общая архитектура тестирования {#architecture}

В продакшене C++ [воркер](../../flow/concepts/glossary.md#worker) отправляет gRPC-запросы [компаньону](../../flow/concepts/companion.md), передавая [сообщения](../../flow/concepts/glossary.md#message), [таймеры](../../flow/concepts/glossary.md#timer), визиты, [стейты](state.md) и [вотермарки](../../flow/concepts/watermarks.md). Компаньон разбирает запрос, собирает по нему `flow.Job` и `flow.Runtime` и вызывает Process Function зарегистрированного компьютейшена.

В юнит-тестах место воркера занимает `flowtest.Harness` из пакета [`flowtest`]({{source-root}}/yt/go/flow/flowtest). Харнесс хранит то, что воркер сообщает компаньону, — стримы, схему ключа, объявленные стейты и параметры — и прогоняет компьютейшен через ту же джобу, тот же рантайм и ту же диспетчеризацию, что и сервер компаньона, вплоть до рендеринга ответа в проволочный формат. Поэтому сообщение в необъявленный стрим, незакодируемый ключ или стейт, записанный пустыми байтами, падают уже в юнит-тесте, а не в джобе.

Тестируется то же значение `*flow.Computation`, которое регистрируется в пайплайне: харнессу передаётся результат `flow.NewRowComputation` или родственного конструктора, а сорс отличается от трансформа только тем, чем он был создан, — сообщать об этом харнессу отдельно не нужно.

Ни кластера, ни gRPC-соединения, ни `flow_server` для юнит-тестов не требуется. Тесты пишутся на стандартном `testing`; в примерах для проверок используется [testify](https://github.com/stretchr/testify) (`require`).

## Зависимости {#dependencies}

Отдельный `PEERDIR` для харнесса не нужен: зависимости Go-модуля выводятся из импортов. Достаточно перечислить тестовые файлы в `GO_TEST_SRCS` модуля пайплайна:

{% code '/yt/yt/flow/examples/go/word_count/ya.make' lang='text' %}

И добавить рядом директорию `gotest` с модулем `GO_TEST_FOR`, через который тесты запускаются:

{% code '/yt/yt/flow/examples/go/word_count/gotest/ya.make' lang='text' %}

## Тестирование Process Function {#testing-process}

### Создание харнесса {#harness}

Харнесс создаётся функцией `flowtest.New(tb, computation, opts)`. Первый аргумент — `*testing.T` (подойдёт также `*testing.B` и `*testing.F`): обо всех ошибках использования харнесс сообщает через него, поэтому в тесте остаётся только то, что тест утверждает.

```go
h := flowtest.New(t, flow.NewRowComputation("mapper", &wordCountMapper{}), flowtest.Options{
    Streams:        map[string]flow.Schema{"words": flowtest.Schema("word:string")},
    KeySchema:      flowtest.Schema("word:string"),
    InternalStates: []string{wordStateName},
})
```

Поля `flowtest.Options`:

| Поле | Описание |
|------|----------|
| `Streams` | Стримы, по которым компьютейшен обменивается сообщениями, по идентификатору стрима. Читать и писать компьютейшен может только перечисленные здесь стримы. |
| `KeySchema` | Схема ключа, по которому сгруппированы входы. Компьютейшен без группировки оставляет поле пустым. |
| `InternalStates` | Имена [внутренних стейтов](internal-state.md), которые объявляет компьютейшен, — они доезжают до него как `parameters.internal_states`. |
| `ExternalStates` | Схемы [внешних стейтов](external-state.md), которыми компьютейшен владеет, по имени стейта. Имена — абсолютные пути, как того требует воркер. |
| `JoinedExternalStates` | Схемы внешних стейтов, которые компьютейшен читает, не владея ими. |
| `Parameters` | Карта `parameters` статической [спеки](../../flow/concepts/glossary.md#spec-and-dynamic-spec) — то, что компьютейшен читает через `rt.Parameters()`. |
| `DynamicParameters` | Карта `parameters` динамической спеки. |

Схема колонок собирается хелпером `flowtest.Schema("word:string", "count:int64")` — имена типов те же, что в {{product-name}}. Схему, которую так не описать, стройте через `flow.NewSchema` из `schema.Schema`.

Для типизированного YSON-стрима используйте ту же схему, которую регистрирует пайплайн: `flow.YSONMessageSchema[event]()`. Так структура со встроенным `flow.YSONMessage` описывает и колонки спеки, и входные строки теста.

Для обычной структуры без `flow.YSONMessage` остаётся `flowtest.SchemaOf(event{})`. Он следует общему `schema.Infer`: в частности, Go-строка становится `utf8`. Если схема должна дословно совпасть с уже существующей спекой, используйте `flowtest.Schema`.

### Входы {#inputs}

Входы одного батча строятся методами харнесса и передаются в `Process` одним вызовом:

| Метод | Что строит |
|-------|------------|
| `h.Key(flowtest.Row{...})` | Ключ по схеме `KeySchema`. |
| `h.Message(streamID, row)` | Сообщение без ключа — то, что получает компьютейшен без группировки. |
| `h.KeyedMessage(streamID, key, row)` | Сообщение вместе с ключом, по которому оно сгруппировано. |
| `h.Timer(key, triggerTimestamp)` | Сработавший таймер ключа. |
| `h.Visit(key)` | Визит ключа из key-visitor стрима. |
| `h.SetWatermark(streamID, watermark)` | Вотермарк стрима; держится до следующей установки. |

Каждому сообщению выдаётся собственный идентификатор, как это делает воркер. Таймстемпы остаются нулевыми: тесту, которому они нужны, достаточно проставить их на результате.

```go
msg := h.KeyedMessage("hits", key, flowtest.Row{"hit_id": "h1"})
msg.EventTimestamp = 1000
```

`h.Process(inputs ...flow.Input)` прогоняет компьютейшен по батчу и возвращает `*flowtest.Response`; если обработка вернула ошибку, тест падает. Стейт переживает прогон: то, что компьютейшен записал, применяется к стейту следующего прогона — ровно так воркер применяет дельту ответа перед отправкой очередного батча. Тесту, которому нужен чистый лист, следует построить новый харнесс.

### Полный пример {#unit-test-example}

Юнит-тесты маппера из [WordCount](examples/wordcount.md) — харнесс, батч сообщений и проверка внутреннего стейта:

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper_test.go' lang='go' lines='[BEGIN unit_test]-[END unit_test]' %}

### Ошибки обработки {#errors}

Ошибка, возвращённая обработчиком, прекращает обработку батча целиком: воркер повторит запрос, поэтому частичного ответа не бывает. Такой прогон проверяется методом `h.ProcessError`, который возвращает ошибку и падает, если обработка, наоборот, прошла успешно:

```go
err := h.ProcessError(h.Message("queue", flowtest.Row{"data": "}not json{"}))

require.ErrorContains(t, err, "parsing the data column")
```

Прогон, завершившийся ошибкой, не производит вывода и не меняет стейт — поэтому `ProcessError` и не возвращает `Response`.

### Таймеры и вотермарки {#timers-and-watermarks}

Таймер строится по ключу и времени срабатывания. Пайплайну с несколькими таймерными стримами нужный выбирается полем `StreamID` — пустое значение означает единственный таймерный стрим пайплайна:

```go
timer := h.Timer(key, closeTime)
timer.StreamID = timerStream

r := h.Process(timer)
```

Вотермарк стрима задаётся `h.SetWatermark` и держится для всех последующих прогонов. Так проверяется отбрасывание опоздавших данных: компьютейшен читает `rt.MinWatermark()`, минимум по входным стримам, поэтому не продвинувшийся стрим удерживает окно открытым для остальных.

```go
h.SetWatermark(hitStream, hitTime+3)
h.SetWatermark(actionStream, 0)
```

Полный набор тестов на окно с таймером и вотермарками — в [Wait Click Join]({{source-root}}/yt/yt/flow/examples/go/wait_click_join/join_function_test.go).

## Тестирование стейтов {#testing-states}

Стейт, с которым компьютейшен начинает прогон, кладётся в харнесс до вызова `Process`, а результат читается из `Response`. Подробнее о самих аксессорах — в разделе [State Accessor](state-accessor.md).

### Internal state {#internal-state}

Имя внутреннего стейта должно быть объявлено в `InternalStates`, иначе харнесс сообщит ровно ту же ошибку, что и рантайм в джобе.

| Метод | Что кладёт |
|-------|------------|
| `h.PutInternalState(name, key, data)` | Сырые байты, которые читает `flow.OpenRawState`. |
| `h.PutInternalStateYSON(name, key, value)` | Значение, сериализованное в YSON, — то, что читает `flow.OpenYSONState`. |
| `h.PutInternalStateProto(name, key, value)` | Сериализованное protobuf-сообщение для `flow.OpenProtoState`. |

Обратно стейт читается методами `Response`:

```go
var counter wordCountState
require.True(t, r.InternalStateYSON(wordStateName, key, &counter))
require.EqualValues(t, 1, counter.Count)
```

### External state {#external-state}

Внешний стейт, которым компьютейшен владеет, кладётся `h.PutExternalState(name, key, row)`, а читается как строка — `r.ExternalState` возвращает `flow.Payload`, `r.ExternalStateRow` — уже декодированную `flowtest.Row`.

{% note info %}

Внутренний стейт и присоединённый внешний стейт доезжают до прогона только для тех ключей, для которых что-то хранят. Внешний стейт, которым компьютейшен владеет, доезжает для каждого ключа батча, пустым там, где не хранится ничего: воркер резолвит собственный стейт для каждого переданного ключа — именно это позволяет писать стейт для ключа, увиденного впервые.

{% endnote %}

Юнит-тесты редьюсера из [Shuffle](examples/shuffle.md), который считает события во внешнем стейте:

{% code '/yt/yt/flow/examples/go/shuffle/event_reducer_test.go' lang='go' lines='[BEGIN reducer_unit_test]-[END reducer_unit_test]' %}

### Joined external state {#joined-external-state}

Присоединённый внешний стейт — стейт, который компьютейшен читает, не владея им, — кладётся `h.PutJoinedExternalState(name, key, row)` и читается через `r.JoinedExternalState` / `r.JoinedExternalStateRow`. Записать в него нельзя: ничего записанного в read-only стейт из ответа не выходит.

```go
h := flowtest.New(t, flow.NewRowComputation("lookup_join", &lookupJoin{}), flowtest.Options{
    Streams:   map[string]flow.Schema{"event": flowtest.Schema("key:uint64")},
    KeySchema: flowtest.Schema("hash:uint64", "key:uint64"),
    JoinedExternalStates: map[string]flow.Schema{
        referenceStateName: flowtest.Schema("hash:uint64", "key:uint64", "name:string"),
    },
})

h.PutJoinedExternalState(referenceStateName, key, flowtest.Row{"key": uint64(1), "name": "alice"})
```

Ключ, для которого строка не положена, до компьютейшена не доезжает — как и в проде, где воркер джойнит то, что нашёл, и ничего сверх того. Полный набор тестов — в [external_state_join]({{source-root}}/yt/yt/flow/examples/go/external_state_join/lookup_join_test.go).

## Анализ результатов {#analyzing-response}

`*flowtest.Response`, возвращённый `Process`, — это то, что произвёл прогон: собранный вывод и стейты в том виде, в каком они будут сохранены.

| Метод | Что возвращает |
|-------|----------------|
| `Groups()` | `[]flow.OutputGroup` — группы вывода в порядке появления. |
| `Messages()` | Выходные сообщения всех групп, по порядку. |
| `MessagesOn(streamID)` | Выходные сообщения одного стрима. |
| `Rows()` | Пейлоады выходных сообщений, декодированные в `flowtest.Row` и выровненные с `Messages()`. |
| `Distribute()` | Флаг [distribute](distribute.md) каждого сообщения, выровненный с `Messages()`. |
| `Timers()` | `[]flow.TimerRequest` — таймеры, которые компьютейшен попросил воркер поставить. |

Группа вывода — это происхождение (lineage) вывода, а не форма входа: `RowFunction` открывает по группе на вход, `BatchFunction` — одну на батч, а группы, в которые ничего не записали, отбрасываются.

Стейты читаются так:

| Метод | Что возвращает |
|-------|----------------|
| `InternalStateRaw(name, key)` | Байты, которые внутренний стейт хранит для ключа. |
| `InternalStateYSON(name, key, dst)` | Десериализует в `dst` YSON внутреннего стейта. |
| `InternalStateProto(name, key, dst)` | Десериализует в `dst` protobuf-сообщение внутреннего стейта. |
| `InternalStateReset(name, key)` | Прогон очистил стейт ключа. |
| `InternalStateWritten(name)` | Прогон писал в стейт: до воркера доезжает только записанное. |
| `InternalStateLen(name)` | Число ключей, для которых стейт читался или писался. |
| `ExternalState(name, key)`, `ExternalStateRow(name, key)` | Строка внешнего стейта — как `flow.Payload` и как `flowtest.Row`. |
| `ExternalStateReset(name, key)`, `ExternalStateWritten(name)`, `ExternalStateLen(name)` | То же для внешнего стейта. |
| `JoinedExternalState(name, key)`, `JoinedExternalStateRow(name, key)` | Строка присоединённого внешнего стейта. |

Стейт рапортуется таким, каким он будет сохранён: очищенная прогоном запись читается как отсутствующая, а отличить её от той, которой никогда не было, позволяет `*Reset`.

## Запуск юнит-тестов {#running-unit-tests}

Юнит-тесты — это тесты размера `SMALL`, кластер им не нужен.

{% if audience == "internal" %}

```bash
cd yt/yt/flow
ya test examples/go/word_count
```

Отфильтровать один тест можно по имени:

```bash
ya test examples/go/word_count -F 'TestCounterSurvivesTheBatch'
```

{% else %}

```bash
cd yt/yt/flow/examples/go/word_count
go test ./...
```

Отфильтровать один тест можно по имени:

```bash
go test ./... -run 'TestCounterSurvivesTheBatch'
```

{% endif %}

## Интеграционное тестирование с FlowTestGoBase {#e2e-tests}

Для полного интеграционного тестирования пайплайна (с реальными C++ воркерами, очередями и стримами) используется базовый класс `FlowTestGoBase` — Python-тест, запускающий тот же Go-бинарь, что поедет в прод.

Запускает пайплайн в таком тесте раннер, а не сам тест: Go-бинарь стартует как `./word_count --config pipeline.yson --flow-bin flow_server`, обогащает [спеку](../../flow/concepts/glossary.md#spec-and-dynamic-spec) и передаёт управление `flow_server`, который её устанавливает. Компаньон в джобе поднимает воркер — ровно как в проде.

### Зависимости {#integration-dependencies}

Интеграционному тесту нужны рецепт кластера, `DEPENDS` на бинарь пайплайна и `flow_server`, а также `DATA` со спекой. Полный `ya.make` теста из [WordCount](examples/wordcount.md):

{% code '/yt/yt/flow/examples/go/word_count/test/ya.make' lang='text' %}

### Настройка {#go-test-setup}

Тест наследуется от `FlowTestGoBase` и задаёт атрибут `GO_COMPANION_BINARY`:

{% code '/yt/yt/flow/examples/go/word_count/test/test_wordcount.py' lang='python' lines='[BEGIN test_setup]-[END test_setup]' %}

| Атрибут | Описание |
|---------|----------|
| `GO_COMPANION_BINARY` | Путь к бинарю Go-пайплайна: он же раннер, он же компаньон. |
| `VANILLA_WORKER_PORT_COUNT` | Число портов на воркер; по умолчанию `3` — rpc, мониторинг и порт, на котором воркер поднимает компаньон. |

Пайплайн запускается методом `start_flow_process_federation`, которому спека передаётся аргументом `--config`; `--flow-bin` базовый класс проставляет сам. Для локальной федерации он же прописывает в ресурсы компаньона путь к собранному бинарю, чтобы воркер запустил его с диска.

[Пример E2E-теста WordCount]({{source-root}}/yt/yt/flow/examples/go/word_count/test/test_wordcount.py)

{% note warning %}

Интеграционные тесты требуют развёрнутого кластера {{product-name}} и относятся к размеру `MEDIUM`, поэтому запускаются через `ya test -tt`. Для быстрой итерации используйте юнит-тесты, описанные выше.

{% endnote %}

{% include notitle [_](../../_includes/flow/testing-integration-body.md) %}

{% include notitle [_](../../_includes/flow/testing-test-param-body.md) %}

## См. также

- [Computation (Go)](computation.md)
- [Работа со стейтами (Go)](state.md)
- [State Accessor (Go)](state-accessor.md)
- [Примеры: Word Count (Go)](examples/wordcount.md)
- Если дорабатываете сам Flow — [Фреймворк для тестирования пайплайнов](../../flow/contributor/testing-framework.md).
