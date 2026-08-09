# Быстрый старт в {{product-name}} Flow (Go)

Поддержка вычислений на Go во Flow реализуется через механизм [компаньонов](../../flow/concepts/glossary.md#companion). Go-код выполняется в отдельном gRPC-процессе, который взаимодействует с C++ [воркером](../../flow/concepts/glossary.md#worker).

[Исходный код Go SDK для Flow]({{source-root}}/yt/go/flow)

[Примеры]({{source-root}}/yt/yt/flow/examples/go)

SDK импортируется как `a.yandex-team.ru/yt/go/flow`.

## Архитектура приложения {#architecture}

Любой [пайплайн](../../flow/concepts/glossary.md#pipeline) Flow состоит из трёх составных частей:

- `Runner` — запускает пайплайн и устанавливает новую версию [спеки](../../flow/concepts/glossary.md#spec-and-dynamic-spec).
- `Controller` — управляет работой пайплайна.
- `Worker` — выполняет непосредственно обработку данных.

Go-бинарь пайплайна играет две роли: запущенный с хоста, он выступает раннером, а запущенный воркером в джобе — компаньоном. Какая из ролей нужна, определяется по окружению, см. [Node companion](#node-companion).

## Pipeline API {#pipeline-api}

Go SDK предоставляет единый подход для настройки компаньона — тип `flow.Pipeline`. Он позволяет зарегистрировать [компьютейшены](../../flow/concepts/glossary.md#stream-and-computation) и запустить gRPC-сервер компаньона:

```go
pipeline := flow.NewPipeline()
pipeline.Add(flow.NewRowComputation("mapper", &wordCountMapper{}))

if err := pipeline.Run(); err != nil {
    fmt.Fprintf(os.Stderr, "word_count: %v\n", err)
    os.Exit(1)
}
```

Конструкторы вида `flow.NewRowComputation(computationID, fn)` связывают функцию обработки с компьютейшеном с указанным идентификатором, а `pipeline.Add(computations ...*flow.Computation)` регистрирует их в пайплайне. Идентификатор должен совпадать с `computation_id` в [спеке](../../flow/concepts/glossary.md#spec-and-dynamic-spec) пайплайна.

`Pipeline` собирается из одной горутины и после этого передаётся в `Run`. Набор компьютейшенов фиксируется на старте сервера: воркер запрашивает его один раз, поэтому компьютейшен, добавленный после старта, воркеру уже не виден.

Типизированные YSON-стримы регистрируются через `pipeline.AddStreams(flow.NewYSONStream[T](id))`. Структура `T` встраивает `flow.YSONMessage`, а колонки схемы выводятся из её `yson`-тегов. При запуске с хоста `Run` добавляет эти схемы в `spec.streams`; вручную дублировать их в `pipeline.yson` не нужно. Подробнее — в разделе [Типизированные YSON-сообщения](computation.md#typed-yson-messages).

## Computation и SourceComputation {#computation-and-source}

Для создания [компьютейшена](../../flow/concepts/glossary.md#stream-and-computation) на Go необходимо выбрать подходящий конструктор, соответствующий [типу Computation-а в C++](../../flow/concepts/companion.md#vidy-computation-ov-dlya-raboty-s-kompanonami):

- `flow.NewRowComputation(id, fn)` и `flow.NewBatchComputation(id, fn)` — для `TTransformCompanionComputation` и `TSwiftMapCompanionComputation`.
- `flow.NewRowSourceComputation(id, fn)` и `flow.NewBatchSourceComputation(id, fn)` — для `TSwiftOrderedSourceCompanionComputation` и `TTransformOrderedSourceCompanionComputation`.

```go
// SourceComputation для чтения данных из источника
pipeline.Add(flow.NewRowSourceComputation("reader", &eventMapper{}))

// Computation для обработки данных
pipeline.Add(flow.NewRowComputation("reducer", &eventReducer{}))
```

У конструкторов два обязательных параметра:

- **id** — по нему происходит маппинг запросов между [воркером](../../flow/concepts/glossary.md#worker) и компаньоном.
- **fn** — значение с логикой обработки [сообщений](../../flow/concepts/glossary.md#message). Реализует интерфейс `flow.RowFunction` или `flow.BatchFunction`; обычную функцию можно передать через адаптеры `flow.RowFunc` и `flow.BatchFunc`.

Тип компьютейшена — это то, чем он был создан: сорс отличается от трансформа только тем, каким он объявляется воркеру. Компьютейшен без функции обработки отвергается на месте — конструктор паникует, поскольку сообщить о такой ошибке по протоколу уже некуда.

Фильтрация сообщений в source-компьютейшенах выполняется через флаг [distribute](distribute.md) при эмите сообщения из Process Function.

## Process Function {#process-function}

Есть два вида ProcessFunction:

- `flow.RowFunction` — получает [сообщения](../../flow/concepts/glossary.md#message) по одному, метод `OnMessage`. [Таймеры](../../flow/concepts/glossary.md#timer) и визиты обрабатываются реализацией интерфейсов `flow.RowTimerFunction` (`OnTimer`) и `flow.RowVisitFunction` (`OnVisit`) на том же типе.
- `flow.BatchFunction` — получает весь батч сообщений сразу, метод `OnMessages`; таймеры и визиты — `flow.BatchTimerFunction` (`OnTimers`) и `flow.BatchVisitFunction` (`OnVisits`).

Компьютейшен объявляет только те обработчики, которые ему нужны. Воркер доставляет входы согласно спеке компьютейшена, а Go SDK пропускает таймеры и визиты, для которых соответствующий обработчик не реализован.

Каждый обработчик получает четыре аргумента:

```go
func (*wordCountMapper) OnMessage(
    ctx context.Context,
    rt flow.Runtime,
    msg flow.ExtendedMessage,
    out flow.OutputCollector,
) error
```

- `ctx` — контекст запроса, в котором пришёл вход: у него есть дедлайн, и он отменяется, когда воркер отказывается от батча.
- `rt` — `flow.Runtime`, через него доступны [стейты](state.md), параметры компьютейшена, [вотермарки](../../flow/concepts/watermarks.md) и схемы стримов.
- `msg` — входное сообщение вместе с ключом, по которому оно сгруппировано.
- `out` — `flow.OutputCollector` для эмита сообщений и постановки таймеров.

Возвращённая обработчиком ошибка прекращает обработку остатка батча: воркер повторит запрос целиком, поэтому частичный ответ привёл бы к повторному учёту уже обработанных входов.

Подробнее — в разделе [Computation (Go)](computation.md).

## Фильтрация сообщений {#message-filtering}

Чтобы отфильтровать сообщение в SourceComputation, эмитьте его с `out.AddUndistributedMessage(msg)` — оно не будет опубликовано дальше по графу, но останется учтённым при оценке watermark.

Подробнее — в разделе [Флаг distribute (Go)](distribute.md).

## Node companion {#node-companion}

Точка входа в Go-компаньон — функция `main`. В ней необходимо сконфигурировать компьютейшены через `flow.Pipeline` и вызвать `pipeline.Run()`. Функция `main` из [WordCount](examples/wordcount.md):

{% code '/yt/yt/flow/examples/go/word_count/main.go' lang='go' %}

Если пользовательским функциям нужны дополнительные ресурсы (словарь, кэш, HTTP-клиент и т. п.), `main` — подходящее место для их создания: они складываются в поля значения, которое связывается с компьютейшеном.

У `pipeline.Run()` два режима, которые выбираются автоматически по паре переменных окружения `YT_FLOW_MODE` и `YT_FLOW_COMPANION_CONFIG`:

- Не задана ни одна из них — процессу никто не сказал, что обслуживать, значит это запуск с хоста. `Run()` обогащает спеку пайплайна (см. [Запуск пайплайна](#launch)) и передаёт управление `flow_server`, поэтому из `Run()` управление не возвращается.
- Задана хотя бы одна — `flow_server` уже запустил этот же бинарь в джобе в роли компаньона. `Run()` поднимает gRPC-сервер компаньона и обслуживает зарегистрированные компьютейшены, пока воркер его не остановит.

Решает именно пара, а не один конфиг: процесс, которому воркер выставил `YT_FLOW_MODE`, но не передал конфиг, — это недонастроенный компаньон, и он должен отказаться обслуживать, а не уйти по ветке раннера и упасть на командной строке, которой ему не давали.

Один и тот же бинарь, таким образом, и запускает пайплайн, и работает компаньоном внутри джобы — отдельно деплоить компаньон не нужно.

Если жизненным циклом сервера нужно управлять самостоятельно (например, в тестах), вместо `Run()` используйте `pipeline.Server(opts...)`, который строит `flow.Server` по конфигу из окружения. По умолчанию сервер пишет ошибки запросов в `stderr`; опция `flow.WithLogger` заменяет этот логгер.

## Параллелизм компаньона {#companion-process-count}

Go-компаньон обслуживает запросы конкурентно: воркер обрабатывает партиции компьютейшена параллельно, и каждый запрос обслуживается собственной горутиной. Поэтому Go-компаньону не нужен пре-форк, который есть у Python (там он существует, чтобы обойти GIL): параметр `companion_process_count` в конфиге компаньона принимается и валидируется, но ничего не задаёт.

Если обработчик сам запускает дочерние горутины, используйте [`flow.Go`](computation.md#goroutines), чтобы их CPU и память учитывались за ту же джобу.

Go SDK пока не поднимает собственный HTTP-эндпоинт мониторинга. Для диагностики используйте метрики воркера и контроллера и логи компаньона.

{% note warning %}

Одно значение `Computation` — а значит и одна связанная с ним функция — обслуживает все запросы к этому идентификатору. Функция, которая хранит состояние между вызовами, синхронизирует его сама.

{% endnote %}

## Сборка с ya make {#build}

Проект с Go-компаньоном собирается через `ya make`. Бинарь пайплайна описывается модулем `GO_PROGRAM`, зависимости на SDK выводятся из импортов, отдельный `PEERDIR` для них не нужен:

```
GO_PROGRAM()

SRCS(
    main.go
    word_count_mapper.go
)

GO_TEST_SRCS(
    word_count_mapper_test.go
)

END()
```

Собрать бинарь пайплайна и `flow_server` можно одной командой:

```bash
cd yt/yt/flow
ya make examples/go/word_count bin/flow_server
```

## Запуск пайплайна {#launch}

Собранный бинарь запускается командой:

```bash
./word_count --config pipeline.yson --flow-bin <путь/к/flow_server>
```

Здесь происходит следующее:

- Go-бинарь читает `pipeline.yson`, обогащает спеку — прописывает в неё *самого себя* как Go-компаньон, который `flow_server` доставит в джобу, — и записывает расширенный конфиг во временный файл.
- После этого он через `execve` передаёт управление указанному `flow_server` (`flow_server --config <расширенный конфиг>`). Замена образа процесса, а не запуск дочернего, оставляет код возврата и сигналы запуска в распоряжении вызывающего.

Неизвестные флаги командной строки пропускаются, а не отвергаются: бинарь пайплайна — это ваша собственная программа, и она вправе объявлять свои флаги.

`flow_server` передаётся явно через `--flow-bin` и не встроен в Go-бинарь: так пайплайн остаётся лёгким, а версию `flow_server` выбирает тот, кто запускает пайплайн.

Весь запуск выполняет именно `flow_server`: он валидирует спеку, при необходимости создаёт vanilla-операцию, **устанавливает спеку пайплайна** (`set-pipeline-specs`) и стартует пайплайн. Go-сторона только *строит* и обогащает спеку и никогда не устанавливает её напрямую.

### Блок `vanilla` {#vanilla}

Если в `pipeline.yson` задан блок `vanilla` с `enable = %true`, `flow_server` запускает пайплайн как одну YT vanilla-операцию (контроллер + воркеры) и доставляет Go-бинарь в джобу как компаньон. Это запуск «одной кнопкой» — отдельно поднятый `flow_server` не нужен.

```yson
{
    "cluster_url" = "{{flow-example-cluster}}";
    "path" = "//home/flow-dev/go-word-count/pipeline";
    "spec" = { ... };
    "vanilla" = {
        "enable" = %true;
        "pool" = "yt-dev";
        "controller" = {
            "count" = 1;
            "cpu_limit" = 4;
            "memory_limit" = 12884901888;
        };
        "worker" = {
            "count" = 5;
            "cpu_limit" = 4;
            "memory_limit" = 12884901888;
        };
    };
}
```

Обязательные параметры: `pool` и `worker.count`. Для остальных полей (`cpu_limit`, `memory_limit`, число контроллеров и т. д.) есть разумные значения по умолчанию — полный список полей и их описание см. в [TVanillaConfig](../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaConfig) и [TVanillaTaskConfig](../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaTaskConfig).

Обогащение спеки выполняется именно для vanilla-запуска и состоит из двух правок:

- Бинарь пайплайна добавляется в `vanilla.worker.local_files` под именем `go_companion` — под этим именем `flow_server` доставляет его в сэндбокс джобы.
- Каждому ресурсу с `resource_class_name = "NYT::NFlow::NCompanion::TCompanionManager"` проставляется `parameters.entrypoint.executable = "./go_companion"`, то есть воркер сам запускает компаньон из сэндбокса.

{% note info %}

Пайплайн, запущенный не через vanilla-операцию, работает с компаньоном по пути на хосте, уже прописанному в его спеке, — обогащение в этом случае ничего не меняет.

{% endnote %}

### Обновление спеки запущенного пайплайна {#release}

`flow_server` — единственный компонент, который устанавливает спеку пайплайна; Go-сторона спеку только строит. Поэтому процесс выкатки изменений в уже запущенный пайплайн такой:

1. Пересобрать Go-бинарь (`ya make ...`).
2. Снова запустить `./word_count --config pipeline.yson --flow-bin <flow_server>`.

`flow_server` заново установит спеку и стартует пайплайн. Для vanilla-запуска используется стратегия make-before-break: новая операция подготавливается (бинарь загружается в YT-кэш), пока старая операция продолжает работать, после чего происходит переключение — старая операция завершается, и стартует уже подготовленная новая. Способом завершения старой операции управляет переменная окружения `YT_FLOW_GRACEFUL_UPDATE`: `1` (по умолчанию) — пайплайн останавливается (`stop`), `0` — ставится на паузу (`pause`).

## См. также

- [Computation (Go)](computation.md)
- [Работа со стейтами (Go)](state.md)
- [Тестирование (Go)](testing.md)
- [Примеры: Word Count (Go)](examples/wordcount.md)
- [Companion](../../flow/concepts/companion.md)
