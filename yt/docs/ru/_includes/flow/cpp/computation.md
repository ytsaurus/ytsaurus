# Computation в {{product-name}} Flow (C++)

{% note info %}

На этой странице описаны особенности реализации Computation на C++. Языконезависимое описание концепции см. в разделе [Computation](../../../flow/concepts/computation.md).

{% endnote %}

В этом разделе приведено описание базовых классов `Computation` и важные детали реализации.

Во Flow в данный момент реализовано четыре базовых класса `Computation`:

- `TTransformComputation`
- `TTransformOrderedSourceComputation`
- `TSwiftOrderedSourceComputation`
- `TSwiftMapComputation`

Классы, содержащие `Swift` в названии, реализуют принцип [Swift](../../../flow/concepts/swift.md). Подробнее — в разделе [Swift](../../../flow/concepts/swift.md).

## Общее

- При наследовании от базового класса можно расширить параметры `Computation` с помощью макросов:

  * `YT_FLOW_EXTEND_PARAMETERS`
  * `YT_FLOW_EXTEND_DYNAMIC_PARAMETERS`

  В них можно передать `yson struct` для парсинга `parameters` из `ComputationSpec`, эта структура должна быть отнаследована от соответствующей структуры родительского класса.

- Все классы предоставляют методы:

  * `GetContext` — для получения `TComputationContext`.
  * `GetSpec` и `GetDynamicSpec` — для получения полной спеки соответствующего `Computation`;
  * `GetParameters` и `GetDynamicParameters` — для получения структурированных `parameters`.

    Хотя методы `GetParameters` и `GetDynamicParameters` возвращают результат парсинга, сам парсинг происходит только при реконфигурации спеки. Если спека не менялась, повторный вызов метода возвращает уже подготовленный объект, то есть метод не нагружает систему дополнительно.

- Конструктор нужно делать максимально простым без создания сложных объектов.
- Логгирование внутри `Computation` стоит делать с использованием `YT_LOG_*`. В классе уже есть подготовленный и заполненный необходимой для дебагга информацией объект типа `NLogging::TLogger`.
- Аналогично, для сбора различных метрик нужно использовать `NProfiling::TProfiler` и заранее подготовленный объект `GetContext()->Profiler`.
- Метод `DoInit` стоит использовать для сложной инициализации (например, инициализации объектов для работы со стейтами).
- Метод `DoSync` может использоваться для сохранения данных вручную в транзакцию `YT`. Однако, более правильно использовать [Sink](../../../flow/concepts/glossary.md#sink) или [ExternalState](../../../flow/cpp/state.md#external-state). Прямая работа с `DoSync` в `SwiftComputation` может привести к нежелаемому поведению. В `Transform` метод является безопасным, однако менее удобным.
- У каждого `Computation` есть семейство методов `DoProcess`. Они принимают либо `IInputContextPtr input` с методами `GetMessages` и `GetTimers`, либо конкретное сообщение или [таймер](../../../flow/concepts/glossary.md#timer). А объект `IOutputCollectorPtr output` предназначен для сбора выходных сообщений и таймеров — подробнее см. [OutputCollector](#output-collector).
- Все Computation'ы берут на себя заполнение всех метаполей `message` и `timer`, включая заполнение `StreamId` или `timer.Key`, если нет двусмысленности. Для создания сообщений можно использовать метод `MakeMessageBuilder`.
- Всё выполнение кода в рамках Computation'ов строго однопоточно и выполняется в рамках `GetContext()->SerializedInvoker`. Многопоточность достигается за счёт увеличения числа [партиций](../../../flow/concepts/glossary.md#partition). Если тем не менее нужно выполнить некоторый код многопоточно, то стоит использовать `GetContext()->PoolInvoker`, но при этом необходимо дождаться результатов выполнения в рамках соответствующего метода.
- Есть возможность конвертировать входные сообщения в `NYTree::TYsonStruct`. Для этого необходимо:
  * Завести класс-наследник от `TYsonMessage` (это специальный наследник `NYTree::TYsonStruct`).
  * Зарегистрировать его в глобальном реестре с помощью `YT_FLOW_DEFINE_YSON_MESSAGE`.
  * В функции `main` необходимо будет создать объект типа `TSimpleSpecBuilder` и зарегистрировать в нём соответствующие `stream_id`.
  * При использовании `TSimpleRunnerProgram` можно передать данный `TSimpleSpecBuilder` сразу в конструктор `TSimpleRunnerProgram`.
  * При самостоятельной реализации `main` необходимо будет передать в `TSimpleSpecBuilder` спеки для обогащения информацией о потоках.
  * Заполнять `spec/streams` в случае использования `TYsonMessage` самостоятельно не нужно — вся информация будет выведена из зарегистрированных `TYsonMessage + stream_id` с помощью `TSimpleSpecBuilder`.
  * В `Computation` будут доступны методы `ConvertToYsonMessage(message)->As<Type>()` и `ConvertToMessage(ysonMessage)` для преобразований `TMessage => TYsonMessage` и обратно.

### OutputCollector {#output-collector}

Объект `IOutputCollectorPtr output` передаётся в методы `DoProcess*` и предназначен для отправки результатов обработки:

| Метод | Описание |
| --- | --- |
| `output->AddMessage(message)` | Добавить выходное сообщение (объект `TMessage`, полученный через `MakeMessageBuilder().Finish()`) |
| `output->AddTimer(timer)` | Добавить [таймер](../../../flow/concepts/glossary.md#timer) |
| `output->SetParents(parentIds)` | Задать parent ID для отслеживания [lineage](../../../flow/concepts/lineage.md). Возвращает новый `IOutputCollectorPtr` с привязанным контекстом lineage |

`SetParents` рекомендуется использовать, когда выходное сообщение логически произведено от конкретного подмножества входных, а не от всего батча. Либо использовать небатчевые `DoProcessMessage` и `DoProcessTimer` — они устанавливают lineage автоматически.

### TMessage {#tmessage}

Структура `TMessage`, используемая в методах `DoProcessMessage` и `AddMessage`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TMessageSerializer.md) %}

## TTransformComputation

Предназначен для произвольных `Transform` преобразований входных данных. Не умеет работать с `Source`. Результат работы обязательно сохраняется в YT, поэтому нет требований на какую-либо детерминированность преобразований.

Свойства `TTransformComputation`:

- Может писать в YT «вхолостую», то есть без реальных изменений, перезаписывая существующее содержимое. Ожидается, что такой поток будет создавать незначительную нагрузку.

Пример работы с `TTransformComputation`:

```cpp
class TMyComputation
    : public TTransformComputation
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TMyParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicMyParameters);

    using TTransformComputation::TTransformComputation;

    void DoInit() override
    {

    }

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        TMyParametersPtr parameters = GetParameters();
        TDynamicMyParametersPtr dynamicParameters = GetDynamicParameters();
        ...
        output->AddTimer(TSystemTimestamp(message.EventTimestamp.Underlying() + TDuration::Minutes(5).Seconds()));
        ...
    }

    void DoProcessTimer(const TTimer& timer, IOutputCollectorPtr output) override
    {
        ...
        auto builder = MakeMessageBuilder();
        builder.Payload().SetValue(...);
        output->AddMessage(builder.Finish());
        ...
    }

    void DoSync(NApi::ITransactionPtr transaction) override
    {
        ...
        transaction->ModifyRows(...);
        ...
    }
};
```

### TTimer {#ttimer}

Структура `TTimer`, используемая в методах `DoProcessTimer` и `AddTimer`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TTimerSerializer.md) %}

### ProcessingMode {#processing-mode}

У `TransformComputation` есть параметр `parameters/processing_mode`, позволяющий снижать гарантии процессинга в обмен на снижение нагрузки на {{product-name}}.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_EProcessingMode.md) %}

### TPassthroughComputation

Наследник `TTransformComputation`. Реализует [passthrough-компьютейшен](../../../flow/concepts/computation.md#passthrough) — создан в основном для демонстрации возможностей.

Особенности:

- Не имеет переопределяемых методов.
- Не может иметь более одного выхода.
- Все входящие `input` потоки превращаются в `output` путём конвертации сообщения с помощью `ConvertMessageToNewSchema`.

### TTransformOrderedSourceComputation {#ttransformorderedsourcecomputation}

`TTransformOrderedSourceComputation` (`yt/yt/flow/library/cpp/computation/transform_ordered_source_computation.h`) обрабатывает сообщения `source` произвольной пользовательской логикой — парсинг, фильтрация, разворачивание одного входного сообщения в несколько выходных. Пользователь переопределяет те же методы обработки, что и у `TTransformComputation`, но на вход приходят сообщения одного упорядоченного `Source`, а не `input`-стримов. Класс заменяет связку `TSwiftPassthroughOrderedSourceComputation` → `TTransformComputation`, когда единственная задача промежуточного компьютейшена — обработать данные источника.

Результат трансформации материализуется в {{product-name}} так же, как у `TTransformComputation`:

- Выходные сообщения получают уникальные `MessageId` и надёжно сохраняются в {{product-name}} до распределения в downstream. После рестарта ещё не доставленные материализованные сообщения дораспределяются с теми же `MessageId`, а не вычисляются заново, поэтому требований к детерминированности трансформации нет.
- Сообщение можно добавить в `output` с явным флагом `distribute`, например `output->AddMessage(std::move(message), /*distribute*/ false)`. Такое сообщение не публикуется в downstream, но участвует в оценке вотермарка наравне с публикуемыми: генератор вотермарка регистрирует чтение по полному набору выходных сообщений ещё до применения фильтра публикации, поэтому вотермарк можно корректно оценивать по полному потоку, даже когда значимая его часть отфильтровывается. Смещение источника в любом случае продвигается в транзакции эпохи.
- Смещение `source`, материализованный выход и стейты коммитятся в одной транзакции эпохи, поэтому обработка каждого сообщения источника применяется ровно один раз.

Переопределяемые методы:

* `DoInit(IJobInitContextPtr initContext)` — инициализация, в том числе создание клиентов стейта;
* `DoProcess(IInputContextPtr input, IOutputCollectorPtr output)` — батч-версия обработки;
* `DoProcessMessage(const TMessage& message, IOutputCollectorPtr output)` — самый частый выбор для обработки без стейта; вариант `DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output)` даёт доступ к метаполям исходного сообщения, в том числе к `message->Key` для обращения к стейту;
* `DoSync(IRetryableTransactionPtr transaction)` — ручная запись в транзакцию эпохи.

Собственный стейт пользователь заводит ровно так же, как в `TTransformComputation` (см. [Работа со стейтами](../../../flow/cpp/state.md#internal-state)): поле `TMutableStateKeyClient<T>`, инициализация `initContext->InitClient(...)` в `DoInit` и аксессор `GetState(message->Key)` при обработке. Перед вызовом `DoProcess` фреймворк сам загружает стейт для ключей сообщений текущей эпохи. Инстанс компьютейшена всегда привязан к единственному `source`-ключу, поэтому все сообщения эпохи несут один и тот же ключ и обращаются к одной и той же строке стейта:

```cpp
class TMyComputation
    : public TTransformOrderedSourceComputation
{
public:
    using TTransformOrderedSourceComputation::TTransformOrderedSourceComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitClient(StateClient_, "my_state");
    }

    void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output) override
    {
        auto state = StateClient_.GetState(message->Key);
        state->Counter += 1;
        ...
    }

private:
    TMutableStateKeyClient<TMyState> StateClient_;
};
```

Стейт-клиенты, созданные через `IJobInitContext`, фреймворк синхронизирует в транзакции эпохи атомарно со смещением `source`, поэтому обычная мутация (например, инкремент счётчика) корректна exactly-once — дополнительная дедупликация по `MessageId` не нужна.

Спека компьютейшена проверяется при запуске; следующие поля приводят к ошибке валидации:

* `input`-стримы;
* [таймеры](../../../flow/concepts/glossary.md#timer);
* [key-visitor-стримы](../../../flow/concepts/key_visitor.md);
* непустой `group_by_schema`;
* `external_state_managers`;
* `external_state_joiners`, у которых не задан `join_on/key_schema_override` (ключ сообщения источника не описывается `group_by_schema`, поэтому схема ключа должна быть задана явно).

`watermark_strategy` поддерживается: `watermark_generator` оценивает вотермарки источника, `watermark_alignment` выравнивает чтение источника относительно других потоков (`read_delays` задерживают чтение, а не публикацию), `event_timestamp_assigner` назначает `event_timestamp` выходным сообщениям. Атомарность коммита смещения `source`, материализованных выходных сообщений и стейта от выравнивания не зависит. `skip_if_expression` также поддерживается.

`skip_if_expression` применяется до обработки, но после того, как входной батч учтён в метриках и в подсчёте опоздавших сообщений: отфильтрованное сообщение не попадает ни в стейт, ни в выход. На оценку вотермарка оно тоже не влияет: генератор регистрирует чтение только по выходным сообщениям, поэтому полностью отфильтрованный батч не сдвигает `EventWatermark`, а на длинной серии таких батчей вотермарк партиции стоит на месте. Маркеры `EventWatermark` во входных записях источник учитывает при чтении независимо от фильтра, но при `use_source_watermark = false` (значение по умолчанию) вотермарк источника только ограничивает оценку сверху и вперёд её не двигает; единственным источником вотермарка партиции он становится при `use_source_watermark = true`. Это полностью совпадает с поведением `TSwiftOrderedSourceComputation`.

Логику можно не наследовать от класса, а написать как [process function](../../../flow/cpp/process-functions.md) и указать в спеке адаптер `NYT::NFlow::TProcessFunctionTransformOrderedSourceComputation`: он исполняет функцию поверх `TTransformOrderedSourceComputation` — с той же материализацией выхода, теми же стейтами и той же валидацией спеки.

Пример — `NYT::NFlow::NExample::TLogParserProcessFunction` из [`examples/cpp/log_parser`]({{source-root}}/yt/yt/flow/examples/cpp/log_parser): разбирает строку лога на записи, эмитит YSON-структуру `TLogRecordMessage` (`level`, `text`, `worst_level_so_far`) и ведёт стейт `TWorstSeverityState` — бегущий максимум severity по партиции источника. Подробнее, вместе с полным исходным кодом, — в разделе [Log Parser](../../../flow/cpp/examples/log_parser.md).

#### TProtoTransformOrderedSourceComputation {#tprototransformorderedsourcecomputation}

Хелпер `NYT::NFlow::TProtoTransformOrderedSourceComputation<TProto>` (`yt/yt/flow/library/cpp/parsers/proto.h`) снимает с пользователя ручной парсинг `Protobuf` — аналог `TProtoSwiftSourceComputation<TProto>` для `TSwiftOrderedSourceComputation`.

`DoProcessMessage` реализован за пользователя: он читает строковую колонку `parameters/data_column` сырого сообщения `source` (по умолчанию `"data"`) и разбирает её в `TProto`. Пользователь переопределяет один из хуков:

* `DoProcessProto(TProto&& proto, IOutputCollectorPtr output)` — на успешный разбор, без доступа к исходному сообщению;
* `DoProcessProto(const TInputMessageConstPtr& inputMessage, TProto&& proto, IOutputCollectorPtr output)` — та же ситуация, но с доступом к исходному сообщению `source`;
* `DoProcessUnparsed(const TInputMessageConstPtr& inputMessage, TError error, IOutputCollectorPtr output)` — значение колонки `data_column` отсутствует (`null`) либо `Protobuf`-разбор бросил исключение; по умолчанию перебрасывает `error` дальше, поведение можно переопределить (например, чтобы молча отбросить невалидные сообщения). Пустая, но присутствующая строка — не то же самое, что отсутствующее значение: она успешно разбирается в сообщение со значениями по умолчанию, если у `TProto` нет обязательных полей, и в этом случае попадает в `DoProcessProto`, а не в `DoProcessUnparsed`.

Ответственность за ошибку разделена: ошибка самого разбора маршрутизируется в `DoProcessUnparsed`, а исключение из `DoProcessProto` пробрасывается наружу и прерывает эпоху — ничего не коммитится. К моменту такого исключения стейт мог быть уже частично изменён, и эту мутацию нельзя молча проглотить как «неразобранное» сообщение.

Собственный стейт заводится так же, как у `TTransformOrderedSourceComputation`, — через `TMutableStateKeyClient<T>` в `DoInit`; ключ для `GetState` — это `inputMessage->Key`, поэтому стейтовому компьютейшену подходит хук `DoProcessProto(const TInputMessageConstPtr&, TProto&&, IOutputCollectorPtr)`.

Пример — `NYT::NFlow::NExample::TProtoLogParserComputation` из [`examples/cpp/proto_parser`]({{source-root}}/yt/yt/flow/examples/cpp/proto_parser): разбирает `TLogRecordProto`, эмитит `TLogRecordMessage` (`level`, `text`, `seen_at_level`) и ведёт стейт `TLevelCountsState` — счётчик записей каждого уровня по партиции источника. Счётчик неидемпотентен к повторной обработке и корректен ровно потому, что стейт коммитится в одной транзакции со смещением `source`. Подробнее — в разделе [Proto Parser](../../../flow/cpp/examples/proto_parser.md).

## TSwiftMapComputation

Реализует детерминированный простой `Map` без материализации результатов в YT.

Особенности:

- Не поддерживает `sources` и `sinks`.
- Поддерживает `timer_streams` и `key_visitor_streams` только для работы со стейтом: эмит выходных сообщений из обработки таймера или визита запрещён, поэтому output-стримы не могут зависеть от таймер- и visit-стримов в `streams_dependency`.
- Должен возвращать один и тот же результат (включая порядок) для каждой входной строки. Если результат меняется при повторных запусках, могут возникать разные отрицательные эффекты. Возможно, что отдельные части системы обработают разные версии выхода, вплоть до дубликатов, если меняется значение полей для последующих `group-by`.
- Как следствие, у каждого результирующего сообщения должен быть ровно один родитель.

### TSwiftPassthroughComputation

Наследник `TSwiftMapComputation`. Аналогичен `TPassthroughComputation`: просто превращает `input` в `output` путём приведения сообщений к новой схеме.

## TSwiftOrderedSourceComputation

Основной класс для чтения данных из внешних источников. Требует, чтобы поток данных из каждого инстанса был упорядочен.

Особенности:

- Должен быть ровно один `source`
- `Source` должен быть наследником `IOrderedSource`.
- Может использовать `watermark_strategy/event_timestamp_assigner` для назначения `event_timestamp` выходным сообщениям при условии указания колонки. Иначе в качестве `event_timestamp` выходного сообщения будет взят `event_timestamp` сообщения из `source` — то есть время создания исходного сообщения.
- Использует `watermark_strategy/watermark_generator` для оценки [вотермарков](../../../flow/concepts/glossary.md#timestamps-and-watermarks) входных источников.
- Использует `watermark_strategy/watermark_alignment` для выравнивания чтения потока относительно других потоков.
- Позволяет отфильтровать часть событий: сообщение, добавленное в `output` с `distribute=false`, не публикуется, но всё равно учитывается при оценке вотермарка. Это позволяет оценивать вотермарк с использованием полного потока даже в случаях, когда значимая часть потока отфильтровывается.
- `system_timestamp` назначается в момент регистрации сообщения в `output`.
- Надёжно сохраняет часть данных в YT для гарантированного восстановления всей метаинформации. Сами сообщения в YT не сохраняет.
- Может писать в YT «вхолостую», то есть без реальных изменений. Предполагается, что такой поток должен создавать минимальную нагрузку.

### TSwiftPassthroughOrderedSourceComputation

Наследник `TSwiftOrderedSourceComputation`. Аналогичен `TPassthroughComputation`: преобразует `source` в `output` путём приведения сообщений к новой схеме.

## FAQ

### Как настроить Source и Sink? {#source-sink-configuration}

`Source` и `Sink` конфигурируются в спеке `Computation` через секции `sources` и `sinks` соответственно. Каждый `Source`/`Sink` задаётся отдельной подсекцией с указанием типа (например, `TQueueSource`{% if audience == "internal" %}, `TLogbrokerSource`, `TLogbrokerSink`{% endif %}) и параметров подключения.

Подробнее о доступных коннекторах см. раздел [Коннекторы](../../../flow/connectors/about.md).

### Как работает батчинг и партиционирование? {#batching-partitions}

Каждая партиция обрабатывается строго однопоточно. Многопоточность достигается за счёт увеличения числа партиций (`partition_count` в спеке). Батчевые методы `DoProcess(IInputContextPtr input, IOutputCollectorPtr output)` получают все сообщения и таймеры за текущую [эпоху](../../../flow/concepts/glossary.md#epoch), что позволяет оптимизировать обработку.

Если нужно выполнить код многопоточно внутри одной партиции, используйте `GetContext()->PoolInvoker`, но обязательно дождитесь завершения в рамках текущего метода.

### Как оценить нагрузку на внутренние таблицы? {#internal-tables-load}

Нагрузка на внутренние таблицы {{product-name}} зависит от типа `Computation` и количества партиций. Ниже приведена приблизительная оценка:

#|
|| **Тип Computation** | **Записей в эпоху на партицию** | **Комментарий** ||
|| `TTransformComputation` | ~2-4 | Запись стейтов + коммит ||
|| `TTransformOrderedSourceComputation` | ~2-4 | Материализация выхода + оффсеты + стейты ||
|| `TSwiftMapComputation` | 0 | Не пишет в YT ||
|| `TSwiftOrderedSourceComputation` | ~1-2 | Метаинформация для восстановления ||
|#

Общая нагрузка = записей на партицию * количество партиций * частота эпох. Для [пайплайна](../../../flow/concepts/glossary.md#pipeline) с 1000 партиций и эпохой в 1 секунду `TTransformComputation` создаст ~2000-4000 записей/с.

## Состояния пайплайна {#pipeline-state}

Возможные состояния пайплайна (тип `EPipelineState`):

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_EPipelineState.md) %}

## См. также

- [Computation (концепция)](../../../flow/concepts/computation.md)
- [Работа со стейтами (C++)](../../../flow/cpp/state.md)
- [Быстрый старт (C++)](../../../flow/cpp/getting-started.md)
