# Spec, DynamicSpec и Config в {{product-name}} Flow

Спека &mdash; основной способ описания желаемого [пайплайна](../../../flow/concepts/glossary.md#pipeline). Любой высокоуровневый API к Flow генерирует спеку пайплайна.

У пайплайна есть две спеки &mdash; `Spec` и `DynamicSpec`:

* `Spec` &mdash; статический объект, содержащий топологию всего пайплайна. Этот объект можно менять только при условии остановки пайплайна. Гарантируется, что в момент выполнения вся система имеет одинаковое представление о `Spec`.
* `DynamicSpec` &mdash; динамическая часть спеки. Её можно менять в любой момент, она применится ко всем частям системы асинхронно. В `DynamicSpec` обычно хранятся размеры буферов, таймауты, число партиций и т. п.

Также важной частью является `Config` ноды. В отличие от спеки, конфиг задаётся ровно один раз в рамках релиза и не подлежит динамическому изменению. Возможно, в будущем часть параметров конфига будут перемещены в спеки.

{% note warning %}

Все параметры во Flow имеют оптимальные значения по умолчанию. Не меняйте настройки, если вы не уверены, для чего они нужны и на что могут повлиять.

{% endnote %}

Значения по умолчанию можно найти поиском по имени параметра в кодовой базе Flow. В основном они сгруппированы в файле [spec.cpp]({{source-root}}/yt/yt/flow/library/cpp/common/spec.cpp).

## Spec

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TPipelineSpec.md) %}

### Computation

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TComputationSpec.md) %}

#### WatermarkStrategy {#watermark-strategy}

Рекомендуется заполнять у `SourceComputation`. Остальные `Computation` могут не поддерживать или поддерживать ограниченно.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TWatermarkStrategySpec.md) %}

##### EventTimestampAssigner {#event-timestamp-assigner}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TEventTimestampAssignerSpec.md) %}

##### WatermarkGenerator {#watermark-generator}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TWatermarkGeneratorSpec.md) %}

Настройки `IdlePartitions`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TIdlePartitionsSpec.md) %}

Настройки `UnavailablePartitionGroups`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnavailablePartitionGroupsSpec.md) %}

##### WatermarkAlignment {#watermark-alignment}

Модуль используется для выравнивания входных [потоков](../../../flow/concepts/glossary.md#stream-and-computation). Может приводить к полной остановке чтения в случае проблем с доступностью и/или оценкой `Watermark` у отдельных [партиций](../../../flow/concepts/glossary.md#partition) или `SourceComputation`.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TWatermarkAlignmentSpec.md) %}

##### WatermarkPercentile {#watermark-percentile}

Модуль используется для игнорирования старых данных в потоках при расчёте `EventWatermark`. Каждая партиция по каждому исходящему потоку поддерживает множество ещё необработанных сообщений. По умолчанию для расчета `EventWatermark` берется минимальное значение `EventTimestamp` среди всех сообщений (то есть учитывается `100%` событий).

Если событие попадает в `value` персентиль - оно безусловно учитывается для расчёта `EventWatermark`. Если событие не попадает в указанный персентиль, однако отличается от `EventTimestamp` этих событий не более, чем на `delay` - то оно тоже будет учтено для расчёта `EventWatermark`. Таким образом, алгоритм расчёта сводится к `EventWatermark = max(MinEventTimestamp, PercentileEventTimestamp - delay)`, где `MinEventTimestamp` - минимум по всем событиям, а `PercentileEventTimestamp` - значение `EventTimestamp` в соответствующем `value` персентиле.

Нужно учитывать, что `inflight` одного потока в рамках партиции может содержать в моменте десятки сообщений, поэтому значения вида `99`, `99.9` могут работать неотличимо от `100` - то есть не приводя к желаемому эффекту. Кроме того, даже если старых событий от всего потока меньше 1%, эти старые события могут быть распределены неравномерно между партициями. Поэтому разумнее выставлять значения вида `80`, `90` и т.п. Чтобы избежать негативных эффектов в виде неучёта свежих событий, существует параметр `delay`: благодаря ему будет теряться информация лишь о действительно старых событиях. Если таковых нет, то система будет работать так же, как и при `value`, равном `100`, без потери какой-либо информации.

Данный модуль влияет только на расчёт `EventWatermark` и никак не влияет на `SystemWatermark`.

{% note warning %}

Настройки никак не влияют на агрегацию информации с разных партиций - будет взято в любом случае минимальное значение по всем партициям.

{% endnote %}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TWatermarkPercentileSpec.md) %}

##### LateDataPartitions {#late-data-partitions}

Модуль используется для игнорирования партиций, содержащих исключительно старые данные. Работает аналогично `WatermarkPercentile`, но на уровне всех партиций, а не сообщений внутри партиции.

`WatermarkPercentile` позволяет игнорировать старые сообщения **внутри партиции**, но если вся партиция содержит только старые данные, она всё равно будет тормозить общий `EventWatermark`. Модуль `LateDataPartitions` решает эту проблему, позволяя игнорировать такие партиции целиком.

Алгоритм расчёта: `FinalWatermark = max(MinPartitionWatermark, PercentilePartitionWatermark - Delay)`, где `MinPartitionWatermark` — минимальный watermark среди всех партиций, а `PercentilePartitionWatermark` — watermark партиции в указанном `value` перcентиле (партиции сортируются по watermark).

Если партиция попадает в `value` персентиль — она безусловно учитывается. Если партиция не попадает в указанный персентиль, однако её watermark отличается от персентильного не более, чем на `delay` — она тоже будет учтена. Партиции с watermark ниже порога (`PercentileWatermark - Delay`) считаются содержащими только late data, и их watermark «скрывается» (приводится к `FinalWatermark`).

Например, при `value=90` и 100 партициях: 90% партиций (с наибольшими watermark) учитываются безусловно, а оставшиеся 10% (с наименьшими watermark) — только если их отставание от 90-го персентиля не превышает `delay`.

{% note warning %}

Этот модуль применяется **после** `IdlePartitions`. Idle-партиции уже исключены из расчёта до применения логики `LateDataPartitions`.

{% endnote %}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TLateDataPartitionsSpec.md) %}

#### Timer

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TTimerSpec.md) %}

Если не указаны `streams` и `streams_with_delays`, то таймер следит за потоками согласно `streams_dependency` с нулевой дополнительной задержкой.

#### Source {#source_spec}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TSourceSpec.md) %}

Встроенные реализации [сорсов](../../../flow/concepts/glossary.md#source) описаны в [документации по коннекторам](../../../flow/connectors/about.md).

#### Sink {#sink_spec}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TSinkSpec.md) %}

Встроенные реализации [синков](../../../flow/concepts/glossary.md#sink) описаны в [документации по коннекторам](../../../flow/connectors/about.md).

#### HeavyHitters

Модуль обнаружения высокочастотных ключей.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_THeavyHittersSpec.md) %}

#### InputOrdering

Настройки порядка обработки событий из входных потоков. Первыми будут обработаны события с наименьшим временем.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TInputOrderingSpec.md) %}

### Streams

В `streams` должны быть зарегистрированы только `output` потоки. `Source` потоки существуют только внутри соответствующих партиций и никак не персистятся в `YT`. А `timer` потоки имеют фиксированную схему.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TStreamSpec.md) %}

### Resource

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TResourceSpec.md) %}

## DynamicSpec

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicPipelineSpec.md) %}

`Stream` и `Resource` в данный момент не имеют или не поддерживают динамические параметры.

### Computation

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicComputationSpec.md) %}

#### Source

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicSourceSpec.md) %}

#### Sink

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicSinkSpec.md) %}

### JobManager

Конфиг модуля управления [джобами](../../../flow/concepts/glossary.md#job) на [контроллере](../../../flow/concepts/glossary.md#controller).

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicJobManagerSpec.md) %}

### JobTracker

Конфиг модуля управления джобами на [воркере](../../../flow/concepts/glossary.md#worker).

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicJobTrackerSpec.md) %}

#### BufferStateManager

Конфиг модуля управления размерами буферов.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicBufferStateManagerSpec.md) %}

Все размерные (измеряемые в количестве элементов или в байтах) параметры в этом конфиге могут парситься из строк вида "50K".

### ControllerConnector

Конфиг управления соединением воркера и контроллера.

Не рекомендуется менять без необходимости.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicControllerConnectorSpec.md) %}

## Parameters {#parameters}

Поле `parameters` есть в классах спек `Computation`, `Source`, `Sink`, `Resource`. В любом классе это слабо типизированное поле (`NYT::NYTree::IMapNodePtr`), которое скрывает за собой параметры, специфичные для [конкретной реализации сущности](*paramsClasses).

В конкретных реализациях сущностей поле `parameters` парсится в [yson struct](../../../user-guide/storage/data-types.md#yson_struct) для удобства и эффективности. При этом классы, в которые парсится поле `parameters`, называются `T*Parameters`, а не `T*Spec`. Как это работает на примере статической спеки `Computation`:

```cpp
// Спека Computation.
class TComputationSpec : public virtual NYTree::TYsonStruct
{
public:
    // ...
    NYTree::IMapNodePtr Parameters; // Слабо типизированное поле с параметрами.
    // ...
};

// Базовый класс сущности.
struct IComputation : public // ...
{
private:
    // Базовый класс yson struct для всех классов, в которые может парситься поле TComputationSpec::Parameters.
    struct TParametersBase : public virtual NYTree::TYsonStruct
    {
        // ...
    };
public:
    // В базовом классе сущности объявляется, что у её наследников могут быть параметры, унаследованные от TParametersBase.
    // Макрос определяет алиасы TParameters[Ptr], связывая их с TParametersBase.
    YT_FLOW_REGISTER_PARAMETERS(TParametersBase);
    // ...
};

class TTransformComputation
    : public TUniversalComputationBase // TUniversalComputationBase → TComputationBase → IComputation.
{
private:
    // Параметры для TTransformComputation. Они унаследованы от параметров родительского класса TTransformComputation.
    // Параметры классов-наследников TTransformComputation должны наследоваться от TExtendedParameters.
    // Но обращаться к ним нужно через алиас: TTransformComputation::TParameters, который будет объявлен ниже по коду.
    struct TExtendedParameters : public TUniversalComputationBase::TParameters
    {
        EProcessingMode ProcessingMode; // Поле, специфичное для TTransformComputation.
        // ...
    };
public:
    // Объявляется, что класс будет использовать более специфичные параметры.
    // При этом computationSpec->Parameters парсится в финальный класс параметров,
    // и TTransformComputation использует их, приводя к более базовому классу — TTransformComputation::TExtendedParameters.
    // Макрос определяет алиасы TParameters[Ptr], связывая их с TExtendedParameters.
    // Также появляется метод TParametersPtr GetParameters(), который можно использовать для получения параметров в коде.
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);
    // ...
};

// Пользовательский Computation.
class TMyClassComputation : public TTransformComputation
{
private:
    struct TExtendedParameters : public TTransformComputation::TParameters
    {
        // Поля, специфичные для TMyComputation.
        // ...
    };
public:
    // В пользовательских классах можно задать ещё более специфичные параметры.
    // Как и в TTransformComputation, переопределяются алиасы TParameters[Ptr] и метод TParametersPtr GetParameters().
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);
};
```

## Config

Конфиг ноды кластера Flow.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TFlowNodeConfig.md) %}

### Controller

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NController_TControllerConfig.md) %}

#### PersistedStateManager

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NController_TPersistedStateManagerConfig.md) %}

#### LeaseManager

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NController_TLeaseManagerConfig.md) %}

#### ControllerService

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NController_TControllerServiceConfig.md) %}


Плюс небольшой запас для кейса, что в некоторые живые партиции тоже не будет записи.

## Runner config {#runner-config}

Конфиг для запуска пайплайна. Формат этого конфига не регламентирован, пользователь может использовать такой конфиг, какой удобнее для его задач.

Flow предоставляет простой вариант этого конфига `NYT::NFlow::TSimpleRunnerConfig` и программу `NYT::NFlow::TSimpleRunnerProgram` для запуска пайплайна с использованием этого конфига. И именно на такой формат конфига рассчитывает тестовый фреймворк Flow, чтобы подставить в него кластер и путь к пайплайну в локальном {{product-name}} и чтобы применить тестовые дефолтные параметры для более быстрой работы теста.

### SimpleRunnerConfig

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TSimpleRunnerConfig.md) %}

## См. также

- [Computation](../../../flow/concepts/computation.md)
- [Watermarks и Timers](../../../flow/concepts/watermarks.md)
- [Коннекторы](../../../flow/connectors/about.md)

[*idle_partitions_max_ratio_default_value]: Почему 0.4: если {% if audience == "internal" %}`Logbroker`{% else %}источник{% endif %} закроет запись в один из ДЦ, то примерно треть партиций станут `idle`.

[*paramsClasses]: `Computation`, `Source`, `Sink`, `Resource`
