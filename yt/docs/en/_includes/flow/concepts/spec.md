# Spec, DynamicSpec, and Config in {{product-name}} Flow

Use a spec to describe the pipeline you want. Any high-level API to Flow generates a pipeline spec.

A pipeline has two specs: `Spec` and `DynamicSpec`:

- `Spec` is a static object that contains the topology of the entire pipeline. You can change this object only if the pipeline is stopped. The system guarantees that at runtime, every part of the system has the same view of the `Spec`.
- `DynamicSpec` is the dynamic part of the spec. You can change it at any time, and the changes apply asynchronously to all parts of the system. `DynamicSpec` usually stores buffer sizes, timeouts, the number of partitions, and so on.

The node’s `Config` is also an important part. Unlike the spec, you set the config exactly once per release, and you can’t change it dynamically. In the future, some config parameters might move to the specs.

{% note warning %}

All parameters in Flow have optimal default values. Don’t change the settings unless you’re sure what they do and how they can affect the system.

{% endnote %}

You can find the default values by searching for the parameter name in the Flow codebase. They’re mostly grouped in the file [spec.cpp]({{source-root}}/yt/yt/flow/library/cpp/common/spec.cpp).

## Spec

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TPipelineSpec.md) %}

### Computation

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TComputationSpec.md) %}

#### WatermarkStrategy {#watermark-strategy}

Fill this for `SourceComputation`. Other `Computation` types might not support it, or might support it only partially.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TWatermarkStrategySpec.md) %}

##### EventTimestampAssigner {#event-timestamp-assigner}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TEventTimestampAssignerSpec.md) %}

##### WatermarkGenerator {#watermark-generator}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TWatermarkGeneratorSpec.md) %}

Settings for `IdlePartitions`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TIdlePartitionsSpec.md) %}

Settings for `UnavailablePartitionGroups`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnavailablePartitionGroupsSpec.md) %}

##### WatermarkAlignment {#watermark-alignment}

Use this module to align input [streams](../../../flow/concepts/glossary.md#stream-and-computation). It can cause a complete read stop if there are availability issues or problems estimating the `Watermark` for individual [partitions](../../../flow/concepts/glossary.md#partition) or `SourceComputation`.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TWatermarkAlignmentSpec.md) %}

##### WatermarkPercentile {#watermark-percentile}

Use this module to ignore old data in streams when calculating `EventWatermark`. Each partition maintains a set of unprocessed messages for each outgoing stream. By default, the system calculates `EventWatermark` using the minimum `EventTimestamp` value across all messages (that is, it accounts for `100%` of events).

If an event falls within the `value` percentile, it’s always included in the `EventWatermark` calculation. If an event doesn’t fall within the specified percentile but its `EventTimestamp` differs from those events by no more than `delay`, it’s also included in the `EventWatermark` calculation. So the calculation algorithm is `EventWatermark = max(MinEventTimestamp, PercentileEventTimestamp - delay)`, where `MinEventTimestamp` is the minimum across all events, and `PercentileEventTimestamp` is the `EventTimestamp` value at the corresponding `value` percentile.

Keep in mind that the `inflight` set for a single stream within a partition can contain dozens of messages at a time. That’s why values like `99` or `99.9` can behave almost identically to `100`—meaning they won’t produce the desired effect. Also, even if old events make up less than 1% of the entire stream, they might be unevenly distributed across partitions. That’s why it’s better to use values like `80` or `90`. To avoid negative effects like ignoring recent events, there’s the `delay` parameter: it ensures that only truly old events are excluded from the calculation. If there are no such old events, the system works the same as when `value` is `100`, without losing any information.

This module affects only the `EventWatermark` calculation and doesn’t affect `SystemWatermark`.

{% note warning %}

The settings don’t affect how the system aggregates information from different partitions. The system always uses the minimum value across all partitions.

{% endnote %}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TWatermarkPercentileSpec.md) %}

##### LateDataPartitions {#late-data-partitions}

Use this module to ignore partitions that contain only old data. It works similarly to `WatermarkPercentile`, but at the level of all partitions, not individual messages inside a partition.

`WatermarkPercentile` lets you ignore old messages **inside a partition**, but if an entire partition contains only old data, it will still slow down the overall `EventWatermark`. The `LateDataPartitions` module solves this by letting you ignore such partitions entirely.

The calculation algorithm is `FinalWatermark = max(MinPartitionWatermark, PercentilePartitionWatermark - Delay)`, where `MinPartitionWatermark` is the minimum watermark across all partitions, and `PercentilePartitionWatermark` is the partition’s watermark at the specified `value` percentile (partitions are sorted by watermark).

If a partition falls within the `value` percentile, it’s always included. If a partition doesn’t fall within the specified percentile but its watermark differs from the percentile value by no more than `delay`, it’s also included. Partitions with a watermark below the threshold (`PercentileWatermark - Delay`) are considered to contain only late data, and their watermark is “hidden” (set to `FinalWatermark`).

For example, with `value=90` and 100 partitions: 90% of the partitions (those with the highest watermarks) are always included, and the remaining 10% (those with the lowest watermarks) are included only if their lag from the 90th percentile doesn’t exceed `delay`.

{% note warning %}

This module runs **after** `IdlePartitions`. Idle partitions are already excluded from the calculation before the `LateDataPartitions` logic applies.

{% endnote %}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TLateDataPartitionsSpec.md) %}

#### Timer

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TTimerSpec.md) %}

If you don’t specify `streams` and `streams_with_delays`, the timer monitors the streams according to `streams_dependency` with zero extra delay.

#### Source {#source_spec}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TSourceSpec.md) %}

Built-in [source](../../../flow/concepts/glossary.md#source) implementations are described in the [connector documentation](../../../flow/connectors/about.md).

#### Sink {#sink_spec}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TSinkSpec.md) %}

Built-in [sink](../../../flow/concepts/glossary.md#sink) implementations are described in the [connector documentation](../../../flow/connectors/about.md).

#### HeavyHitters

This module detects high-frequency keys.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_THeavyHittersSpec.md) %}

#### InputOrdering

These settings control the order in which the system processes events from input streams. Events with the earliest timestamps are processed first.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TInputOrderingSpec.md) %}

### Streams

Register only `output` streams in `streams`. `Source` streams exist only inside their corresponding partitions and aren’t persisted in YT. `Timer` streams have a fixed schema.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TStreamSpec.md) %}

### Resource

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TResourceSpec.md) %}

## DynamicSpec

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicPipelineSpec.md) %}

`Stream` and `Resource` currently don’t have or don’t support dynamic parameters.

### Computation

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicComputationSpec.md) %}

#### Source

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicSourceSpec.md) %}

#### Sink

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicSinkSpec.md) %}

### JobManager

This is the config for the module that manages [jobs](../../../flow/concepts/glossary.md#job) on the [controller](../../../flow/concepts/glossary.md#controller).

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicJobManagerSpec.md) %}

### JobTracker

This is the config for the module that manages jobs on the [worker](../../../flow/concepts/glossary.md#worker).

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicJobTrackerSpec.md) %}

#### BufferStateManager

This is the config for the module that manages buffer sizes.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicBufferStateManagerSpec.md) %}

All dimensional parameters in this config (measured in number of items or bytes) can be parsed from strings like "50K".

### ControllerConnector

This is the config for managing the connection between the worker and the controller.

Don’t change it unless necessary.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicControllerConnectorSpec.md) %}

## Parameters {#parameters}

The `parameters` field exists in the spec classes `Computation`, `Source`, `Sink`, and `Resource`. In any class, this is a weakly typed field (`NYT::NYTree::IMapNodePtr`) that hides parameters specific to a [particular entity implementation](*paramsClasses).

In concrete entity implementations, the `parameters` field is parsed into a [yson struct](../../../user-guide/storage/data-types.md#yson_struct) for convenience and efficiency. The classes that the `parameters` field parses into are named `T*Parameters`, not `T*Spec`. Here’s how it works, using the static `Computation` spec as an example:

```cpp
// Computation spec.
class TComputationSpec : public virtual NYTree::TYsonStruct
{
public:
    // ...
    NYTree::IMapNodePtr Parameters; // Weakly typed field with parameters.
    // ...
};

// Base entity class.
struct IComputation : public // ...
{
private:
    // Base yson struct class for all classes that the TComputationSpec::Parameters field can parse into.
    struct TParametersBase : public virtual NYTree::TYsonStruct
    {
        // ...
    };
public:
    // The base entity class declares that its descendants can have parameters inherited from TParametersBase.
    // The macro defines TParameters[Ptr] aliases, linking them to TParametersBase.
    YT_FLOW_REGISTER_PARAMETERS(TParametersBase);
    // ...
};

class TTransformComputation
    : public TUniversalComputationBase // TUniversalComputationBase → TComputationBase → IComputation.
{
private:
    // Parameters for TTransformComputation. They’re inherited from the parent class TTransformComputation’s parameters.
    // Parameters for TTransformComputation’s descendant classes must inherit from TExtendedParameters.
    // But you must access them via the alias: TTransformComputation::TParameters, which is declared later in the code.
    struct TExtendedParameters : public TUniversalComputationBase::TParameters
    {
        EProcessingMode ProcessingMode; // Field specific to TTransformComputation.
        // ...
    };
public:
    // Declare that the class will use more specific parameters.
    // At the same time, computationSpec->Parameters is parsed into the final parameters class,
    // and TTransformComputation uses them, casting to a more basic class — TTransformComputation::TExtendedParameters.
    // The macro defines TParameters[Ptr] aliases, linking them to TExtendedParameters.
    // Also, a TParametersPtr GetParameters() method appears, which you can use to get the parameters in code.
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);
    // ...
};

// Custom Computation.
class TMyClassComputation : public TTransformComputation
{
private:
    struct TExtendedParameters : public TTransformComputation::TParameters
    {
        // Fields specific to TMyComputation.
        // ...
    };
public:
    // In custom classes, you can define even more specific parameters.
    // Like in TTransformComputation, you redefine the TParameters[Ptr] aliases and the TParametersPtr GetParameters() method.
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);
};
```

## Config

This is the config for a Flow cluster node.

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TFlowNodeConfig.md) %}

### Controller

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NController_TControllerConfig.md) %}

#### PersistedStateManager

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NController_TPersistedStateManagerConfig.md) %}

#### LeaseManager

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NController_TLeaseManagerConfig.md) %}

#### ControllerService

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_NController_TControllerServiceConfig.md) %}


Plus a small buffer for the case where some live partitions also won’t receive any writes.

## Runner config {#runner-config}

This is the config for running a pipeline. The format of this config isn’t standardized; you can use whatever config works best for your tasks.

Flow provides a simple version of this config, `NYT::NFlow::TSimpleRunnerConfig`, and a program, `NYT::NFlow::TSimpleRunnerProgram`, to run a pipeline using this config. The Flow test framework relies on this config format to inject the cluster and the pipeline path in the local {{product-name}}, and to apply test default parameters for faster test runs.

### SimpleRunnerConfig

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TSimpleRunnerConfig.md) %}

## See also

- [Computation](../../../flow/concepts/computation.md)
- [Watermarks and Timers](../../../flow/concepts/watermarks.md)
- [Connectors](../../../flow/connectors/about.md)

[*idle_partitions_max_ratio_default_value]: Why 0.4: if {% if audience == "internal" %}`Logbroker`{% else %}the source{% endif %} stops writing to one of the data centers, about a third of the partitions will become `idle`.

[*paramsClasses]: `Computation`, `Source`, `Sink`, `Resource`