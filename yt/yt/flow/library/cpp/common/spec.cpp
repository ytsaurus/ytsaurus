#include "spec.h"
#include "registry.h"
#include "schema.h"
#include "state.h"
#include "state_cache.h"
#include "yt_path_option.h"

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/retryable_client_spec.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/misc/configurable_singleton_def.h>
#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <library/cpp/iterator/concatenate.h>

#include <cmath>

namespace NYT::NFlow {

using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TStreamId MakeGlobalStreamId(const TComputationId& computationId, const TStreamId& localStreamId, const TComputationSpecPtr& spec)
{
    if (spec->InputStreamIds.contains(localStreamId) || spec->OutputStreamIds.contains(localStreamId)) {
        return localStreamId;
    }

    return TStreamId(Format("%v/%v", computationId, localStreamId));
}

////////////////////////////////////////////////////////////////////////////////

void TStreamSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("class_name", &TThis::ClassName)
        .Default();

    registrar.Parameter("schema", &TThis::Schema);

    registrar.Parameter("migration_function", &TThis::MigrationFunction)
        .Default("NYT::NFlow::DefaultMigrationFunction");
}

bool operator==(const TStreamSpec& lhs, const TStreamSpec& rhs)
{
    return (lhs.Schema && rhs.Schema ? *lhs.Schema == *rhs.Schema : lhs.Schema == rhs.Schema) &&
        lhs.MigrationFunction == rhs.MigrationFunction;
}

////////////////////////////////////////////////////////////////////////////////

void TEventTimestampAssignerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("column", &TThis::Column)
        .Default();
    registrar.Parameter("format", &TThis::Format)
        .Default(ETimestampFormat::Seconds);
    registrar.Parameter("limit_by_system_timestamp", &TThis::LimitBySystemTimestamp)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TIdlePartitionsSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("duration", &TThis::Duration)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("max_ratio", &TThis::MaxRatio)
        .Default(0.4);
}

////////////////////////////////////////////////////////////////////////////////

void TUnavailablePartitionGroupsSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("max_groups", &TThis::MaxGroups)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

void TLateDataPartitionsSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("value", &TThis::Value)
        .Default(PreciseWatermarkPercentile)
        .InRange(IgnoreInflightWatermarkPercentile, PreciseWatermarkPercentile);
    registrar.Parameter("delay", &TThis::Delay)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TWatermarkGeneratorSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("use_source_watermark", &TThis::UseSourceWatermark)
        .Default(false);
    registrar.Parameter("out_of_orderness_bound", &TThis::OutOfOrdernessBound)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("idle_partitions", &TThis::IdlePartitions)
        .Default();
    registrar.Parameter("unavailable_partition_groups", &TThis::UnavailablePartitionGroups)
        .Default();
    registrar.Parameter("late_data_partitions", &TThis::LateDataPartitions)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TWatermarkAlignmentSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("group_name", &TThis::GroupName)
        .Default(TWatermarkAlignmentGroup("default-alignment-group"));
    registrar.Parameter("drift_bound", &TThis::DriftBound)
        .Default(TDuration::Minutes(20));
    registrar.Parameter("read_delays", &TThis::ReadDelays)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TWatermarkPercentileSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("value", &TThis::Value)
        .Default(PreciseWatermarkPercentile)
        .InRange(IgnoreInflightWatermarkPercentile, PreciseWatermarkPercentile);
    registrar.Parameter("delay", &TThis::Delay)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TWatermarkStrategySpec::Register(TRegistrar registrar)
{
    registrar.Parameter("event_timestamp_assigner", &TThis::EventTimestampAssigner)
        .DefaultNew();
    registrar.Parameter("watermark_generator", &TThis::WatermarkGenerator)
        .Default();
    registrar.Parameter("watermark_alignment", &TThis::WatermarkAlignment)
        .Default();
    registrar.Parameter("watermark_percentile", &TThis::WatermarkPercentile)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TSourceSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("source_class_name", &TThis::SourceClassName)
        .Default();
    registrar.Parameter("parameters", &TThis::Parameters)
        .DefaultCtor([] {
            return GetEphemeralNodeFactory()->CreateMap();
        })
        .ResetOnLoad();
}

////////////////////////////////////////////////////////////////////////////////

void TSinkSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("sink_class_name", &TThis::SinkClassName)
        .Default();
    registrar.Parameter("input_stream_ids", &TThis::InputStreamIds)
        .Default();
    registrar.Parameter("parameters", &TThis::Parameters)
        .DefaultCtor([] {
            return GetEphemeralNodeFactory()->CreateMap();
        })
        .ResetOnLoad();
}

////////////////////////////////////////////////////////////////////////////////

void TTimerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("time_type", &TThis::TimeType)
        .Default(ETimeType::EventTime);
    registrar.Parameter("streams", &TThis::Streams)
        .Default();
    registrar.Parameter("streams_with_delays", &TThis::StreamsWithDelays)
        .Default();
    registrar.Parameter("deduplicate_equal_timestamps", &TThis::DeduplicateEqualTimestamps)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TKeyVisitorStreamSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("names", &TThis::Names)
        .Default();
    registrar.Parameter("external_names", &TThis::ExternalNames)
        .Default();
    registrar.Parameter("bucket_count", &TThis::BucketCount)
        .GreaterThanOrEqual(1)
        .Default(8);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicKeyVisitorStreamSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("period", &TThis::Period)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Days(1));
    registrar.Parameter("buffer_row_limit", &TThis::BufferRowLimit)
        .GreaterThan(NYTree::TSize(0))
        .Default(NYTree::TSize(5'000));
    registrar.Parameter("max_scan_rows_per_iteration", &TThis::MaxScanRowsPerIteration)
        .GreaterThan(NYTree::TSize(0))
        .Default(NYTree::TSize(10'000));
    registrar.Parameter("catchup_lag_threshold", &TThis::CatchupLagThreshold)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default(TDuration::Minutes(1));
    registrar.Parameter("catchup_speedup_multiplier", &TThis::CatchupSpeedupMultiplier)
        .GreaterThan(1.0)
        .Default(1.2);
}

////////////////////////////////////////////////////////////////////////////////

void THeavyHittersSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("window", &TThis::Window)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("threshold", &TThis::Threshold)
        .GreaterThan(NYTree::TSize(0))
        .LessThan(1)
        .Default(0.01);
    registrar.Parameter("limit", &TThis::Limit)
        .GreaterThanOrEqual(0)
        .Default(5);
}

////////////////////////////////////////////////////////////////////////////////

void TPivotFinderSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("part_count", &TThis::PartCount)
        .GreaterThanOrEqual(0)
        .Default(16);
    registrar.Parameter("window_size", &TThis::WindowSize)
        .GreaterThan(0)
        .Default(16 * 1024);
}

////////////////////////////////////////////////////////////////////////////////

void TInputOrderingSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("time_type", &TThis::TimeType)
        .Default(ETimeType::EventTime);
    registrar.Parameter("stream_delays", &TThis::StreamDelays)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TExternalStateManagerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("external_state_manager_class_name", &TThis::ExternalStateManagerClassName)
        .Alias("class_name")
        .Default("NYT::NFlow::TSimpleExternalStateManager");
    registrar.Parameter("parameters", &TThis::Parameters)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicExternalStateManagerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("parameters", &TThis::Parameters)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TExternalStateJoinerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("external_state_joiner_class_name", &TThis::ExternalStateJoinerClassName)
        .Alias("class_name")
        .Default("NYT::NFlow::TSimpleExternalStateJoiner");
    registrar.Parameter("client_provider_resource_id", &TThis::ClientProviderResourceId)
        .Default();
    registrar.Parameter("client_factory_resource_id", &TThis::ClientFactoryResourceId)
        .Default();
    registrar.Parameter("join_on", &TThis::JoinOn)
        .DefaultNew();
    registrar.Parameter("auto_preload", &TThis::AutoPreload)
        .Default(true);
    registrar.Parameter("parameters", &TThis::Parameters)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TStateJoinSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("key_schema_override", &TThis::KeySchemaOverride)
        .Default();
    registrar.Parameter("key_provider_streams", &TThis::KeyProviderStreams)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicExternalStateJoinerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("parameters", &TThis::Parameters)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TStateJoinerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("computation_id", &TThis::ComputationId);
    registrar.Parameter("state_name", &TThis::StateName);
    registrar.Parameter("join_on", &TThis::JoinOn)
        .DefaultNew();
    registrar.Parameter("auto_preload", &TThis::AutoPreload)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicStateJoinerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("cache", &TThis::Cache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TComputationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("computation_class_name", &TThis::ComputationClassName);

    registrar.Parameter("processing_function", &TThis::ProcessingFunction)
        .Default();
    registrar.Parameter("processing_function_parameters", &TThis::ProcessingFunctionParameters)
        .Default();

    registrar.Parameter("group_by_schema", &TThis::GroupBySchema)
        // Because default constructor of TTableSchema has different behaviour.
        .DefaultCtor([] {
            return New<NTableClient::TTableSchema>(std::vector<TColumnSchema>{});
        })
        .ResetOnLoad();

    registrar.Parameter("experimental_enable_non_uint_key", &TThis::ExperimentalEnableNonUintKey)
        .Default();

    registrar.Parameter("input_stream_ids", &TThis::InputStreamIds)
        .Default();
    registrar.Parameter("output_stream_ids", &TThis::OutputStreamIds)
        .Default();

    registrar.Parameter("streams_dependency", &TThis::StreamsDependency)
        .Default();

    registrar.Parameter("watermark_strategy", &TThis::WatermarkStrategy)
        .DefaultNew();

    registrar.Parameter("required_resource_ids", &TThis::RequiredResourceIds)
        .Default();

    registrar.Parameter("parameters", &TThis::Parameters)
        .DefaultCtor([] {
            return GetEphemeralNodeFactory()->CreateMap();
        })
        .ResetOnLoad();

    registrar.Parameter("timer_streams", &TThis::TimerStreams)
        .Default();

    registrar.Parameter("key_visitor_streams", &TThis::KeyVisitorStreams)
        .Default();

    registrar.Parameter("source_streams", &TThis::SourceStreams)
        .Default();

    registrar.Parameter("sinks", &TThis::Sinks)
        .Default();

    registrar.Parameter("external_state_managers", &TThis::ExternalStateManagers)
        .Default();
    registrar.Parameter("external_state_joiners", &TThis::ExternalStateJoiners)
        .Default();
    registrar.Parameter("state_joiners", &TThis::StateJoiners)
        .Default();

    registrar.Parameter("heavy_hitters", &TThis::HeavyHitters)
        .Default();

    registrar.Parameter("pivot_finder", &TThis::PivotFinder)
        .Default();

    registrar.Parameter("input_ordering", &TThis::InputOrdering)
        .DefaultNew();
    registrar.Parameter("distribution_ordering", &TThis::DistributionOrdering)
        .Default(EDistributionOrdering::Strict);

    registrar.Parameter("worker_group", &TThis::WorkerGroup)
        .Default();

    registrar.Parameter("allow_timer_self_dependency", &TThis::AllowTimerSelfDependency)
        .Default(false);

    registrar.Parameter("use_compact_input_messages", &TThis::UseCompactInputMessages)
        .Default();

    registrar.Postprocessor([] (TThis* computationSpec) {
        // All validations are placed in ValidatePipelineSpec().

        for (const auto& timerStreamId : GetKeys(computationSpec->TimerStreams)) {
            if (!computationSpec->StreamsDependency.contains(timerStreamId)) {
                computationSpec->StreamsDependency[timerStreamId] = {};
                for (const auto& inputStreamId : computationSpec->InputStreamIds) {
                    computationSpec->StreamsDependency[timerStreamId].insert(inputStreamId);
                }
                for (const auto& sourceStreamId : GetKeys(computationSpec->SourceStreams)) {
                    computationSpec->StreamsDependency[timerStreamId].insert(sourceStreamId);
                }
            }
        }
        // key_visitor streams are intentionally NOT auto-added to the graph:
        // visitors are usually internal cleanup loops with no downstream
        // output, and a still-running visitor must not gate output completion.
        // The last-pass signal reaches the visitor through the worker-local
        // `SetUpstreamCompleted` path (computation_base.cpp), which does not
        // depend on `streams_dependency`. If a computation emits into an
        // output from `DoProcessVisit` and wants the output's watermark to
        // wait for the visitor, the user must list the key-visitor stream as
        // a parent of that output explicitly.
        for (const auto& outputStreamId : computationSpec->OutputStreamIds) {
            if (!computationSpec->StreamsDependency.contains(outputStreamId)) {
                computationSpec->StreamsDependency[outputStreamId] = {};
                for (const auto& inputStreamId : computationSpec->InputStreamIds) {
                    computationSpec->StreamsDependency[outputStreamId].insert(inputStreamId);
                }
                for (const auto& sourceStreamId : GetKeys(computationSpec->SourceStreams)) {
                    computationSpec->StreamsDependency[outputStreamId].insert(sourceStreamId);
                }
                for (const auto& timerStreamId : GetKeys(computationSpec->TimerStreams)) {
                    computationSpec->StreamsDependency[outputStreamId].insert(timerStreamId);
                }
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

THashSet<TStreamId> ComputeAllowedInputStreams(
    const THashSet<TStreamId>& allowedOutputStreams,
    const TComputationSpecPtr& spec)
{
    // Fill by all input/injected/timer/key-visitor streams.
    auto allStreamsView = Concatenate(
        spec->InputStreamIds,
        GetKeys(spec->SourceStreams),
        GetKeys(spec->TimerStreams),
        GetKeys(spec->KeyVisitorStreams));
    THashSet<TStreamId> result(allStreamsView.begin(), allStreamsView.end());

    // Remove prohibited streams.
    for (const auto& outputStreamId : spec->OutputStreamIds) {
        if (allowedOutputStreams.contains(outputStreamId)) {
            continue;
        }
        for (const auto& prohibitedInputStream : GetOrCrash(spec->StreamsDependency, outputStreamId)) {
            result.erase(prohibitedInputStream);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool ResolveUseCompactInputMessages(const TComputationSpecPtr& spec)
{
    if (spec->UseCompactInputMessages) {
        return *spec->UseCompactInputMessages;
    }
    // With a uint key the first key column is a required uint64 hash and partitioning splits
    // purely by it, so the compact key[0]-only deduplication key uniquely identifies a message.
    return !spec->ExperimentalEnableNonUintKey.value_or(TComputationSpec::ExperimentalEnableNonUintKeyDefault);
}

////////////////////////////////////////////////////////////////////////////////

void TResourceDescription::Register(TRegistrar registrar)
{
    registrar.Parameter("alias", &TThis::Alias)
        .Default();
    registrar.Parameter("worker", &TThis::Worker)
        .Default(true);
    registrar.Parameter("controller", &TThis::Controller)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TResourceSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("resource_class_name", &TThis::ResourceClassName);
    registrar.Parameter("parameters", &TThis::Parameters)
        .DefaultCtor([] {
            return GetEphemeralNodeFactory()->CreateMap();
        })
        .ResetOnLoad();
    registrar.Parameter("dependencies", &TThis::Dependencies)
        .Default();
    registrar.Parameter("required_capabilities", &TThis::RequiredCapabilities)
        .Default();
    registrar.Parameter("preload_required", &TThis::PreloadRequired)
        .Default(false);
    registrar.Parameter("always_on", &TThis::AlwaysOn)
        .Default(false);

    registrar.Postprocessor([] (TThis* spec) {
        if (spec->PreloadRequired && spec->AlwaysOn) {
            THROW_ERROR_EXCEPTION("Resource spec cannot set both \"preload_required\" and \"always_on\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TPipelineSpec::Register(TRegistrar registrar)
{
    // Keep unrecognized fields so they survive the round-trip through the struct;
    // they are surfaced as warnings by ValidateSpecs instead of being silently dropped.
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

    registrar.Parameter("validate_binary_version", &TThis::ValidateBinaryVersion)
        .Default(false);
    registrar.Parameter("binary_version", &TThis::BinaryVersion)
        .Default();
    registrar.Parameter("validate_binary_checksum", &TThis::ValidateBinaryChecksum)
        .Default(false);
    registrar.Parameter("binary_checksum", &TThis::BinaryChecksum)
        .Default();
    registrar.Parameter("computations", &TThis::Computations)
        .Default();
    registrar.Parameter("resources", &TThis::Resources)
        .Default();
    registrar.Parameter("streams", &TThis::Streams)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TExtendedComputationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("all_stream_ids", &TThis::AllStreamIds)
        .Default();
    registrar.Parameter("subscribers_new", &TThis::Subscribers)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TExtendedPipelineSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("computations", &TThis::Computations)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TExtendedPipelineSpecPtr BuildExtendedPipelineSpec(const TPipelineSpecPtr& spec)
{
    auto extendedSpec = New<TExtendedPipelineSpec>();

    THashMap<TStreamId, THashSet<TComputationId>> subscribers;
    for (const auto& [computationId, computationSpec] : spec->Computations) {
        for (const auto& streamId : computationSpec->InputStreamIds) {
            subscribers[streamId].insert(computationId);
        }
    }

    for (const auto& [computationId, computationSpec] : spec->Computations) {
        auto extendedComputationSpec = New<TExtendedComputationSpec>();
        for (const auto& streamId : computationSpec->InputStreamIds) {
            extendedComputationSpec->AllStreamIds.insert(streamId);
        }

        for (const auto& streamId : computationSpec->OutputStreamIds) {
            extendedComputationSpec->Subscribers[streamId] = GetOrDefault(subscribers, streamId, {});
            extendedComputationSpec->AllStreamIds.insert(streamId);
        }
        for (const auto& streamId : GetKeys(computationSpec->TimerStreams)) {
            extendedComputationSpec->AllStreamIds.insert(streamId);
        }
        for (const auto& streamId : GetKeys(computationSpec->KeyVisitorStreams)) {
            extendedComputationSpec->AllStreamIds.insert(streamId);
        }
        for (const auto& streamId : GetKeys(computationSpec->SourceStreams)) {
            extendedComputationSpec->AllStreamIds.insert(streamId);
        }
        extendedSpec->Computations[computationId] = extendedComputationSpec;
    }


    return extendedSpec;
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicSourceSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("parameters", &TThis::Parameters)
        .DefaultCtor([] {
            return GetEphemeralNodeFactory()->CreateMap();
        })
        .ResetOnLoad();
    registrar.Parameter("draining", &TThis::Draining)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicSinkSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("parameters", &TThis::Parameters)
        .DefaultCtor([] {
            return GetEphemeralNodeFactory()->CreateMap();
        })
        .ResetOnLoad();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicStateFormatSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("compress", &TThis::Compress)
        .Default(false);
    registrar.Parameter("recode_probability", &TThis::RecodeProbability)
        .Default(0.1);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicTableRequestSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("select_min_limit", &TThis::SelectMinLimit)
        .Default(NYTree::TSize::FromString("10"));
    registrar.Parameter("select_limit_multiplier", &TThis::SelectLimitMultiplier)
        .Default(5);
    registrar.Parameter("select_max_limit", &TThis::SelectMaxLimit)
        .Default(NYTree::TSize::FromString("10K"));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicStateSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("format", &TThis::Format)
        .DefaultNew();
    registrar.Parameter("table_request", &TThis::TableRequest)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicStateManagerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("format_overrides", &TThis::FormatOverrides)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicInputStoreSpec::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TDynamicTimerStoreSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("table_request", &TThis::TableRequest)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicOutputStoreSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("table_request", &TThis::TableRequest)
        .DefaultNew();
    registrar.Parameter("compression_codec", &TThis::CompressionCodec)
        .Default(NCompression::ECodec::Zstd_6);
    registrar.Parameter("max_chunk_message_count", &TThis::MaxChunkMessageCount)
        .Default(NYTree::TSize(1024))
        .GreaterThan(NYTree::TSize(0))
        .LessThanOrEqual(NYTree::TSize(1 << 20));
}

////////////////////////////////////////////////////////////////////////////////

void TMessageBatcherSettings::Register(TRegistrar registrar)
{
    registrar.Parameter("batch_duration", &TThis::BatchDuration)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_rows_per_batch", &TThis::MaxRowsPerBatch)
        .Default(NYTree::TSize(1'000));
    registrar.Parameter("max_bytes_per_batch", &TThis::MaxBytesPerBatch)
        .Default(NYTree::TSize(10_MB));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicRetryableRequestSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("lease_check_period", &TThis::LeaseCheckPeriod)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicComputationSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("draining", &TThis::Draining)
        .Default(false);

    registrar.Parameter("empty_batch_backoff", &TThis::EmptyBatchBackoff)
        .Default(TDuration::MilliSeconds(250));

    registrar.Parameter("lease_check_period", &TThis::LeaseCheckPeriod)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("retryable_request", &TThis::RetryableRequest)
        .DefaultNew();

    registrar.Parameter("tracer", &TThis::Tracer)
        .DefaultNew();

    registrar.Parameter("parameters", &TThis::Parameters)
        .DefaultCtor([] {
            return GetEphemeralNodeFactory()->CreateMap();
        })
        .ResetOnLoad();

    registrar.Parameter("processing_function_parameters", &TThis::ProcessingFunctionParameters)
        .Default();

    registrar.Parameter("source_streams", &TThis::SourceStreams)
        .Default();
    registrar.Parameter("key_visitor_streams", &TThis::KeyVisitorStreams)
        .Default();
    registrar.Parameter("sinks", &TThis::Sinks)
        .Default();

    registrar.Parameter("external_state_managers", &TThis::ExternalStateManagers)
        .Default();
    registrar.Parameter("external_state_joiners", &TThis::ExternalStateJoiners)
        .Default();
    registrar.Parameter("state_joiners", &TThis::StateJoiners)
        .Default();

    registrar.Parameter("state_manager", &TThis::StateManager)
        .DefaultNew();
    registrar.Parameter("input_store", &TThis::InputStore)
        .DefaultNew();
    registrar.Parameter("timer_store", &TThis::TimerStore)
        .DefaultNew();
    registrar.Parameter("output_store", &TThis::OutputStore)
        .DefaultNew();

    // 400 bytes per timer.
    registrar.Parameter("timer_store_count_limit", &TThis::TimerStoreCountLimit)
        .Default(NYTree::TSize::FromString("250K"));
    registrar.Parameter("timer_store_byte_size_limit", &TThis::TimerStoreByteSizeLimit)
        .Default(NYTree::TSize(100_MB));

    registrar.Parameter("output_store_count_limit", &TThis::OutputStoreCountLimit)
        .Default(NYTree::TSize::FromString("1G"));
    registrar.Parameter("output_store_byte_size_limit", &TThis::OutputStoreByteSizeLimit)
        .Default(NYTree::TSize(100_GB));

    registrar.Parameter("input_rows_throttler_id", &TThis::InputRowsThrottlerId)
        .Default();
    registrar.Parameter("input_bytes_throttler_id", &TThis::InputBytesThrottlerId)
        .Default();

    registrar.Parameter("skip_if_expression", &TThis::SkipIfExpression)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicPartitionTracerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("epochs_in_trace_context", &TThis::EpochsInTraceContext)
        .GreaterThanOrEqual(1)
        .Default(3);
    registrar.Parameter("trace_probability", &TThis::TraceProbability)
        .InRange(0.0, 1.0)
        .Default(0.0);
    registrar.Parameter("trace_probability_partition_override", &TThis::TraceProbabilityPartitionOverride)
        .CheckThat([] (const THashMap<TPartitionId, double>& value) {
            for (const auto& [partitionId, probability] : value) {
                THROW_ERROR_EXCEPTION_UNLESS(0.0 <= probability && probability <= 1.0, "Probability %v is not in range [0.0, 1.0]", probability);
            }
        })
        .Optional();
    registrar.Parameter("wall_time_half_decay_period", &TThis::WallTimeHalfDecayPeriod)
        .GreaterThanOrEqual(TDuration::MilliSeconds(1))
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicResourceSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("parameters", &TThis::Parameters)
        .DefaultCtor([] {
            return GetEphemeralNodeFactory()->CreateMap();
        })
        .ResetOnLoad();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicThrottlerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("limit", &TThis::Limit)
        .Default();
    registrar.Parameter("period", &TThis::Period)
        .Default(TDuration::Seconds(10))
        .GreaterThanOrEqual(TDuration::MilliSeconds(1));
    registrar.Parameter("request_period", &TThis::RequestPeriod)
        .Default(TDuration::Seconds(5))
        .GreaterThanOrEqual(TDuration::MilliSeconds(1));
    registrar.Parameter("retrying_channel", &TThis::RetryingChannel)
        .DefaultCtor([] {
            auto config = New<NRpc::TRetryingChannelConfig>();
            // Generous defaults to ride out controller failover.
            config->RetryAttempts = 100;
            config->EnableExponentialRetryBackoffs = true;
            config->RetryTimeout = TDuration::Minutes(10);
            return config;
        });
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));
}

NConcurrency::TThroughputThrottlerConfigPtr TDynamicThrottlerSpec::BuildThroughputConfig() const
{
    auto config = New<NConcurrency::TThroughputThrottlerConfig>();
    config->Limit = Limit;
    config->Period = Period;
    return config;
}

NConcurrency::TPrefetchingThrottlerConfigPtr TDynamicThrottlerSpec::BuildPrefetchingConfig() const
{
    auto config = New<NConcurrency::TPrefetchingThrottlerConfig>();
    config->TargetRps = 1.0 / RequestPeriod.SecondsFloat();
    config->MinPrefetchAmount = 1;
    if (Limit) {
        double tokensPerSecond = *Limit / Period.SecondsFloat();
        auto maxPrefetch = static_cast<i64>(std::ceil(tokensPerSecond * RequestPeriod.SecondsFloat()));
        config->MaxPrefetchAmount = std::max<i64>(maxPrefetch, 1);
    } else {
        // Unlimited: no meaningful upper bound; pick a large batch so the
        // prefetcher does not call into the server for every unit.
        config->MaxPrefetchAmount = 1'000'000'000;
    }
    config->Window = std::max(RequestPeriod * 10, TDuration::Seconds(1));
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicJobBalancerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("balancer_type", &TThis::BalancerType)
        .Default(EJobBalancerType::CpuAware);
    registrar.Parameter("use_cpu_aware_balancer", &TThis::UseCpuAwareBalancer)
        .Default();
    registrar.Parameter("rebalance_delay_after_pipeline_sync", &TThis::RebalanceDelayAfterPipelineSync)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("rebalance_target_deviation", &TThis::RebalanceTargetDeviation)
        .Default(0.1);
    registrar.Parameter("rebalance_hot_mode_coeff", &TThis::RebalanceHotModeCoef)
        .Default(2);
    registrar.Parameter("rebalance_action_min_time", &TThis::RebalanceActionMinTime)
        .Default(TDuration::Seconds(4));
    registrar.Parameter("rebalance_action_max_time", &TThis::RebalanceActionMaxTime)
        .Default(TDuration::Seconds(12));
    registrar.Parameter("rebalance_sync_period", &TThis::RebalanceSyncPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("rebalance_count_exceeded_allowed", &TThis::RebalanceCountExceedAllowed)
        .Default(1.2);
    registrar.Parameter("rebalance_min_cpu_spread", &TThis::RebalanceMinCpuSpread)
        .Default(1.0);
    registrar.Parameter("rebalance_min_cpu_ratio", &TThis::RebalanceMinCpuRatio)
        .GreaterThanOrEqual(1.0)
        .Default(1.2);
    registrar.Parameter("disable_even_load_gate", &TThis::DisableEvenLoadGate)
        .Default();
    registrar.Parameter("async_balancing", &TThis::AsyncBalancing)
        .Default(true);
    registrar.Parameter("graceful_move", &TThis::GracefulMove)
        .Default(true);
    registrar.Parameter("zero_queue_latency", &TThis::ZeroQueueLatency)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("planning_horizon", &TThis::PlanningHorizon)
        .Default(TDuration::Minutes(10));

    registrar.Postprocessor([] (TThis* spec) {
        // Handle deprecated UseCpuAwareBalancer field.
        if (spec->UseCpuAwareBalancer.has_value()) {
            spec->BalancerType = *spec->UseCpuAwareBalancer
                ? EJobBalancerType::CpuAware
                : EJobBalancerType::Greedy;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicJobManagerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("minimum_worker_count", &TThis::MinimumWorkerCount)
        .Default(1);
    registrar.Parameter("lost_job_timeout", &TThis::LostJobTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("faulty_address_window", &TThis::FaultyAddressWindow)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("faulty_address_attempts", &TThis::FaultyAddressAttempts)
        .Default(5);
    registrar.Parameter("worker_group_override", &TThis::WorkerGroupOverride)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicBufferStateManagerSpec::TOneSideBufferSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("fair_share_pool", &TThis::FairSharePool)
        .Default(NYTree::TSize(3_GB));
    registrar.Parameter("job_guarantee", &TThis::JobGuarantee)
        .Default(NYTree::TSize(5_MB));
    registrar.Parameter("job_limit", &TThis::JobLimit)
        .Default(NYTree::TSize(500_MB));

    registrar.Parameter("max_duration", &TThis::MaxDuration)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("job_overrides", &TThis::JobOverrides)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicBufferStateManagerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("manage_period", &TThis::ManagePeriod)
        .Default(TDuration::MilliSeconds(1000));
    registrar.Parameter("demand_window", &TThis::DemandWindow)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("input_buffer", &TThis::InputBuffer)
        .DefaultNew();
    registrar.Parameter("output_buffer", &TThis::OutputBuffer)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicMessageDistributorSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("send_queue_max_rows_per_batch", &TThis::SendQueueMaxRowsPerBatch)
        .Default(NYTree::TSize(1000));
    registrar.Parameter("send_queue_max_bytes_per_batch", &TThis::SendQueueMaxBytesPerBatch)
        .Default(NYTree::TSize(1_MB));
    registrar.Parameter("send_queue_batch_duration", &TThis::SendQueueBatchDuration)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("push_messages_timeout", &TThis::PushMessagesTimeout)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("compression_codec", &TThis::CompressionCodec)
        .Default(NCompression::ECodec::Lz4);

    registrar.Parameter("thread_count", &TThis::ThreadCount)
        .GreaterThan(NYTree::TSize(0))
        .Default(4);

    registrar.Parameter("hung_task_threshold", &TThis::HungTaskThreshold)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("max_processed_batch_size", &TThis::MaxProcessedBatchSize)
        .GreaterThan(0)
        .Default(500'000);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicJobTrackerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("job_control_threads", &TThis::JobControlThreads)
        .Default(3);

    registrar.Parameter("job_threads", &TThis::JobThreads)
        .Default();

    registrar.Parameter("buffer_state_manager", &TThis::BufferStateManager)
        .DefaultNew();

    registrar.Parameter("load_throughput_throttler", &TThis::LoadThroughputThrottler)
        .DefaultCtor([] () {
            auto throttler = New<TLoadThroughputThrottlerSpec>();
            throttler->Limit = 128_MB;
            throttler->Period = TDuration::Seconds(1);
            return throttler;
        });

    registrar.Parameter("state_cache", &TThis::StateCache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicControllerConnectorSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("controller_wait_timeout", &TThis::ControllerWaitTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("controller_discover_period", &TThis::ControllerDiscoverPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("controller_heartbeat_period", &TThis::ControllerHeartbeatPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("controller_heartbeat_rpc_timeout", &TThis::ControllerHeartbeatRpcTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("controller_heartbeat_failure_backoff", &TThis::ControllerHeartbeatFailureBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("controller_handshake_rpc_timeout", &TThis::ControllerHandshakeRpcTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("controller_handshake_failure_backoff", &TThis::ControllerHandshakeFailureBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("orchid_update_period", &TThis::OrchidUpdatePeriod)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicStateCacheSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("compressed_cache_weight", &TThis::CompressedCacheWeight)
        .Default(NYTree::TSize(1_GB));
    registrar.Parameter("uncompressed_cache_weight", &TThis::UncompressedCacheWeight)
        .Default(NYTree::TSize(100_MB));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicPipelineSpec::Register(TRegistrar registrar)
{
    // Keep unrecognized fields so they survive the round-trip through the struct;
    // they are surfaced as warnings by ValidateSpecs instead of being silently dropped.
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

    registrar.Parameter("target_state", &TThis::TargetState)
        .Default(EPipelineState::Unknown);

    registrar.Parameter("computations", &TThis::Computations)
        .Default();
    registrar.Parameter("resources", &TThis::Resources)
        .Default();
    registrar.Parameter("throttlers", &TThis::Throttlers)
        .Default();

    registrar.Parameter("job_manager", &TThis::JobManager)
        .DefaultNew();

    registrar.Parameter("message_distributor", &TThis::MessageDistributor)
        .DefaultNew();

    registrar.Parameter("job_tracker", &TThis::JobTracker)
        .DefaultNew();

    registrar.Parameter("controller_connector", &TThis::ControllerConnector)
        .DefaultNew();

    registrar.Parameter("singletons", &TThis::Singletons)
        .DefaultNew();

    registrar.Parameter("enable_mermaid_graph_describe", &TThis::EnableMermaidGraphDescribe)
        .Default(false);

    registrar.Parameter("flow_view_cache_codec", &TThis::FlowViewCacheCodec)
        .Default(NCompression::ECodec::Zstd_2);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_ENUM(EStreamColor,
    ((White)  (0))
    ((Gray)   (1))
    ((Black)  (2))
);

void ValidateIsDirectedAcyclicGraph(
    const TStreamId& streamId,
    const THashMap<TStreamId, std::vector<TStreamId>>& outputStreams,
    THashMap<TStreamId, EStreamColor>& colors)
{
    colors[streamId] = EStreamColor::Gray;
    for (const auto& outputStreamId : GetOrCrash(outputStreams, streamId)) {
        switch (colors[outputStreamId]) {
            case EStreamColor::White:
                ValidateIsDirectedAcyclicGraph(outputStreamId, outputStreams, colors);
                break;
            case EStreamColor::Gray:
                THROW_ERROR_EXCEPTION("Stream graph should be acyclic. Cycle found around the edge %Qv => %Qv",
                    streamId,
                    outputStreamId);
                break;
            case EStreamColor::Black:
                break;
        }
    }
    colors[streamId] = EStreamColor::Black;
}

} // namespace

THashMap<TStreamId, std::vector<TStreamId>> BuildStreamGraph(const TPipelineSpecPtr& spec, bool addReadDelayEdges)
{
    THashMap<TStreamId, std::vector<TStreamId>> streamGraph;
    for (const auto& [computationId, computationSpec] : spec->Computations) {
        for (const auto& localStreamId : Concatenate(computationSpec->InputStreamIds, GetKeys(computationSpec->SourceStreams))) {
            streamGraph.try_emplace(MakeGlobalStreamId(computationId, localStreamId, computationSpec));
        }
        for (const auto& [localStreamId, localParentStreamIds] : computationSpec->StreamsDependency) {
            const auto streamId = MakeGlobalStreamId(computationId, localStreamId, computationSpec);
            streamGraph.try_emplace(streamId);
            for (const auto& localParentStreamId : localParentStreamIds) {
                const auto parentStreamId = MakeGlobalStreamId(computationId, localParentStreamId, computationSpec);
                if (streamId == parentStreamId && computationSpec->TimerStreams.contains(localStreamId) && computationSpec->AllowTimerSelfDependency) {
                    continue;
                }
                streamGraph[parentStreamId].push_back(streamId);
            }
        }
    }

    if (!addReadDelayEdges) {
        return streamGraph;
    }

    // Add read delay edges: blocker stream -> source stream.
    for (const auto& [computationId, computationSpec] : spec->Computations) {
        if (!computationSpec->WatermarkStrategy->WatermarkAlignment || !computationSpec->WatermarkStrategy->WatermarkAlignment->ReadDelays) {
            continue;
        }
        for (const auto& localSourceStreamId : GetKeys(computationSpec->SourceStreams)) {
            const auto sourceStreamId = MakeGlobalStreamId(computationId, localSourceStreamId, computationSpec);
            for (const auto& [blockerStreamId, delay] : *computationSpec->WatermarkStrategy->WatermarkAlignment->ReadDelays) {
                THROW_ERROR_EXCEPTION_UNLESS(streamGraph.contains(blockerStreamId), "Blocker stream %Qv not found", blockerStreamId);
                YT_ASSERT(streamGraph.contains(sourceStreamId)); // It is ensured in first part of this function.
                streamGraph[blockerStreamId].push_back(sourceStreamId);
            }
        }
    }
    return streamGraph;
}

void ValidateIsDirectedAcyclicGraph(const THashMap<TStreamId, std::vector<TStreamId>>& streamGraph)
{
    THashMap<TStreamId, EStreamColor> colors;
    for (const auto& streamId : GetKeys(streamGraph)) {
        if (colors[streamId] == EStreamColor::White) {
            ValidateIsDirectedAcyclicGraph(streamId, streamGraph, colors);
        }
    }
}

namespace {

std::vector<TYTPathClaim> CollectAllYTPathClaims(const TPipelineSpecPtr& spec)
{
    std::vector<TYTPathClaim> allClaims;

    auto sortedComputationIds = GetKeys(spec->Computations);
    Sort(sortedComputationIds);
    for (const auto& computationId : sortedComputationIds) {
        const auto& computationSpec = GetOrCrash(spec->Computations, computationId);

        auto collect = [&] (
            TStringBuf kind,
            TStringBuf name,
            const TYsonStructPtr& parameters) {
            std::vector<TYTPathClaim> claims;
            try {
                claims = TRegistry::Get()->CollectYTPathClaims(parameters);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION(ex)
                    << TErrorAttribute("computation_id", computationId)
                    << TErrorAttribute(TString(kind), TString(name));
            }
            for (auto& claim : claims) {
                claim.Origin = Format("computation %Qv / %v %Qv / parameter %Qv",
                    computationId,
                    kind,
                    name,
                    claim.ParameterKey);
                allClaims.push_back(std::move(claim));
            }
        };

        collect("computation", computationId.Underlying(), TRegistry::Get()->ParseComputationParameters(computationSpec));

        auto sortedSources = GetKeys(computationSpec->SourceStreams);
        Sort(sortedSources);
        for (const auto& name : sortedSources) {
            const auto& sourceSpec = GetOrCrash(computationSpec->SourceStreams, name);
            collect("source_stream", name.Underlying(), TRegistry::Get()->ParseSourceParameters(sourceSpec));
        }

        auto sortedSinks = GetKeys(computationSpec->Sinks);
        Sort(sortedSinks);
        for (const auto& name : sortedSinks) {
            const auto& sinkSpec = GetOrCrash(computationSpec->Sinks, name);
            collect("sink", name.Underlying(), TRegistry::Get()->ParseSinkParameters(sinkSpec));
        }

        auto sortedManagers = GetKeys(computationSpec->ExternalStateManagers);
        Sort(sortedManagers);
        for (const auto& name : sortedManagers) {
            const auto& managerSpec = GetOrCrash(computationSpec->ExternalStateManagers, name);
            collect("external_state_manager", name, TRegistry::Get()->ParseExternalStateManagerParameters(managerSpec));
        }

        auto sortedJoiners = GetKeys(computationSpec->ExternalStateJoiners);
        Sort(sortedJoiners);
        for (const auto& name : sortedJoiners) {
            const auto& joinerSpec = GetOrCrash(computationSpec->ExternalStateJoiners, name);
            collect("external_state_joiner", name, TRegistry::Get()->ParseExternalStateJoinerParameters(joinerSpec));
        }
    }

    return allClaims;
}

void CollectAndValidateYTPaths(const TPipelineSpecPtr& spec)
{
    auto allClaims = CollectAllYTPathClaims(spec);

    struct TGroup
    {
        const TYTPathClaim* FirstWriter = nullptr;
        const TYTPathClaim* FirstExclusive = nullptr;
    };

    THashMap<std::pair<std::string, NYPath::TYPath>, TGroup> groups;
    for (const auto& claim : allClaims) {
        if (claim.Ownership == EYTPathOwnership::ReadOnly) {
            continue;
        }
        auto& group = groups[std::pair(claim.Cluster, claim.Path)];
        bool isExclusive = claim.Ownership == EYTPathOwnership::ExclusiveWrite;
        if ((isExclusive && group.FirstWriter) || group.FirstExclusive) {
            const auto& other = group.FirstExclusive ? *group.FirstExclusive : *group.FirstWriter;
            THROW_ERROR_EXCEPTION(
                "YT path %v on cluster %Qv is claimed for writing by %v and %v; "
                "an exclusive writer cannot coexist with any other writer",
                claim.Path,
                claim.Cluster,
                other.Origin,
                claim.Origin);
        }
        if (isExclusive && !group.FirstExclusive) {
            group.FirstExclusive = &claim;
        }
        if (!group.FirstWriter) {
            group.FirstWriter = &claim;
        }
    }
}

} // namespace

std::vector<TYTPathClaim> CollectPipelineYTPaths(const TPipelineSpecPtr& spec)
{
    auto claims = CollectAllYTPathClaims(spec);
    SortBy(claims, [] (const TYTPathClaim& claim) {
        return std::tuple(claim.Cluster, claim.Path, claim.Ownership);
    });
    claims.erase(
        std::unique(
            claims.begin(),
            claims.end(),
            [] (const TYTPathClaim& lhs, const TYTPathClaim& rhs) {
                return lhs.Cluster == rhs.Cluster &&
                    lhs.Path == rhs.Path &&
                    lhs.Ownership == rhs.Ownership;
            }),
        claims.end());
    return claims;
}

void ValidatePipelineSpec(const TPipelineSpecPtr& spec)
{
    for (const auto& [streamId, streamSpec] : spec->Streams) {
        try {
            ValidateStreamSchema(*streamSpec->Schema);
            TRegistry::Get()->ValidateStreamSpec(streamSpec);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(ex)
                << TErrorAttribute("stream_id", streamId);
        }
    }

    // Validate computation specs.
    for (const auto& [computationId, computationSpec] : spec->Computations) {
        try {
            ValidateGroupBySchema(computationSpec->GroupBySchema);
            if (!computationSpec->KeyVisitorStreams.empty()) {
                const auto& columns = computationSpec->GroupBySchema->Columns();
                if (columns.empty() ||
                    columns[0].GetWireType() != NTableClient::EValueType::Uint64)
                {
                    THROW_ERROR_EXCEPTION(
                        "Computations using key_visitor_streams must have group_by_schema "
                        "with Uint64 as the first column (used as bucket hash)");
                }
                for (const auto& [streamId, streamSpec] : computationSpec->KeyVisitorStreams) {
                    if (streamSpec->Names) {
                        for (const auto& name : *streamSpec->Names) {
                            try {
                                ValidateStateName(name);
                            } catch (const std::exception& ex) {
                                THROW_ERROR_EXCEPTION(ex)
                                    << TErrorAttribute("key_visitor_stream", streamId);
                            }
                        }
                    }
                    if (streamSpec->ExternalNames) {
                        for (const auto& name : *streamSpec->ExternalNames) {
                            try {
                                ValidateStateName(name);
                            } catch (const std::exception& ex) {
                                THROW_ERROR_EXCEPTION(ex)
                                    << TErrorAttribute("key_visitor_stream", streamId);
                            }
                            if (!computationSpec->ExternalStateManagers.contains(name) &&
                                !computationSpec->ExternalStateJoiners.contains(name))
                            {
                                THROW_ERROR_EXCEPTION(
                                    "key_visitor_streams[%Qv].external_names references "
                                    "unknown external state manager or joiner %Qv",
                                    streamId,
                                    name);
                            }
                        }
                    }
                }
            }
            TRegistry::Get()->ValidateComputationSpec(computationSpec);

            if (ResolveUseCompactInputMessages(computationSpec) &&
                computationSpec->ExperimentalEnableNonUintKey.value_or(TComputationSpec::ExperimentalEnableNonUintKeyDefault))
            {
                THROW_ERROR_EXCEPTION(
                    "\"use_compact_input_messages\" cannot be enabled while "
                    "\"experimental_enable_non_uint_key\" is set: compact deduplication keys on the uint64 hash "
                    "and require the full key otherwise");
            }

            auto validateExternalStateKey = [] (TStringBuf kind, const std::string& name) {
                try {
                    ValidateStateName(name);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Invalid %v key %Qv: state names must start with \"/\"",
                            kind,
                            name)
                        << ex;
                }
            };
            for (const auto& name : GetKeys(computationSpec->ExternalStateManagers)) {
                validateExternalStateKey("external_state_managers", name);
            }
            for (const auto& [name, joinerSpec] : computationSpec->ExternalStateJoiners) {
                validateExternalStateKey("external_state_joiners", name);
                if (joinerSpec->JoinOn->KeySchemaOverride) {
                    try {
                        ValidateSchemaExpressions(joinerSpec->JoinOn->KeySchemaOverride);
                    } catch (const std::exception& ex) {
                        THROW_ERROR_EXCEPTION(
                                "Invalid \"key_schema_override\" of external state joiner %Qv",
                                name)
                            << ex;
                    }
                }
            }
            for (const auto& [name, joinerSpec] : computationSpec->StateJoiners) {
                validateExternalStateKey("state_joiners", name);
                try {
                    ValidateStateName(joinerSpec->StateName);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Invalid \"state_name\" of state joiner %Qv", name)
                        << ex;
                }
                if (joinerSpec->JoinOn->KeySchemaOverride) {
                    try {
                        ValidateSchemaExpressions(joinerSpec->JoinOn->KeySchemaOverride);
                    } catch (const std::exception& ex) {
                        THROW_ERROR_EXCEPTION(
                                "Invalid \"key_schema_override\" of state joiner %Qv",
                                name)
                            << ex;
                    }
                }
                // The joiner reads the target computation's state keyed by its group-by key, so the
                // key it computes (``key_schema_override``, or its own group-by key) must yield the
                // full target key with matching column types. Names and expressions may differ.
                auto targetIt = spec->Computations.find(joinerSpec->ComputationId);
                if (targetIt == spec->Computations.end()) {
                    THROW_ERROR_EXCEPTION("State joiner %Qv references unknown computation %Qv",
                        name,
                        joinerSpec->ComputationId);
                }
                const auto& targetKeySchema = targetIt->second->GroupBySchema;
                const auto& joinerKeySchema = joinerSpec->JoinOn->KeySchemaOverride
                    ? joinerSpec->JoinOn->KeySchemaOverride
                    : computationSpec->GroupBySchema;
                if (joinerKeySchema->GetColumnCount() != targetKeySchema->GetColumnCount()) {
                    THROW_ERROR_EXCEPTION(
                        "State joiner %Qv key has %v columns but the group-by key of target computation "
                        "%Qv has %v",
                        name,
                        joinerKeySchema->GetColumnCount(),
                        joinerSpec->ComputationId,
                        targetKeySchema->GetColumnCount());
                }
                for (int i = 0; i < joinerKeySchema->GetColumnCount(); ++i) {
                    const auto& joinerType = *MakeOptionalIfNot(joinerKeySchema->Columns()[i].LogicalType());
                    const auto& targetType = *MakeOptionalIfNot(targetKeySchema->Columns()[i].LogicalType());
                    if (joinerType != targetType) {
                        THROW_ERROR_EXCEPTION(
                            "State joiner %Qv key column %v has type %v but the group-by key of target "
                            "computation %Qv has type %v",
                            name,
                            i,
                            joinerType,
                            joinerSpec->ComputationId,
                            targetType);
                    }
                }
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(ex)
                << TErrorAttribute("computation_id", computationId);
        }
    }

    THashMap<TStreamId, TComputationId> streamProducers;
    // Fill stream producers.
    for (const auto& [computationId, computationSpec] : spec->Computations) {
        THashSet<TStreamId> sourceStreamIds;
        for (const auto& streamId : GetKeys(computationSpec->SourceStreams)) {
            sourceStreamIds.insert(streamId);
        }
        THashSet<TStreamId> timerStreamIds;
        for (const auto& streamId : GetKeys(computationSpec->TimerStreams)) {
            timerStreamIds.insert(streamId);
        }
        THashSet<TStreamId> keyVisitorStreamIds;
        for (const auto& streamId : GetKeys(computationSpec->KeyVisitorStreams)) {
            keyVisitorStreamIds.insert(streamId);
        }
        // Check local uniqueness.
        THashSet<TStreamId> localStreamIds;
        for (const auto& streamId : Concatenate(
            computationSpec->InputStreamIds,
            sourceStreamIds,
            timerStreamIds,
            keyVisitorStreamIds,
            computationSpec->OutputStreamIds))
        {
            if (!localStreamIds.insert(streamId).second) {
                THROW_ERROR_EXCEPTION("Stream %Qv is registered twice in computation %Qv",
                    streamId,
                    computationId);
            }
        }

        // Check stream spec existence.
        for (const auto& streamId : Concatenate(computationSpec->InputStreamIds, computationSpec->OutputStreamIds)) {
            if (!spec->Streams.contains(streamId)) {
                THROW_ERROR_EXCEPTION("Stream %Qv in computation %Qv is not registered in \"streams\"",
                    streamId,
                    computationId);
            }
        }
        for (const auto& [sinkId, sinkSpec] : computationSpec->Sinks) {
            for (const auto& streamId : sinkSpec->InputStreamIds) {
                if (!computationSpec->OutputStreamIds.contains(streamId)) {
                    THROW_ERROR_EXCEPTION("Input stream %Qv of sink %Qv in computation %Qv is not an output stream of this computation",
                        streamId,
                        sinkId,
                        computationId);
                }
            }
        }
        for (const auto& streamId : timerStreamIds) {
            if (spec->Streams.contains(streamId)) {
                THROW_ERROR_EXCEPTION("Timer stream %Qv of computation %Qv should not be registered in \"streams\"",
                    streamId,
                    computationId);
            }
        }
        for (const auto& streamId : keyVisitorStreamIds) {
            if (spec->Streams.contains(streamId)) {
                THROW_ERROR_EXCEPTION("Stream %Qv is a key_visitor_streams entry of computation %Qv and must not be registered in \"streams\"",
                    streamId,
                    computationId);
            }
        }

        // Check global uniqueness.
        for (const auto& streamId : computationSpec->OutputStreamIds) {
            if (!streamProducers.emplace(streamId, computationId).second) {
                THROW_ERROR_EXCEPTION("Stream %Qv is registered both in computation %Qv and %Qv",
                    streamId,
                    computationId,
                    streamProducers[streamId]);
            }
        }

        // Check grouping.
        for (const auto& streamId : computationSpec->InputStreamIds) {
            auto streamSpec = GetOrCrash(spec->Streams, streamId);
            try {
                ValidateIsGroupable(*streamSpec->Schema, *computationSpec->GroupBySchema);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION(ex)
                    << TErrorAttribute("computation_id", computationId)
                    << TErrorAttribute("stream_id", streamId);
            }
        }

        // Check joiner stream filters (shared by external and internal-state joiners).
        THashSet<TStreamId> inputStreamSet(
            computationSpec->InputStreamIds.begin(),
            computationSpec->InputStreamIds.end());
        auto validateJoinerStreams = [&] (TStringBuf kind, const std::string& joinerName, const TStateJoinSpecPtr& joinOn) {
            if (!joinOn->KeyProviderStreams) {
                return;
            }
            for (const auto& streamId : *joinOn->KeyProviderStreams) {
                if (computationSpec->OutputStreamIds.contains(streamId)) {
                    THROW_ERROR_EXCEPTION(
                        "%v %Qv references output stream %Qv "
                        "of computation %Qv in \"key_provider_streams\"",
                        kind,
                        joinerName,
                        streamId,
                        computationId);
                }
                if (!localStreamIds.contains(streamId)) {
                    THROW_ERROR_EXCEPTION(
                        "%v %Qv references unknown stream %Qv "
                        "in \"key_provider_streams\" (must be an input, source or "
                        "timer stream of computation %Qv)",
                        kind,
                        joinerName,
                        streamId,
                        computationId);
                }
                if (!joinOn->KeySchemaOverride) {
                    continue;
                }
                if (inputStreamSet.contains(streamId)) {
                    auto streamSpec = GetOrCrash(spec->Streams, streamId);
                    try {
                        ValidateIsGroupable(*streamSpec->Schema, *joinOn->KeySchemaOverride);
                    } catch (const std::exception& ex) {
                        THROW_ERROR_EXCEPTION(
                                "%v %Qv: input stream %Qv is not "
                                "groupable into key_schema_override",
                                kind,
                                joinerName,
                                streamId)
                            << TErrorAttribute("computation_id", computationId)
                            << ex;
                    }
                } else if (timerStreamIds.contains(streamId)) {
                    try {
                        ValidateIsGroupable(*computationSpec->GroupBySchema, *joinOn->KeySchemaOverride);
                    } catch (const std::exception& ex) {
                        THROW_ERROR_EXCEPTION(
                                "%v %Qv: timer stream %Qv key (= group-by) "
                                "is not groupable into key_schema_override",
                                kind,
                                joinerName,
                                streamId)
                            << TErrorAttribute("computation_id", computationId)
                            << ex;
                    }
                }
                // source_stream: silently allowed — the source schema is not materialized
                // at validation time; #ConvertPayloadToNewSchema() will surface a runtime
                // error if the actual payload schema lacks the override columns.
            }
        };
        for (const auto& [joinerName, joinerSpec] : computationSpec->ExternalStateJoiners) {
            validateJoinerStreams("External state joiner", joinerName, joinerSpec->JoinOn);
        }
        for (const auto& [joinerName, joinerSpec] : computationSpec->StateJoiners) {
            validateJoinerStreams("State joiner", joinerName, joinerSpec->JoinOn);
        }

        // Check stream dependency.
        for (const auto& streamId : Concatenate(computationSpec->InputStreamIds, sourceStreamIds)) {
            if (computationSpec->StreamsDependency.contains(streamId)) {
                THROW_ERROR_EXCEPTION("Stream %Qv in computation %Qv could not be registered in \"streams_dependency\" as a key",
                    streamId,
                    computationId);
            }
        }
        for (const auto& streamId : Concatenate(timerStreamIds, computationSpec->OutputStreamIds)) {
            if (!computationSpec->StreamsDependency.contains(streamId)) {
                THROW_ERROR_EXCEPTION("Stream %Qv in computation %Qv should be registered in \"streams_dependency\" as a key",
                    streamId,
                    computationId);
            }
        }

        for (const auto& [streamId, parentStreamIds] : computationSpec->StreamsDependency) {
            if (!timerStreamIds.contains(streamId) &&
                !keyVisitorStreamIds.contains(streamId) &&
                !computationSpec->OutputStreamIds.contains(streamId))
            {
                THROW_ERROR_EXCEPTION("Unknown stream %Qv in computation %Qv in \"streams_dependency\"",
                    streamId,
                    computationId);
            }
            for (const auto& parentStreamId : parentStreamIds) {
                if (!localStreamIds.contains(parentStreamId)) {
                    THROW_ERROR_EXCEPTION("Unknown stream %Qv in computation %Qv in \"streams_dependency\"",
                        parentStreamId,
                        computationId);
                }
                if (computationSpec->OutputStreamIds.contains(parentStreamId) && timerStreamIds.contains(streamId)) {
                    THROW_ERROR_EXCEPTION("Timer stream %Qv in computation %Qv could not depend on output stream %Qv",
                        streamId,
                        computationId,
                        parentStreamId);
                }
            }
        }

        // Check read delays.
        if (computationSpec->WatermarkStrategy &&
            computationSpec->WatermarkStrategy->WatermarkAlignment &&
            computationSpec->WatermarkStrategy->WatermarkAlignment->ReadDelays &&
            !computationSpec->WatermarkStrategy->WatermarkAlignment->ReadDelays->empty() &&
            computationSpec->SourceStreams.empty())
        {
            THROW_ERROR_EXCEPTION("Computation %Qv has non-empty \"read_delays\" but no source streams",
                computationId);
        }

        // Validate resource.
        for (const auto& resourceId : GetKeys(computationSpec->RequiredResourceIds)) {
            if (!spec->Resources.contains(resourceId)) {
                THROW_ERROR_EXCEPTION("Unknown resource %Qv required in computation %Qv",
                    resourceId,
                    computationId);
            }
        }
    }

    // Check input streams.
    for (const auto& [computationId, computationSpec] : spec->Computations) {
        for (const auto& streamId : computationSpec->InputStreamIds) {
            if (!streamProducers.contains(streamId)) {
                THROW_ERROR_EXCEPTION("Stream %Qv does not have producer in computation %Qv",
                    streamId,
                    computationId);
            }
        }
    }

    for (const auto& [resourceId, resourceSpec] : spec->Resources) {
        try {
            TRegistry::Get()->ValidateResourceSpec(resourceSpec);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(ex)
                << TErrorAttribute("resource_id", resourceId);
        }
    }

    CollectAndValidateYTPaths(spec);

    ValidateIsDirectedAcyclicGraph(BuildStreamGraph(spec, /*addReadDelayEdges*/ true));
}

void ValidateDynamicPipelineSpec(const TDynamicPipelineSpecPtr& dynamicSpec)
{
    for (const auto& [computationId, computationSpec] : dynamicSpec->Computations) {
        auto checkThrottlerId = [&] (const std::optional<TThrottlerId>& throttlerId, TStringBuf field) {
            if (throttlerId && !dynamicSpec->Throttlers.contains(*throttlerId)) {
                THROW_ERROR_EXCEPTION("Throttler %Qv referenced in %v is not declared in dynamic_spec/throttlers",
                        *throttlerId,
                        field)
                    << TErrorAttribute("computation_id", computationId);
            }
        };
        checkThrottlerId(computationSpec->InputRowsThrottlerId, "input_rows_throttler_id");
        checkThrottlerId(computationSpec->InputBytesThrottlerId, "input_bytes_throttler_id");

        auto validateStateKey = [&] (TStringBuf field, const std::string& name) {
            try {
                ValidateStateName(name);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Invalid %v key %Qv: state names must start with \"/\"",
                        field,
                        name)
                    << TErrorAttribute("computation_id", computationId)
                    << TError(ex);
            }
        };

        for (const auto& name : GetKeys(computationSpec->ExternalStateManagers)) {
            validateStateKey("external_state_managers", name);
        }
        for (const auto& name : GetKeys(computationSpec->ExternalStateJoiners)) {
            validateStateKey("external_state_joiners", name);
        }
        for (const auto& name : GetKeys(computationSpec->StateJoiners)) {
            validateStateKey("state_joiners", name);
        }

        if (computationSpec->StateManager) {
            for (const auto& name : GetKeys(computationSpec->StateManager->FormatOverrides)) {
                validateStateKey("state_manager/format_overrides", name);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
