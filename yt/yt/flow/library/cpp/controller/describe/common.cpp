#include "common.h"
#include "intermediate_description.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <yt/yt/client/ypath/rich.h>

#include <map>
#include <set>

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

template <class TVertex>
std::map<TVertex, size_t> BuildDeterministicTopologicalIndex(
    const std::map<TVertex, std::vector<TVertex>>& graph)
{
    std::map<TVertex, std::set<TVertex>> outEdges;
    std::map<TVertex, int> inDegree;

    for (const auto& [parent, children] : graph) {
        outEdges[parent];
        inDegree[parent];
        for (const auto& child : children) {
            if (outEdges[parent].insert(child).second) {
                ++inDegree[child];
                inDegree.try_emplace(child, 0);
                outEdges.try_emplace(child);
            }
        }
    }

    // Kahn's algorithm; sorted set ensures lexicographic tie-breaking.
    std::set<TVertex> ready;
    for (const auto& [vertex, degree] : inDegree) {
        if (degree == 0) {
            ready.insert(vertex);
        }
    }

    std::map<TVertex, size_t> result;
    size_t idx = 0;
    while (!ready.empty()) {
        auto it = ready.begin();
        TVertex vertex = std::move(*it);
        ready.erase(it);
        result[vertex] = idx++;
        for (const auto& child : outEdges[vertex]) {
            if (--inDegree[child] == 0) {
                ready.insert(child);
            }
        }
    }

    return result;
}

// Explicit instantiations for types used in this library.
template std::map<std::string, size_t> BuildDeterministicTopologicalIndex(
    const std::map<std::string, std::vector<std::string>>&);
template std::map<TStreamId, size_t> BuildDeterministicTopologicalIndex(
    const std::map<TStreamId, std::vector<TStreamId>>&);

////////////////////////////////////////////////////////////////////////////////

using namespace NLogging;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("text", &TThis::Text)
        .Default();
    registrar.Parameter("yson", &TThis::Yson)
        .Default();
    registrar.Parameter("error", &TThis::Error)
        .Default();
    registrar.Parameter("markdown_text", &TThis::MarkdownText)
        .Default();
    registrar.Parameter("level", &TThis::Level)
        .Default(ELogLevel::Info);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionsStats::Register(TRegistrar registrar)
{
    registrar.Parameter("count", &TThis::Count)
        .Default();
    registrar.Parameter("count_by_state", &TThis::CountByState)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TComputationDescription::Register(TRegistrar registrar)
{
    registrar.Parameter("id", &TThis::Id)
        .Default();
    registrar.Parameter("name", &TThis::Name)
        .Default();
    registrar.Parameter("class_name", &TThis::ClassName)
        .Default();
    registrar.Parameter("description", &TThis::Description)
        .Default();
    registrar.Parameter("status", &TThis::Status)
        .Default(ELogLevel::Info);
    registrar.Parameter("messages", &TThis::Messages)
        .Default();
    registrar.Parameter("group_by_schema_str", &TThis::GroupBySchemaStr)
        .Default();
    registrar.Parameter("epoch_per_second", &TThis::EpochPerSecond)
        .Default();
    registrar.Parameter("partitions_stats", &TThis::PartitionsStats)
        .Default();
    registrar.Parameter("metrics", &TThis::Metrics)
        .DefaultNew();
    registrar.Parameter("cpu_usage", &TThis::CpuUsage)
        .Default(0);
    registrar.Parameter("highlight_cpu_usage", &TThis::HighlightCpuUsage)
        .Default(true);
    registrar.Parameter("memory_usage", &TThis::MemoryUsage)
        .Default(0);
    registrar.Parameter("highlight_memory_usage", &TThis::HighlightMemoryUsage)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TPerformanceMetricsBase::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu_usage", &TThis::CpuUsage)
        .Default();
    registrar.Parameter("memory_usage", &TThis::MemoryUsage)
        .Default();
    registrar.Parameter("messages_per_second", &TThis::MessagesPerSecond)
        .Default();
    registrar.Parameter("bytes_per_second", &TThis::BytesPerSecond)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionDescription::Register(TRegistrar registrar)
{
    registrar.Parameter("partition_id", &TThis::PartitionId)
        .Default();
    registrar.Parameter("computation_id", &TThis::ComputationId)
        .Default();
    registrar.Parameter("lower_key", &TThis::LowerKey)
        .Default();
    registrar.Parameter("upper_key", &TThis::UpperKey)
        .Default();
    registrar.Parameter("source_key", &TThis::SourceKey)
        .Default();
    registrar.Parameter("key_or_range", &TThis::KeyOrRange)
        .Default();
    registrar.Parameter("lexicographically_serialized_key_or_range", &TThis::LexicographicallySerializedKeyOrRange)
        .Default();
    registrar.Parameter("current_job_id", &TThis::CurrentJobId)
        .Default();
    registrar.Parameter("current_worker_address", &TThis::CurrentWorkerAddress)
        .Default();
    registrar.Parameter("current_worker_incarnation_id", &TThis::CurrentWorkerIncarnationId)
        .Default();
    registrar.Parameter("state", &TThis::State)
        .Default();
    registrar.Parameter("job_state", &TThis::JobState)
        .Default();
    registrar.Parameter("status", &TThis::Status)
        .Default(NLogging::ELogLevel::Info);
    registrar.Parameter("last_retryable_error_instant", &TThis::LastRetryableErrorInstant)
        .Default();
    registrar.Parameter("previous_job_fail_instant", &TThis::PreviousJobFailInstant)
        .Default();
    registrar.Parameter("previous_rebalancing_instant", &TThis::PreviousRebalancingInstant)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TWorkerDescription::Register(TRegistrar registrar)
{
    RegisterNodeInfoStruct(registrar);

    registrar.Parameter("address", &TThis::Address)
        .Default();
    registrar.Parameter("groups", &TThis::Groups)
        .Default();
    registrar.Parameter("register_time", &TThis::RegisterTime)
        .Default();
    registrar.Parameter("status", &TThis::Status)
        .Default(NLogging::ELogLevel::Info);
    registrar.Parameter("partitions", &TThis::Partitions)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void AppendPartitionStats(TPartitionsStats& stats, const NFlow::TPartitionPtr partition)
{
    stats.Count++;
    auto convertedState = EPartitionDescribeState::Unknown;
    switch (partition->State) {
        case EPartitionState::Executing:
            convertedState = EPartitionDescribeState::Executing;
            break;
        case EPartitionState::Interrupting:
        case EPartitionState::Completing:
            convertedState = EPartitionDescribeState::Transient;
            break;
        case EPartitionState::Completed:
            convertedState = EPartitionDescribeState::Completed;
            break;
        case EPartitionState::Interrupted:
            convertedState = EPartitionDescribeState::Interrupted;
            break;
    }
    stats.CountByState[convertedState]++;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ValidateSpecs(TPipelineSpecPtr pipelineSpec, TDynamicPipelineSpecPtr dynamicPipelineSpec, std::vector<TMessage>& messages)
{
    auto staticSpecErrors = TRegistry::Get()->ValidatePipelineSpecParseability(ConvertTo<IMapNodePtr>(pipelineSpec));
    auto dynamicSpecErrors = TRegistry::Get()->ValidateDynamicPipelineSpecParseability(pipelineSpec, ConvertTo<IMapNodePtr>(dynamicPipelineSpec));

    for (auto& error : staticSpecErrors) {
        auto& message = messages.emplace_back();
        message.Error = error;
        message.Text = "Pipeline static spec parseability error";
        message.Level = ELogLevel::Warning;
    }

    for (auto& error : dynamicSpecErrors) {
        auto& message = messages.emplace_back();
        message.Error = error;
        message.Text = "Pipeline dynamic spec parseability error";
        message.Level = ELogLevel::Warning;
    }
}

////////////////////////////////////////////////////////////////////////////////

THashSet<TComputationId> GetTopForHighlighting(const THashMap<TComputationId, double>& values)
{
    if (values.size() < 2) {
        return {};
    }

    double total = 0;
    for (const auto& [computationId, value] : values) {
        total += value;
    }

    double thresholdMultiplier = 1.0 / std::sqrt(values.size());
    double threshold = total * thresholdMultiplier;

    THashSet<TComputationId> result;
    for (const auto& [computationId, value] : values) {
        if (value > threshold) {
            result.insert(computationId);
        }
    }
    return result;
}

void FillJobFailErrors(const THashMap<EJobFinishReason, TError>& errors, std::vector<TMessage>& messages, NLogging::ELogLevel* status)
{
    if (errors.empty()) {
        return;
    }
    if (status) {
        *status = std::max(*status, ELogLevel::Warning);
    }

    for (const auto& [reason, error] : errors) {
        auto& message = messages.emplace_back();
        message.Level = ELogLevel::Warning;
        message.Text = Format("Job failed (JobFinishReason: %v): %v", reason, error.GetMessage());
        message.Error = error;
    }
}

void FillRetryableErrors(const THashMap<std::string, TError>& errors, std::vector<TMessage>& messages, NLogging::ELogLevel* status)
{
    if (errors.empty()) {
        return;
    }
    if (status) {
        *status = std::max(*status, ELogLevel::Warning);
    }
    for (const auto& [component, error] : errors) {
        auto& message = messages.emplace_back();
        message.Level = ELogLevel::Warning;
        message.Text = Format("Retryable error in component %Qv: %v", component, error.GetMessage());
        message.Error = error;
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

double GetMostStableCpuUsage(const TNodePerformanceMetricsPtr& metrics)
{
    return metrics->CpuUsage10m.value_or(metrics->CpuUsage30s.value_or(metrics->CpuUsageCurrent.value_or(0.0)));
}

i64 GetMostStableMemoryUsage(const TNodePerformanceMetricsPtr& metrics)
{
    for (i64 value : {metrics->MemoryUsage10m, metrics->MemoryUsage30s, metrics->MemoryUsageCurrent}) {
        if (value) {
            return value;
        }
    }
    return 0;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

THashMap<TComputationId, TComputationDescription> MakeComputationDescriptions(
    const TFlowViewPtr& flowView,
    const THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>>& intermediateDescriptions)
{
    THashMap<TComputationId, TComputationDescription> computationDescriptions;

    auto layout = flowView->State->ExecutionSpec->Layout;
    auto spec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();
    auto dynamicSpec = flowView->State->ExecutionSpec->DynamicPipelineSpec->GetValue();

    TDescribeTraitsContext describeTraitsContext{.PipelinePath = flowView->EphemeralState->PipelinePath};

    // Create computation descriptions for all computations.
    for (const auto& [computationId, computationSpec] : spec->Computations) {
        auto& computationDescription = computationDescriptions[computationId];
        computationDescription.Id = MakeComputationGraphId(computationId);
        computationDescription.Name = computationId.Underlying();
        computationDescription.ClassName = computationSpec->ComputationClassName;

        auto intermediatePartitions = GetOrDefault(intermediateDescriptions, computationId);
        for (const auto& intermediatePartition : intermediatePartitions) {
            AppendPartitionStats(computationDescription.PartitionsStats, intermediatePartition.Partition);
        }

        // Errors.
        THashMap<std::string, TError> retryableErrors;
        THashMap<EJobFinishReason, TError> jobFailErrors;
        for (const auto& intermediatePartition : intermediatePartitions) {
            if (intermediatePartition.PartitionJobStatus && intermediatePartition.PartitionJobStatus->CurrentJobStatus) {
                const auto& currentJobStatus = intermediatePartition.PartitionJobStatus->CurrentJobStatus;
                NodePerformanceMetricsAdd(computationDescription.Metrics, currentJobStatus->PerformanceMetrics);

                // Try to get fast and stable estimations of usages. So take most stable metric for every job and accumulate them.
                computationDescription.CpuUsage += GetMostStableCpuUsage(currentJobStatus->PerformanceMetrics);
                computationDescription.MemoryUsage += GetMostStableMemoryUsage(currentJobStatus->PerformanceMetrics);

                for (const auto& [component, error] : currentJobStatus->RetryableErrors) {
                    auto [it, success] = retryableErrors.try_emplace(component, error);
                    if (success) {
                        it->second <<= TErrorAttribute("flow_partition_id", intermediatePartition.Partition->PartitionId);
                    }
                }
            }
            if (const auto& state = intermediatePartition.PartitionEphemeralState) {
                if (!state->PreviousJobFailError.IsOK()) {
                    auto [it, success] = jobFailErrors.try_emplace(state->PreviousJobFinishReason, state->PreviousJobFailError);
                    if (success) {
                        it->second <<= TErrorAttribute("flow_partition_id", intermediatePartition.Partition->PartitionId);
                    }
                }
            }
        }
        FillJobFailErrors(jobFailErrors, computationDescription.Messages, &computationDescription.Status);
        FillRetryableErrors(retryableErrors, computationDescription.Messages, &computationDescription.Status);

        // Specs and group by schema.
        {
            auto& message = computationDescription.Messages.emplace_back();
            message.Text = "Spec";
            message.Yson = ConvertToYsonString(MakePrettyForUIComputationSpec(computationSpec, describeTraitsContext));
        }
        if (auto* dynamicComputationSpec = dynamicSpec->Computations.FindPtr(computationId)) {
            auto& message = computationDescription.Messages.emplace_back();
            message.Text = "DynamicSpec";
            message.Yson = ConvertToYsonString(MakePrettyForUIDynamicComputationSpec(*dynamicComputationSpec, computationSpec, describeTraitsContext));
        }

        for (const auto& column : computationSpec->GroupBySchema->Columns()) {
            if (!computationDescription.GroupBySchemaStr.empty()) {
                computationDescription.GroupBySchemaStr += ", ";
            }
            if (column.Expression() && !column.Expression().value().empty()) {
                computationDescription.GroupBySchemaStr += column.Expression().value();
            } else {
                computationDescription.GroupBySchemaStr += column.Name();
            }
        }
    }

    // Cpu and memory usage.
    THashMap<TComputationId, double> cpuUsages;
    THashMap<TComputationId, double> memoryUsages;
    for (auto& [computationId, computationDescription] : computationDescriptions) {
        cpuUsages[computationId] = computationDescription.CpuUsage;
        memoryUsages[computationId] = computationDescription.MemoryUsage;
    }
    auto cpuHighlights = GetTopForHighlighting(cpuUsages);
    auto memoryHighlights = GetTopForHighlighting(memoryUsages);
    for (auto& [computationId, computationDescription] : computationDescriptions) {
        computationDescription.HighlightCpuUsage = cpuHighlights.contains(computationId);
        computationDescription.HighlightMemoryUsage = memoryHighlights.contains(computationId);
    }

    return computationDescriptions;
}

////////////////////////////////////////////////////////////////////////////////

void FillPartitionDescription(
    const TPartitionIntermediateDescription& intermediateDescription,
    TPartitionDescription& description,
    std::vector<TMessage>& messages)
{
    const auto& flowViewPartition = intermediateDescription.Partition;
    const auto& partitionJobStatus = intermediateDescription.PartitionJobStatus;
    const auto& partitionEphemaralState = intermediateDescription.PartitionEphemeralState;

    description.PartitionId = flowViewPartition->PartitionId;
    description.ComputationId = flowViewPartition->ComputationId;

    auto serializeKey = [] (const TKey& key) -> std::string {
        return ConvertToYsonString(key, EYsonFormat::Text).ToString();
    };

    if (flowViewPartition->SourceKey.has_value()) {
        description.SourceKey = serializeKey(*flowViewPartition->SourceKey);
        description.KeyOrRange = *description.SourceKey;
        description.LexicographicallySerializedKeyOrRange = LexicographicallySerializeUnversionedRowV2(flowViewPartition->SourceKey->Underlying().Get());
    } else if (flowViewPartition->LowerKey.has_value() && flowViewPartition->UpperKey.has_value()) {
        description.LowerKey = serializeKey(*flowViewPartition->LowerKey);
        description.UpperKey = serializeKey(*flowViewPartition->UpperKey);
        description.KeyOrRange = Format("%v ... %v", *description.LowerKey, *description.UpperKey);
        description.LexicographicallySerializedKeyOrRange = LexicographicallySerializeUnversionedRowV2(flowViewPartition->LowerKey->Underlying().Get());
    }

    description.CurrentJobId = flowViewPartition->CurrentJobId;
    description.State = flowViewPartition->State;

    if (partitionJobStatus && partitionJobStatus->CurrentJobStatus) {
        const auto& jobStatus = partitionJobStatus->CurrentJobStatus;
        FillRetryableErrors(jobStatus->RetryableErrors, messages, &description.Status);
        description.LastRetryableErrorInstant = partitionJobStatus->LastRetryableErrorInstant;

        const auto& performanceMetrics = jobStatus->PerformanceMetrics;
        description.CpuUsage = GetMostStableCpuUsage(performanceMetrics);
        description.MemoryUsage = performanceMetrics->MemoryUsage10m;

        if (jobStatus->InputMetrics) {
            description.MessagesPerSecond += jobStatus->InputMetrics->Global.MessagesPerSecond;
            description.BytesPerSecond += jobStatus->InputMetrics->Global.BytesPerSecond;
        }
    }

    if (partitionEphemaralState) {
        description.PreviousJobFailInstant = partitionEphemaralState->PreviousJobFailInstant;
        description.PreviousRebalancingInstant = partitionEphemaralState->PreviousRebalancingInstant;

        if (!partitionEphemaralState->PreviousJobFailError.IsOK()) {
            THashMap<EJobFinishReason, TError> jobFailErrors;
            auto [it, success] = jobFailErrors.try_emplace(
                partitionEphemaralState->PreviousJobFinishReason,
                partitionEphemaralState->PreviousJobFailError);
            if (success) {
                it->second <<= TErrorAttribute("flow_partition_id", description.PartitionId);
            }
            FillJobFailErrors(jobFailErrors, messages, &description.Status);
        }
    }

    description.JobState = std::invoke([&] {
        if (!flowViewPartition->CurrentJobId.has_value()) {
            if (partitionEphemaralState && partitionEphemaralState->PreviousJobFinishReason == EJobFinishReason::Stopped) {
                return EPartitionJobState::Stopped;
            } else {
                return EPartitionJobState::Unknown;
            }
        }
        if (partitionJobStatus && partitionJobStatus->CurrentJobStatus && partitionJobStatus->CurrentJobStatus->InitedTime.has_value()) {
            return EPartitionJobState::Working;
        } else {
            return EPartitionJobState::Recovering;
        }
    });

    if (intermediateDescription.Job) {
        description.CurrentWorkerAddress = intermediateDescription.Job->WorkerAddress;
        description.CurrentWorkerIncarnationId = intermediateDescription.Job->WorkerIncarnationId;
    }
}

////////////////////////////////////////////////////////////////////////////////


void FillTopHeavyHittersMessages(
    const TFlowViewPtr& flowView,
    const THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>>& intermediateDescriptions,
    const TComputationId& computationId,
    TComputationDescription& computationDescription)
{
    auto spec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();
    auto computationSpec = spec->Computations.at(computationId);

    auto partitionIntermediateDescriptions = GetOrDefault(intermediateDescriptions, computationId);

    std::vector<TNodeInputMetricsPtr> partitionMetrics;
    partitionMetrics.reserve(partitionIntermediateDescriptions.size());
    for (auto const& partitionIntermediateDescription : partitionIntermediateDescriptions) {
        if (partitionIntermediateDescription.PartitionJobStatus && partitionIntermediateDescription.PartitionJobStatus->CurrentJobStatus) {
            partitionMetrics.push_back(partitionIntermediateDescription.PartitionJobStatus->CurrentJobStatus->InputMetrics);
        }
    }

    std::vector<std::pair<double, TKey>> heavyHittersVector;
    if (!partitionMetrics.empty()) {
        heavyHittersVector = AggregateNodeInputMetrics(partitionMetrics)->Global->Total.HeavyHitters;
    }

    int topSize = std::min(static_cast<size_t>(computationSpec->HeavyHitters.Limit), heavyHittersVector.size());
    if (topSize > 0) {
        std::partial_sort(heavyHittersVector.begin(), heavyHittersVector.begin() + topSize, heavyHittersVector.end(), std::greater{});
        heavyHittersVector.resize(topSize);

        THashMap<TKey, TPartitionId> keyToPartitionId;
        // Fill keys.
        keyToPartitionId.reserve(heavyHittersVector.size());
        for (const auto& [ratio, key] : heavyHittersVector) {
            keyToPartitionId[key]; // Just create key with default value.
        }
        // Fill values for filled keys.
        for (auto const& partitionIntermediateDescription : partitionIntermediateDescriptions) {
            if (partitionIntermediateDescription.PartitionJobStatus && partitionIntermediateDescription.PartitionJobStatus->CurrentJobStatus) {
                for (const auto& [ratio, key] : partitionIntermediateDescription.PartitionJobStatus->CurrentJobStatus->InputMetrics->Global.HeavyHitters) {
                    auto it = keyToPartitionId.find(key);
                    if (it != keyToPartitionId.end()) {
                        it->second = partitionIntermediateDescription.Partition->PartitionId;
                    }
                }
            }
        }

        std::vector<std::string> topHeavyHitters;
        for (const auto& [ratio, key] : heavyHittersVector) {
            topHeavyHitters.push_back(Format("Key=%v, Ratio=%v, PartitionId=%v", key, ratio, keyToPartitionId[key]));
        }

        auto& message = computationDescription.Messages.emplace_back();
        message.Level = ELogLevel::Info;
        message.Text = Format("Top %v heavy hitters\n", heavyHittersVector.size());
        message.Yson = ConvertToYsonString(topHeavyHitters, NYson::EYsonFormat::Text);
    }
}

////////////////////////////////////////////////////////////////////////////////

void FillWorkerErrors(
    const TFlowViewPtr& flowView,
    const TWorkerPtr& worker,
    std::vector<TMessage>& messages,
    NLogging::ELogLevel* status)
{
    if (auto workerStatus = GetOrDefault(flowView->Feedback->WorkerStatuses, worker->RpcAddress)) {
        if (!workerStatus->PreviousCrashError.IsOK()) {
            if (status) {
                *status = std::max(*status, ELogLevel::Error);
            }
            auto& message = messages.emplace_back();
            message.Level = ELogLevel::Error;
            message.Text = "Worker crashed; it is probably an assertion failure or a segmentation fault";
            message.MarkdownText = Format("```WorkerRpcAddress: %v\n%v\n```", worker->RpcAddress, workerStatus->PreviousCrashError); // Has multiline attributes, they are better rendered as text.
        }
        for (const auto& [component, error] : workerStatus->Errors) {
            if (status) {
                *status = std::max(*status, ELogLevel::Warning);
            }
            auto& message = messages.emplace_back();
            message.Level = ELogLevel::Warning;
            message.Text = Format("Worker error (Component: %v, WorkerRpcAddress: %v)", component, worker->RpcAddress);
            message.Error = error;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void FillWorkerDescription(
    const TFlowViewPtr& flowView,
    const TWorkerPtr& worker,
    const std::vector<TPartitionIntermediateDescription>& partitionDescriptions,
    TWorkerDescription& description,
    std::vector<TMessage>& messages)
{
    description.Load(ConvertTo<IMapNodePtr>(worker)); // All fields of TWorker are included in TWorkerDescription now.
    description.Address = worker->RpcAddress;         // Compatibility for UI.

    // Process pre-computed partition descriptions.
    for (const auto& partitionDesc : partitionDescriptions) {
        std::vector<TMessage> partitionMessages; // To be ignored.
        FillPartitionDescription(partitionDesc, description.Partitions.emplace_back(), partitionMessages);
    }

    if (description.Partitions.empty()) {
        return;
    }

    auto doForAllFields = [] (auto callback) {
#define XX(metric) callback([] (auto& metrics) -> auto& { \
    return metrics.metric;                                \
});
        YTFLOW_ITERATE_PERFORMANCE_METRICS_FIELDS(XX);
#undef XX
    };

    doForAllFields([&] (auto getter) {
        getter(description) = 0;
        for (const auto& partition : description.Partitions) {
            getter(description) += getter(partition);
        }
    });

    FillWorkerErrors(flowView, worker, messages, &description.Status);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Applies the entity's describe traits over its parameters node, in place.
//! A null #traits is a no-op.
void ApplyDescribeTraits(const IMapNodePtr& parameters, const IDescribeTraitsPtr& traits)
{
    if (traits && parameters) {
        traits->MakeLinks(parameters);
    }
}

//! Returns a copy of #originalNode with #traits applied (only if it is a map). A null #traits is a no-op.
INodePtr MakeLinksWithTraits(const INodePtr& originalNode, const IDescribeTraitsPtr& traits)
{
    auto node = ConvertToNode(originalNode); // Make a copy.
    if (node->GetType() == ENodeType::Map) {
        ApplyDescribeTraits(node->AsMap(), traits);
    }
    return node;
}

//! Shared body for both the static and dynamic "pretty for UI" computation specs.
//! #spec is cloned and linkified in place; class names are always resolved from #classSpec
//! (the static computation spec) by stream/sink/name id, since the dynamic spec carries none.
//! For the static case #spec and #classSpec are the same object, so the lookups always hit.
template <class TSpec>
INodePtr MakePrettyForUIComputationSpecImpl(
    const TIntrusivePtr<TSpec>& spec,
    const TComputationSpecPtr& classSpec,
    const TDescribeTraitsContext& context)
{
    auto* registry = TRegistry::Get();

    // Clone so the entity parameters can be rewritten in place without touching the original spec.
    auto clone = CloneYsonStruct(spec);

    ApplyDescribeTraits(clone->Parameters, registry->CreateComputationDescribeTraits(classSpec->ComputationClassName, context));
    for (const auto& [streamId, sourceSpec] : clone->SourceStreams) {
        if (auto classSourceSpec = classSpec->SourceStreams.FindPtr(streamId); classSourceSpec && sourceSpec->Parameters) {
            sourceSpec->Parameters = MakeSourceLinks(sourceSpec->Parameters, (*classSourceSpec)->SourceClassName, context)->AsMap();
        }
    }
    for (const auto& [sinkId, sinkSpec] : clone->Sinks) {
        if (auto classSinkSpec = classSpec->Sinks.FindPtr(sinkId); classSinkSpec && sinkSpec->Parameters) {
            sinkSpec->Parameters = MakeSinkLinks(sinkSpec->Parameters, (*classSinkSpec)->SinkClassName, context)->AsMap();
        }
    }
    // State managers/joiners live in their own spec blocks, not under the computation parameters.
    for (const auto& [name, stateManagerSpec] : clone->ExternalStateManagers) {
        if (auto classStateManagerSpec = classSpec->ExternalStateManagers.FindPtr(name)) {
            ApplyDescribeTraits(stateManagerSpec->Parameters, registry->CreateExternalStateManagerDescribeTraits((*classStateManagerSpec)->ExternalStateManagerClassName, context));
        }
    }
    for (const auto& [name, stateJoinerSpec] : clone->ExternalStateJoiners) {
        if (auto classStateJoinerSpec = classSpec->ExternalStateJoiners.FindPtr(name)) {
            ApplyDescribeTraits(stateJoinerSpec->Parameters, registry->CreateExternalStateJoinerDescribeTraits((*classStateJoinerSpec)->ExternalStateJoinerClassName, context));
        }
    }

    return ConvertToNode(clone);
}

} // namespace

INodePtr MakeSourceLinks(const INodePtr& parameters, const std::string& sourceClassName, const TDescribeTraitsContext& context)
{
    return MakeLinksWithTraits(parameters, TRegistry::Get()->CreateSourceDescribeTraits(sourceClassName, context));
}

INodePtr MakeSinkLinks(const INodePtr& parameters, const std::string& sinkClassName, const TDescribeTraitsContext& context)
{
    return MakeLinksWithTraits(parameters, TRegistry::Get()->CreateSinkDescribeTraits(sinkClassName, context));
}

INodePtr MakePrettyForUIComputationSpec(const TComputationSpecPtr& computationSpec, const TDescribeTraitsContext& context)
{
    // The class names live on the spec itself, so it doubles as the class spec.
    return MakePrettyForUIComputationSpecImpl(computationSpec, computationSpec, context);
}

INodePtr MakePrettyForUIDynamicComputationSpec(
    const TDynamicComputationSpecPtr& dynamicSpec,
    const TComputationSpecPtr& staticSpec,
    const TDescribeTraitsContext& context)
{
    // The dynamic spec carries no class names; resolve them from the static spec.
    return MakePrettyForUIComputationSpecImpl(dynamicSpec, staticSpec, context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
