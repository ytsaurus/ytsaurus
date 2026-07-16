#include "describe_pipeline.h"
#include "draw_pipeline_graph.h"
#include "fill_graph_limits.h"

#include <yt/yt/flow/library/cpp/common/authenticator.h>
#include <yt/yt/flow/library/cpp/common/flow_core_build_info.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/internal_urls.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/debug_build_warning.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NFlow::NDescribe {

using namespace NLogging;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

TSinkId MakeGlobalSinkId(const TComputationId& computationId, const TSinkId& localSinkId)
{
    return TSinkId(Format("%v/%v", computationId, localSinkId));
}

////////////////////////////////////////////////////////////////////////////////

constexpr auto& VcsCommitUrlPrefix = NInternalUrls::VcsCommitUrlPrefix;
constexpr auto& VcsRevisionUrlPrefix = NInternalUrls::VcsRevisionUrlPrefix;
constexpr auto& VcsBranchUrlPrefix = NInternalUrls::VcsBranchUrlPrefix;
constexpr auto& FlowCoreTargetDocUrl = NInternalUrls::FlowCoreTargetDocUrl;

TString CommitUrl(const TFlowCoreBuildInfo& buildInfo)
{
    if (buildInfo.CommitHash.empty() || VcsCommitUrlPrefix.empty()) {
        return {};
    }
    return Format("%v%v", VcsCommitUrlPrefix, buildInfo.CommitHash);
}

TString RevisionUrl(const TFlowCoreBuildInfo& buildInfo)
{
    if (buildInfo.SvnRevision <= 0 || VcsRevisionUrlPrefix.empty()) {
        return {};
    }
    return Format("%v%v", VcsRevisionUrlPrefix, buildInfo.SvnRevision);
}

TString BranchUrl(const TFlowCoreBuildInfo& buildInfo)
{
    if (buildInfo.Branch.empty() || VcsBranchUrlPrefix.empty()) {
        return {};
    }
    return Format("%v%v", VcsBranchUrlPrefix, buildInfo.Branch);
}

//! Renders |text| either as a markdown link or as a plain inline code block.
TString FormatMaybeLink(std::string_view text, std::string_view url)
{
    return url.empty()
        ? Format("`%v`", text)
        : Format("[%v](%v)", text, url);
}

//! Parses a `FlowCoreVersion` value and, when the value is a commit hash, returns {hash, commit-url}.
std::pair<std::string_view, TString> ExtractCommit(std::string_view flowCoreVersion)
{
    constexpr std::string_view kSuffix = " (commit hash)";
    if (!flowCoreVersion.ends_with(kSuffix)) {
        return {};
    }
    auto hash = flowCoreVersion.substr(0, flowCoreVersion.size() - kSuffix.size());
    auto url = VcsCommitUrlPrefix.empty()
        ? TString{}
        : Format("%v%v", VcsCommitUrlPrefix, hash);
    return {hash, std::move(url)};
}

////////////////////////////////////////////////////////////////////////////////

TString FormatRunnerBuildInfo(const TFlowCoreTarget& flowCoreTarget)
{
    TStringBuilder out;
    out.AppendFormat("**Runner build info:**\n");
    const auto& targetValue = flowCoreTarget.Underlying();
    if (targetValue.empty()) {
        out.AppendFormat("* Binary version (FlowCoreTarget): not set\n");
    } else {
        out.AppendFormat("* Binary version (FlowCoreTarget): `%v`\n", flowCoreTarget);
        if (auto [hash, url] = ExtractCommit(targetValue); !hash.empty()) {
            out.AppendFormat("* Commit: %v\n", FormatMaybeLink(hash, url));
        }
    }
    return out.Flush();
}

TString FormatControllerBuildInfo(std::string_view controllerFlowCoreVersion, const TFlowCoreBuildInfo& buildInfo)
{
    TStringBuilder out;
    out.AppendFormat("**Controller build info:**\n");
    out.AppendFormat("* Binary version: `%v`\n", controllerFlowCoreVersion);
    if (buildInfo.IsEmpty()) {
        return out.Flush();
    }
    if (!buildInfo.CommitHash.empty()) {
        out.AppendFormat("* Commit: %v", FormatMaybeLink(buildInfo.CommitHash, CommitUrl(buildInfo)));
        if (buildInfo.SvnRevision > 0) {
            out.AppendFormat(" (%v)", FormatMaybeLink(Format("r%v", buildInfo.SvnRevision), RevisionUrl(buildInfo)));
        }
        out.AppendChar('\n');
    }
    if (!buildInfo.Author.empty()) {
        out.AppendFormat("* Author: `%v`\n", buildInfo.Author);
    }
    if (!buildInfo.Branch.empty()) {
        out.AppendFormat("* Branch: %v\n", FormatMaybeLink(buildInfo.Branch, BranchUrl(buildInfo)));
    }
    if (!buildInfo.Tag.empty()) {
        out.AppendFormat("* Tag: `%v`\n", buildInfo.Tag);
    }
    // Render the commit message up to the first non-ASCII byte.
    if (!buildInfo.CommitSummary.empty()) {
        auto [asciiPrefix, truncated] = NDetail::ExtractAsciiPrefix(buildInfo.CommitSummary);
        if (truncated) {
            out.AppendFormat("* Message: %v`<non-ascii text not supported yet>`\n", asciiPrefix);
        } else {
            out.AppendFormat("* Message: %v\n", asciiPrefix);
        }
    }
    if (buildInfo.BuildTimestamp > 0) {
        out.AppendFormat("* Built at: `%v`\n",
            TInstant::Seconds(buildInfo.BuildTimestamp).ToStringUpToSeconds());
    }
    if (!buildInfo.BuildHost.empty()) {
        out.AppendFormat("* Built on: `%v`\n", buildInfo.BuildHost);
    }
    return out.Flush();
}

//! Renders the Worker build-info section. Groups accepted workers by their
//! FlowCoreVersion and lists mismatched-version groups with one example address.
TString FormatWorkerBuildInfo(const TFlowViewPtr& flowView)
{
    THashMap<std::string, ui64> acceptedByVersion;
    for (const auto& [_, worker] : flowView->State->Workers) {
        ++acceptedByVersion[worker->FlowCoreVersion];
    }
    const auto& mismatchedVersionGroups = flowView->EphemeralState->FlowCoreTargetMismatchedWorkers;
    if (acceptedByVersion.empty() && mismatchedVersionGroups.empty()) {
        return {};
    }

    TStringBuilder out;
    auto emitGroup = [&] (std::string_view version, TString suffix) {
        out.AppendFormat("* Binary version: `%v`%v\n", version, suffix);
        if (auto [hash, url] = ExtractCommit(version); !hash.empty()) {
            out.AppendFormat("* Commit: %v\n", FormatMaybeLink(hash, url));
        }
    };
    out.AppendFormat("**Worker build info:**\n");
    for (const auto& [version, count] : acceptedByVersion) {
        emitGroup(version, Format(" -- %v worker(s)", count));
    }
    for (const auto& [version, group] : mismatchedVersionGroups) {
        emitGroup(version, Format(" -- %v worker(s) (excluded due to mismatch, example: `%v`)", group.Count, group.ExampleAddress));
    }
    return out.Flush();
}

void FillFlowCoreTargetMessage(
    const TFlowViewPtr& flowView,
    const std::string& controllerFlowCoreVersion,
    std::vector<TMessage>& messages)
{
    const auto& flowCoreTarget = flowView->State->ExecutionSpec->FlowCoreTarget->GetValue();
    const auto& targetValue = flowCoreTarget.Underlying();
    const bool targetNotSet = targetValue.empty();
    const bool controllerMatches = !targetNotSet && targetValue == controllerFlowCoreVersion;

    ui64 totalMismatchedWorkers = 0;
    for (const auto& [_, group] : flowView->EphemeralState->FlowCoreTargetMismatchedWorkers) {
        totalMismatchedWorkers += group.Count;
    }

    auto& message = messages.emplace_back();
    if (!targetNotSet && !controllerMatches) {
        message.Text = "Controller and Runner binary mismatch - pipeline paused. Rollout in progress?";
        message.Level = ELogLevel::Error;
    } else if (totalMismatchedWorkers > 0) {
        message.Text = "Controller and Worker binary mismatch - some workers excluded. Rollout in progress?";
        message.Level = ELogLevel::Error;
    } else if (targetNotSet) {
        message.Text = "Zombie-processes protection is off";
        message.Level = ELogLevel::Warning;
    } else {
        message.Text = "Flow binaries matching";
    }

    TStringBuilder markdownText;
    const bool hasMismatch = targetNotSet || !controllerMatches || totalMismatchedWorkers > 0;
    if (hasMismatch && !FlowCoreTargetDocUrl.empty()) {
        markdownText.AppendFormat(
            "**How to recover**: wait for a rollout complete, see [FlowCoreTarget](%v) if not resolved.\n\n",
            FlowCoreTargetDocUrl);
    }
    markdownText.AppendString(FormatRunnerBuildInfo(flowCoreTarget));
    markdownText.AppendChar('\n');
    markdownText.AppendString(FormatControllerBuildInfo(controllerFlowCoreVersion, *GetFlowCoreBuildInfo()));
    markdownText.AppendChar('\n');
    if (auto worker = FormatWorkerBuildInfo(flowView); !worker.empty()) {
        markdownText.AppendString(worker);
        markdownText.AppendChar('\n');
    }
    message.MarkdownText = markdownText.Flush();
}

//! Mirrors NBalancer::WorkerBelongsToGroup (job_balancer_common.cpp); duplicated here because the
//! describe library cannot depend on the controller library (that edge would be circular). A worker
//! that declares no groups belongs to the default (empty) group only.
bool WorkerBelongsToGroup(const TWorkerPtr& worker, const TWorkerGroupId& workerGroup)
{
    if (worker->Groups.empty()) {
        return workerGroup.Underlying().empty();
    }
    for (const auto& group : worker->Groups) {
        if (group == workerGroup) {
            return true;
        }
    }
    return false;
}

//! Emits an error message for each used worker group that has fewer workers than the configured
//! minimum. The minimum (TDynamicJobManagerSpec::MinimumWorkerCount) is global, but it must hold per
//! used group: a group with no workers cannot run its computations even when the overall worker count
//! looks healthy. Mirrors the controller-side guard in TJobManager::DistributeJobs.
void FillWorkerCountMessages(
    const TFlowViewPtr& flowView,
    const TPipelineSpecPtr& pipelineSpec,
    const TDynamicPipelineSpecPtr& dynamicPipelineSpec,
    std::vector<TMessage>& messages)
{
    const auto& jobManagerSpec = dynamicPipelineSpec->JobManager;

    THashSet<TWorkerGroupId> usedWorkerGroups;
    for (const auto& [computationId, computationSpec] : pipelineSpec->Computations) {
        usedWorkerGroups.insert(computationSpec->WorkerGroup);
    }

    for (const auto& workerGroup : usedWorkerGroups) {
        // The required count comes from the group's override when present (mirrors the controller).
        const TDynamicJobManagerGroupSpec* groupJobManagerSpec = jobManagerSpec.Get();
        if (auto* overrideSpec = jobManagerSpec->WorkerGroupOverride.FindPtr(workerGroup)) {
            groupJobManagerSpec = overrideSpec->Get();
        }
        const auto minimumWorkerCount = groupJobManagerSpec->MinimumWorkerCount;
        if (minimumWorkerCount == 0) {
            continue;
        }
        ui64 workersInGroup = 0;
        for (const auto& [_, worker] : flowView->State->Workers) {
            if (WorkerBelongsToGroup(worker, workerGroup)) {
                ++workersInGroup;
            }
        }
        if (workersInGroup >= minimumWorkerCount) {
            continue;
        }
        auto& message = messages.emplace_back();
        message.Text = workerGroup.Underlying().empty()
            ? Format("Too few workers (Count: %v, Required: %v)",
            workersInGroup,
            minimumWorkerCount)
            : Format("Too few workers in worker group %Qv (Count: %v, Required: %v)",
            workerGroup.Underlying(),
            workersInGroup,
            minimumWorkerCount);
        message.Level = ELogLevel::Error;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TStream::Register(TRegistrar registrar)
{
    registrar.Parameter("id", &TThis::Id)
        .Default();
    registrar.Parameter("name", &TThis::Name)
        .Default();
    registrar.Parameter("global_name", &TThis::GlobalName)
        .Default();
    registrar.Parameter("status", &TThis::Status)
        .Default(ELogLevel::Info);
    registrar.Parameter("messages", &TThis::Messages)
        .Default();
    // YSON keys kept as "source_id"/"sink_id" for UI backward compatibility (values are graph entity ids).
    registrar.Parameter("source_id", &TThis::SourceGraphEntityId)
        .Default();
    registrar.Parameter("sink_id", &TThis::SinkGraphEntityId)
        .Default();
    registrar.Parameter("messages_per_second", &TThis::MessagesPerSecond)
        .Default();
    registrar.Parameter("bytes_per_second", &TThis::BytesPerSecond)
        .Default();
    registrar.Parameter("inflight_rows", &TThis::InflightRows)
        .Default();
    registrar.Parameter("inflight_bytes", &TThis::InflightBytes)
        .Default();
}

void TStreamLimitStats::Register(TRegistrar registrar)
{
    registrar.Parameter("max", &TThis::Max)
        .Default();
    registrar.Parameter("max_partition_id", &TThis::MaxPartitionId)
        .Default();
    registrar.Parameter("total", &TThis::Total)
        .Default();
}

void TReadDelayEdge::Register(TRegistrar registrar)
{
    registrar.Parameter("blocker_stream_graph_entity_id", &TThis::BlockerStreamGraphEntityId)
        .Default();
    registrar.Parameter("delayed_stream_graph_entity_id", &TThis::DelayedStreamGraphEntityId)
        .Default();
    registrar.Parameter("delay", &TThis::Delay)
        .Default();
}

void TStreamEdge::Register(TRegistrar registrar)
{
    registrar.Parameter("stream_graph_entity_id", &TThis::StreamGraphEntityId)
        .Default();
    registrar.Parameter("completed", &TThis::Completed)
        .Default(false);
    registrar.Parameter("drained", &TThis::Drained)
        .Default(false);
    registrar.Parameter("backpressure_detected", &TThis::BackpressureDetected)
        .Default(false);
    registrar.Parameter("messages", &TThis::Messages)
        .Default();
}

void TPipelineComputationDescription::Register(TRegistrar registrar)
{
    registrar.Parameter("input_streams", &TThis::InputStreams)
        .Default();
    registrar.Parameter("output_streams", &TThis::OutputStreams)
        .Default();
    registrar.Parameter("timer_streams", &TThis::TimerStreams)
        .Default();
    registrar.Parameter("source_streams", &TThis::SourceStreams)
        .Default();
    registrar.Parameter("extended_input_streams", &TThis::ExtendedInputStreams)
        .Default();
    registrar.Parameter("extended_output_streams", &TThis::ExtendedOutputStreams)
        .Default();
    registrar.Parameter("extended_timer_streams", &TThis::ExtendedTimerStreams)
        .Default();
    registrar.Parameter("extended_source_streams", &TThis::ExtendedSourceStreams)
        .Default();
    registrar.Parameter("streams_dependency", &TThis::StreamsDependency)
        .Default();
    registrar.Parameter("input_limit_stats", &TThis::InputLimitStats)
        .Default();
    registrar.Parameter("output_limit_stats", &TThis::OutputLimitStats)
        .Default();
    registrar.Parameter("read_delays", &TThis::ReadDelays)
        .Default();
    registrar.Parameter("total_partition_count", &TThis::TotalPartitionCount)
        .Default(0);
    registrar.Parameter("unstable_partition_count", &TThis::UnstablePartitionCount)
        .Default(0);
    registrar.Parameter("state_path", &TThis::StatePath)
        .Default();
}

void TSource::Register(TRegistrar registrar)
{
    registrar.Parameter("id", &TThis::Id)
        .Default();
    registrar.Parameter("name", &TThis::Name)
        .Default();
    registrar.Parameter("description", &TThis::Description)
        .Default();
    registrar.Parameter("status", &TThis::Status)
        .Default(ELogLevel::Info);
    registrar.Parameter("messages", &TThis::Messages)
        .Default();
    // YSON key kept as "stream_id" for UI backward compatibility (value is a graph entity id).
    registrar.Parameter("stream_id", &TThis::StreamGraphEntityId)
        .Default();
}

void TSink::Register(TRegistrar registrar)
{
    registrar.Parameter("id", &TThis::Id)
        .Default();
    registrar.Parameter("name", &TThis::Name)
        .Default();
    registrar.Parameter("description", &TThis::Description)
        .Default();
    registrar.Parameter("status", &TThis::Status)
        .Default(ELogLevel::Info);
    registrar.Parameter("messages", &TThis::Messages)
        .Default();
    // YSON key kept as "stream_id" for UI backward compatibility (value is a graph entity id).
    registrar.Parameter("stream_id", &TThis::StreamGraphEntityId)
        .Default();
}

void TPipelineDescription::Register(TRegistrar registrar)
{
    registrar.Parameter("status", &TThis::Status)
        .Default();
    registrar.Parameter("messages", &TThis::Messages)
        .Default();

    registrar.Parameter("streams", &TThis::Streams)
        .Default();
    registrar.Parameter("sources", &TThis::Sources)
        .Default();
    registrar.Parameter("sinks", &TThis::Sinks)
        .Default();
    registrar.Parameter("computations", &TThis::Computations)
        .Default();
    registrar.Parameter("read_delay_edges", &TThis::ReadDelayEdges)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void RegisterStreams(const TPipelineSpecPtr& pipelineSpec, const TComputationId& computationId,
    TPipelineDescription& pipeline, TPipelineComputationDescription& computationDescription,
    const TDescribeTraitsContext& describeTraitsContext)
{
    auto computationSpec = pipelineSpec->Computations[computationId];

    // regStream: registers a stream in pipeline.Streams.
    // globalStreamId — global stream ID without prefix (e.g. "heavy_reader/queue")
    // localStreamId  — local name within the computation (e.g. "queue")
    // Sets Id = globalEntityId = StreamPrefix + globalStreamId.
    auto regStream = [&] (const TStreamId& globalStreamId, const TStreamId& localStreamId) -> decltype(auto) {
        auto& streamDescription = pipeline.Streams[globalStreamId];
        streamDescription.Id = MakeStreamGraphId(globalStreamId);
        streamDescription.Name = localStreamId;
        streamDescription.GlobalName = globalStreamId;
        return streamDescription;
    };

    THashMap<TStreamId, TSinkId> sinkStreams;
    for (const auto& [localSinkId, spec] : computationSpec->Sinks) {
        auto globalSinkId = MakeGlobalSinkId(computationId, localSinkId);
        for (const auto& streamId : spec->InputStreamIds) {
            sinkStreams.emplace(streamId, globalSinkId);

            auto& sinkDescription = pipeline.Sinks[globalSinkId];
            sinkDescription.Id = MakeSinkGraphId(globalSinkId);
            sinkDescription.Name = globalSinkId;
            sinkDescription.Description = spec->SinkClassName;
            sinkDescription.StreamGraphEntityId = MakeStreamGraphId(streamId);
            auto& message = sinkDescription.Messages.emplace_back();
            message.Text = "Sink parameters";
            message.Yson = ConvertToYsonString(MakeSinkLinks(ConvertTo<IMapNodePtr>(spec->Parameters), spec->SinkClassName, describeTraitsContext));
        }
    }

    for (const auto& streamId : computationSpec->InputStreamIds) {
        computationDescription.InputStreams.emplace(MakeStreamGraphId(streamId));
        regStream(streamId, streamId);
    }
    for (const auto& streamId : computationSpec->OutputStreamIds) {
        computationDescription.OutputStreams.emplace(MakeStreamGraphId(streamId));
        auto& streamDescription = regStream(streamId, streamId);
        if (sinkStreams.contains(streamId)) {
            streamDescription.SinkGraphEntityId = MakeSinkGraphId(sinkStreams.at(streamId));
        }

        if (auto it = pipelineSpec->Streams.find(streamId); it != pipelineSpec->Streams.end()) {
            auto& message = streamDescription.Messages.emplace_back();
            message.Text = "Stream spec";
            message.Yson = ConvertToYsonString(it->second);
        }
    }
    for (const auto& [localStreamId, spec] : computationSpec->TimerStreams) {
        auto globalStreamId = MakeGlobalStreamId(computationId, localStreamId, computationSpec);
        computationDescription.TimerStreams.emplace(MakeStreamGraphId(globalStreamId));
        regStream(globalStreamId, localStreamId);
    }
    for (const auto& [localStreamId, spec] : computationSpec->SourceStreams) {
        auto globalStreamId = MakeGlobalStreamId(computationId, localStreamId, computationSpec);
        computationDescription.SourceStreams.emplace(MakeStreamGraphId(globalStreamId));

        auto& streamDescription = regStream(globalStreamId, localStreamId);
        streamDescription.SourceGraphEntityId = MakeSourceGraphId(globalStreamId);

        auto& sourceDescription = pipeline.Sources[globalStreamId];
        sourceDescription.Id = MakeSourceGraphId(globalStreamId);
        sourceDescription.Name = localStreamId;
        sourceDescription.Description = spec->SourceClassName;
        sourceDescription.StreamGraphEntityId = MakeStreamGraphId(globalStreamId);
        auto& message = sourceDescription.Messages.emplace_back();
        message.Text = "Source parameters";
        message.Yson = ConvertToYsonString(MakeSourceLinks(ConvertTo<IMapNodePtr>(spec->Parameters), spec->SourceClassName, describeTraitsContext));
    }

    // Populate StreamsDependency: graphEntityId(output) -> set of graphEntityId(input) it depends on.
    // Convert local stream IDs → globalStreamId (via MakeGlobalStreamId) → graphEntityId.
    for (const auto& [localOutputStreamId, localInputStreamIds] : computationSpec->StreamsDependency) {
        auto outputEntityId = MakeStreamGraphId(MakeGlobalStreamId(computationId, localOutputStreamId, computationSpec));
        auto& deps = computationDescription.StreamsDependency[outputEntityId];
        for (const auto& localInputStreamId : localInputStreamIds) {
            deps.emplace(MakeStreamGraphId(MakeGlobalStreamId(computationId, localInputStreamId, computationSpec)));
        }
    }
}

void FillStreamTraverseData(const TFlowViewPtr& flowView, const TPipelineSpecPtr pipelineSpec, TPipelineDescription& pipeline)
{
    auto regStream = [&] (const TStreamId& globalStreamId, const TStreamId& localStreamId) -> decltype(auto) {
        auto& streamDescription = pipeline.Streams[globalStreamId];
        streamDescription.Id = MakeStreamGraphId(globalStreamId);
        streamDescription.Name = localStreamId;
        return streamDescription;
    };

    for (const auto& [computationId, nodeTraverseData] : flowView->State->TraverseData->Computations) {
        const auto& computationSpec = GetOrCrash(pipelineSpec->Computations, computationId);
        for (const auto& [streamId, streamTraverseData] : nodeTraverseData->Streams) {
            if (computationSpec->InputStreamIds.contains(streamId)) {
                // Use stream from corresponding output stream.
                continue;
            }
            const auto& inflightMetrics = streamTraverseData->InflightMetrics;
            if (!inflightMetrics) {
                continue;
            }
            auto& streamDescription = regStream(MakeGlobalStreamId(computationId, streamId, computationSpec), streamId);
            streamDescription.MessagesPerSecond = inflightMetrics->ProcessedCountPerSec.value_or(0);
            streamDescription.BytesPerSecond = inflightMetrics->ProcessedBytesPerSec.value_or(0);
            streamDescription.InflightRows = inflightMetrics->Count;
            streamDescription.InflightBytes = inflightMetrics->ByteSize.value_or(0);
        }
    }
}

void FillStreamStatisticsMessages(const TFlowViewPtr& flowView, TPipelineDescription& pipeline)
{
    const auto& speedStats = flowView->State->SpeedStatistics.StreamSpeed1d;
    const auto& timestampStats = flowView->EphemeralState->MessageTransferingInfo->StreamTimestampStatistics;

    for (auto& [streamId, streamDescription] : pipeline.Streams) {
        const auto* speed = speedStats.FindPtr(streamId);
        const auto* tsStats = timestampStats.FindPtr(streamId);

        if (!speed && !tsStats) {
            continue;
        }

        TStringBuilder markdown;
        markdown.AppendString("#### Stream statistics\n");
        markdown.AppendString("| Metric | Value |\n");
        markdown.AppendString("|--------|-------|\n");

        if (speed) {
            markdown.AppendFormat("| Speed 1d (messages/s) | %.2f |\n", speed->ProcessedMessagesPerSecond);
            markdown.AppendFormat("| Speed 1d (bytes/s) | %.2f |\n", speed->ProcessedBytesPerSecond);
        }
        if (tsStats) {
            markdown.AppendFormat("| Alignment bias (s) | %.3f |\n", tsStats->EstimateAlignmentToEventTimestampBias());
            markdown.AppendFormat("| Message count | %v |\n", tsStats->MessageCount);
        }

        auto& message = streamDescription.Messages.emplace_back();
        message.Text = "Stream statistics";
        message.MarkdownText = markdown.Flush();
    }
}

void FillStreamStateMessage(
    const TFlowViewPtr& flowView,
    const THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>>& intermediateDescriptions,
    std::vector<TMessage>& messages)
{
    const auto& pipelineSpec = flowView->State->ExecutionSpec->PipelineSpec->GetValue();

    // Collect stream states.
    struct TStreamInfo
    {
        TStreamId StreamId;
        TComputationId ComputationId;
        TComputationSpecPtr ComputationSpec;
        EStreamState State;
        std::optional<TPartitionId> ExamplePartition;
    };

    std::vector<TStreamInfo> streamInfos;
    int totalStreams = 0;
    int completedStreams = 0;
    int drainedStreams = 0;

    for (const auto& [computationId, nodeTraverseData] : flowView->State->TraverseData->Computations) {
        const auto& computationSpec = GetOrCrash(pipelineSpec->Computations, computationId);

        for (const auto& [streamId, streamTraverseData] : nodeTraverseData->Streams) {
            // Skip input streams.
            if (computationSpec->InputStreamIds.contains(streamId)) {
                continue;
            }

            totalStreams += 1;
            completedStreams += (streamTraverseData->State == EStreamState::Completed);
            drainedStreams += (streamTraverseData->State == EStreamState::Drained);

            auto info = TStreamInfo{
                .StreamId = streamId,
                .ComputationId = computationId,
                .ComputationSpec = computationSpec,
                .State = streamTraverseData->State,
            };

            // Find example partition with this stream in Active state.
            for (const auto& partitionDesc : GetOrDefault(intermediateDescriptions, computationId)) {
                if (partitionDesc.PartitionJobStatus && partitionDesc.PartitionJobStatus->CurrentJobStatus) {
                    const auto& jobStatus = partitionDesc.PartitionJobStatus->CurrentJobStatus;
                    if (jobStatus->FromPartitionTraverseData && jobStatus->FromPartitionTraverseData->Node) {
                        const auto& nodeStreams = jobStatus->FromPartitionTraverseData->Node->Streams;
                        if (nodeStreams.contains(streamId) && nodeStreams.at(streamId)->State == EStreamState::Active) {
                            info.ExamplePartition = partitionDesc.Partition->PartitionId;
                            break;
                        }
                    }
                }
            }

            streamInfos.push_back(info);
        }
    }

    // Sort streams in deterministic topological order.
    {
        auto hashStreamGraph = BuildStreamGraph(pipelineSpec, /*addReadDelayEdges*/ true);
        auto topologicalIndex = BuildDeterministicTopologicalIndex(
            std::map<TStreamId, std::vector<TStreamId>>(hashStreamGraph.begin(), hashStreamGraph.end()));

        // Drop streams that are no longer in the spec graph - can happen briefly after a rename, before the
        // next leader iteration cleans up traverse data.
        EraseIf(streamInfos, [&] (const TStreamInfo& info) {
            auto globalStreamId = MakeGlobalStreamId(info.ComputationId, info.StreamId, info.ComputationSpec);
            return !topologicalIndex.contains(globalStreamId);
        });

        SortBy(streamInfos, [&] (const TStreamInfo& info) {
            auto globalStreamId = MakeGlobalStreamId(info.ComputationId, info.StreamId, info.ComputationSpec);
            return std::make_tuple(GetOrCrash(topologicalIndex, globalStreamId), globalStreamId.Underlying(), info.ComputationId.Underlying());
        });
    }

    // Build markdown table.
    TStringBuilder markdown;
    markdown.AppendString("#### Streams in topological order\n");
    markdown.AppendString("| Stream ID | Computation ID | State | Example active partition |\n");
    markdown.AppendString("|-----------|----------------|-------|--------------------------|\n");

    for (const auto& info : streamInfos) {
        markdown.AppendFormat("| %v | %v | %v | %v |\n",
            info.StreamId.Underlying(),
            info.ComputationId.Underlying(),
            info.State,
            info.ExamplePartition);
    }

    // Add message.
    auto& message = messages.emplace_back();
    message.Text = Format("Stream state info (TotalStreams: %v, CompletedStreams: %v, DrainedStreams: %v)", totalStreams, completedStreams, drainedStreams);
    message.MarkdownText = markdown.Flush();
}

TPipelineDescription DescribePipeline(const TDescribePipelineArguments& arguments)
{
    auto flowView = arguments.FlowView;

    TPipelineDescription pipeline;
    FillRetryableErrors(arguments.ControllerErrors, pipeline.Messages);

    if (!flowView && arguments.StatusOnly) {
        auto& message = pipeline.Messages.emplace_back();
        message.Text = Format("Flow view is not available (probably uninitialized)");
        message.Level = ELogLevel::Warning;

        return pipeline;
    }

    THROW_ERROR_EXCEPTION_UNLESS(flowView, "Flow view is required when StatusOnly=false");

    const auto& executionSpec = flowView->State->ExecutionSpec;
    pipeline.Status = executionSpec->PipelineState->GetValue();

    const auto& pipelineSpec = executionSpec->PipelineSpec->GetValue();
    const auto& pipelineDynamicSpec = executionSpec->DynamicPipelineSpec->GetValue();

    ValidateSpecs(pipelineSpec, pipelineDynamicSpec, pipeline.Messages);

    {
        auto& message = pipeline.Messages.emplace_back();
        message.Text = Format("Pipeline common information and useful commands");

        TStringBuilder markdownText;

        if (arguments.Authenticator) {
            markdownText.AppendFormat("Authentication method: %v\n\n", arguments.Authenticator->GetAuthDescription());
        }

        auto prettyPath = Format("%v:%v",
            flowView->EphemeralState->PipelinePath.GetCluster().value_or(""),
            flowView->EphemeralState->PipelinePath.GetPath());

        markdownText.AppendFormat("Useful commands:\n");
        markdownText.AppendFormat("* Pipeline rich path: `%v`\n", prettyPath);
        markdownText.AppendFormat("* Draw pipeline debug graph: `ya tool yt-flow-draw-pipeline-graph --input %v`\n", prettyPath);
        markdownText.AppendFormat("* Run job investigation: `ya tool yt-flow-job-investigation --input %v`\n", prettyPath);
        markdownText.AppendFormat("* Run reshard flow tables: `ya run yt/yt/flow/tools/reshard_flow_tables -- --pipeline-path %v`\n", prettyPath);
        markdownText.AppendFormat("* Show public controller logs: `ya tool yt --proxy %v flow show-logs --pipeline-path %v`\n",
            flowView->EphemeralState->PipelinePath.GetCluster(),
            flowView->EphemeralState->PipelinePath.GetPath());
        markdownText.AppendFormat("\n");

        // ControllerBuildType travels in node_info; old controllers don't report it.
        // Omit the line + warning in that case (mirrors how FlowCoreTarget skips its line
        // when empty).
        if (!arguments.ControllerBuildType.empty()) {
            markdownText.AppendFormat("Controller build type: `%v`\n\n", arguments.ControllerBuildType);
        }

        // The flow tables bundle is resolved via the YT connector; omit the line when it could
        // not be resolved (mirrors how ControllerBuildType skips its line when empty).
        if (!arguments.FlowTablesBundle.Bundle.empty()) {
            auto bundleUrl = Format("/%v/tablet_cell_bundles/tablet_cells?activeBundle=%v",
                flowView->EphemeralState->PipelinePath.GetCluster().value_or(""),
                arguments.FlowTablesBundle.Bundle);
            TString clockClusterTagSuffix;
            if (arguments.FlowTablesBundle.ClockClusterTag) {
                clockClusterTagSuffix = Format(" (clock_cluster_tag: %v)", *arguments.FlowTablesBundle.ClockClusterTag);
            }
            markdownText.AppendFormat("Tablet cell bundle: [%v](%v)%v\n\n",
                arguments.FlowTablesBundle.Bundle,
                bundleUrl,
                clockClusterTagSuffix);
        }

        markdownText.AppendFormat("FlowViewAge: %v\n\n", TInstant::Now() - TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying()));

        message.MarkdownText = markdownText.Flush();
    }

    if (IsSlowBuildType(arguments.ControllerBuildType)) {
        auto& message = pipeline.Messages.emplace_back();
        message.Text = Format("Controller is running a slow build (%v)", arguments.ControllerBuildType);
        message.MarkdownText = SlowBuildPipelineMessageText(arguments.ControllerBuildType);
        message.Level = ELogLevel::Warning;
    }

    FillFlowCoreTargetMessage(flowView, arguments.ControllerFlowCoreVersion, pipeline.Messages);

    FillWorkerCountMessages(flowView, pipelineSpec, pipelineDynamicSpec, pipeline.Messages);

    auto intermediateDescriptions = GetComputationPartitionIntermediateDescriptions(flowView);
    auto computationBaseDescriptions = MakeComputationDescriptions(flowView, intermediateDescriptions);
    for (const auto& [computationId, computationDescription] : computationBaseDescriptions) {
        // Copy common parameters.
        auto& pipelineCompDesc = pipeline.Computations[computationId] = ConvertTo<TPipelineComputationDescription>(computationDescription);
        pipelineCompDesc.Id = MakeComputationGraphId(computationId);
    }

    // Build a reverse map: partitionId -> computationId, for unstable partition counting.
    // We need to iterate partitions to count TotalPartitionCount and UnstablePartitionCount.
    THashMap<TPartitionId, TComputationId> partitionToComputation;
    for (const auto& [partitionId, partition] : flowView->State->ExecutionSpec->Layout->Partitions) {
        if (partition->State != EPartitionState::Interrupted && partition->State != EPartitionState::Completed) {
            partitionToComputation[partitionId] = partition->ComputationId;
        }
    }

    // Count TotalPartitionCount per computation.
    for (const auto& [partitionId, computationId] : partitionToComputation) {
        if (auto* compDesc = pipeline.Computations.FindPtr(computationId)) {
            compDesc->TotalPartitionCount++;
        }
    }

    // Count UnstablePartitionCount: partitions with no running job OR job inited < 5 min ago.
    const TDuration unstableThreshold = TDuration::Minutes(5);
    const TInstant now = TInstant::Now();
    for (const auto& [partitionId, computationId] : partitionToComputation) {
        auto* compDesc = pipeline.Computations.FindPtr(computationId);
        if (!compDesc) {
            continue;
        }
        const auto* jobStatus = flowView->Feedback->PartitionJobStatuses.FindPtr(partitionId);
        if (!jobStatus || !(*jobStatus)->CurrentJobStatus) {
            // No running job — unstable.
            compDesc->UnstablePartitionCount++;
        } else {
            const auto& currentJobStatus = (*jobStatus)->CurrentJobStatus;
            if (currentJobStatus->InitedTime && now - *currentJobStatus->InitedTime < unstableThreshold) {
                // Job recently started — warming up, unstable.
                compDesc->UnstablePartitionCount++;
            }
        }
    }

    THashSet<TComputationId> problematicComputations;
    TMessage computationMostCriticalMessage;
    for (const auto& [computationId, computationSpec] : pipelineSpec->Computations)
    {
        auto& computationDescription = GetOrCrash(pipeline.Computations, computationId);

        for (const auto& message : computationDescription.Messages) {
            if (message.Level >= ELogLevel::Warning) {
                problematicComputations.insert(computationId);
            }
            if (message.Level > computationMostCriticalMessage.Level) {
                computationMostCriticalMessage = message;
            }
        }

        RegisterStreams(pipelineSpec, computationId, pipeline, computationDescription, TDescribeTraitsContext{.PipelinePath = flowView->EphemeralState->PipelinePath});
        FillTopHeavyHittersMessages(flowView, intermediateDescriptions, computationId, computationDescription);

        // ReadDelays are filled together with ReadDelayEdges by FillReadDelayEdges.

        // Populate StatePath from computationSpec->Parameters["state"]["path"].
        if (computationSpec->Parameters) {
            if (auto stateNode = computationSpec->Parameters->FindChild("state")) {
                if (stateNode->GetType() == NYTree::ENodeType::Map) {
                    if (auto pathNode = stateNode->AsMap()->FindChild("path")) {
                        if (pathNode->GetType() == NYTree::ENodeType::String) {
                            computationDescription.StatePath = pathNode->AsString()->GetValue();
                        }
                    }
                }
            }
        }
    }

    THashSet<std::string> problematicWorkers;
    TMessage workerMostCriticalMessage;
    for (const auto& [workerAddress, worker] : flowView->State->Workers) {
        std::vector<TMessage> messages;
        FillWorkerErrors(flowView, worker, messages);
        for (const auto& message : messages) {
            if (message.Level >= ELogLevel::Warning) {
                problematicWorkers.insert(workerAddress);
            }
            if (message.Level > workerMostCriticalMessage.Level) {
                workerMostCriticalMessage = message;
            }
        }
    }

    if (!problematicComputations.empty()) {
        auto& message = pipeline.Messages.emplace_back();
        message.Text = Format("Some computations have problems (ProblematicComputations: %v)", problematicComputations.size());
        message.Level = ELogLevel::Warning;
        message.Yson = ConvertToYsonString(problematicComputations);
    }
    if (computationMostCriticalMessage.Level > ELogLevel::Info) {
        auto message = computationMostCriticalMessage;
        message.Text = Format("One of most critical computation problems: %v", message.Text);
        pipeline.Messages.push_back(message);
    }
    if (!problematicWorkers.empty()) {
        auto& message = pipeline.Messages.emplace_back();
        message.Text = Format("Some workers have problems (ProblematicWorkers: %v)", problematicWorkers.size());
        message.Level = ELogLevel::Warning;
        message.Yson = ConvertToYsonString(problematicWorkers);
    }
    if (workerMostCriticalMessage.Level > ELogLevel::Info) {
        auto message = workerMostCriticalMessage;
        message.Text = Format("One of most critical worker problems: %v", message.Text);
        pipeline.Messages.push_back(message);
    }

    FillStreamStateMessage(flowView, intermediateDescriptions, pipeline.Messages);

    FillStreamTraverseData(flowView, pipelineSpec, pipeline);
    FillStreamStatisticsMessages(flowView, pipeline);

    FillGraphLimits(flowView, intermediateDescriptions, pipeline);
    FillReadDelayEdges(flowView, pipeline);
    // Extended-streams output goes into per-computation fields, which are dropped
    // by the StatusOnly filter, so skip it in that mode.
    if (!arguments.StatusOnly) {
        FillExtendedStreams(flowView, pipeline);
    }

    // Embed mermaid computations graph as a message (experimental, behind a dynspec flag;
    // available even in StatusOnly mode, since it is stored in top-level Messages).
    if (pipelineDynamicSpec->EnableMermaidGraphDescribe) {
        auto mermaidDiagram = BuildMermaidComputationsGraph(pipeline);
        auto& message = pipeline.Messages.emplace_back();
        message.Text = "Pipeline graph (mermaid)";
        message.MarkdownText = "```mermaid\n" + std::string(mermaidDiagram.data(), mermaidDiagram.size()) + "\n```";
    }

    if (arguments.StatusOnly) {
        TPipelineDescription filteredPipeline;
        filteredPipeline.Status = pipeline.Status;
        filteredPipeline.Messages = pipeline.Messages;
        return filteredPipeline;
    }

    return pipeline;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
