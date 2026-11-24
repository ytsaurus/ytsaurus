#include "source.h"

#include "client_factory.h"
#include "helpers.h"
#include "options.h"
#include "payload_splitter.h"
#include "source_engine.h"
#include "state.h"
#include "topic_info.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/library/arcadia_future_interop/interop.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr std::string_view YdbTopicSourceIdentityNamespace = "ydb-topic/v1";

TYdbTopologyOptions MakeSourceTopologyOptions(const TYdbSourceParameters& parameters)
{
    auto options = MakeYdbTopologyOptions(parameters);
    options.IncludePartitionStats = true;
    return options;
}

TYdbPayloadSplitter CreateWholePayloadSplitter(const TYdbSourceParameters& /*parameters*/)
{
    return CreateWholeYdbPayloadSplitter();
}

TYdbPayloadSplitter CreateFramingPayloadSplitter(const TYdbSourceParameters& parameters)
{
    return CreateFramingYdbPayloadSplitter(dynamic_cast<const TYdbFramingSourceParameters&>(parameters).FramingFormat);
}

TYdbPayloadSplitter CreateMultiLinePayloadSplitter(const TYdbSourceParameters& parameters)
{
    return CreateMultiLineYdbPayloadSplitter(dynamic_cast<const TYdbMultiLineSourceParameters&>(parameters).RowDelimiter);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYdbSourceBase::TYdbSourceBase(
    TSourceContextPtr context,
    TDynamicSourceContextPtr dynamicContext,
    TPayloadSplitterFactory createPayloadSplitter)
    : TIntegerOffsetOrderedSourceBase(std::move(context), std::move(dynamicContext))
    , Engine_(New<TYdbSourceEngine>(
        GetContext(),
        MakeYdbSourceOptions(*GetParameters(), createPayloadSplitter(*GetParameters())),
        MakeYdbSourceDynamicOptions(*GetDynamicParameters()),
        GetContext()->GetStaticResource(YdbDriverDefaultResourceId)->As<IYdbClientFactory>(),
        TOrderedSourceBase::Logger,
        GetProfiler(),
        BIND(&TYdbSourceBase::ApplyPartitionInfoUpdate, MakeWeak(this))))
{
    SubscribeReconfigured(BIND([
        weakThis = MakeWeak(this),
        sourceSpec = GetContext()->SourceSpec,
        invoker = GetContext()->SerializedInvoker
    ] (const TDynamicSourceContextPtr& incoming) {
        auto parsed = DynamicPointerCast<TDynamicYdbSourceParameters>(
            TRegistry::Get()->ParseDynamicSourceParameters(sourceSpec, incoming->DynamicSourceSpec));
        auto options = MakeYdbSourceDynamicOptions(*parsed);
        auto partitionSpec = DynamicPointerCast<TYdbSourcePartitionSpec>(
            TRegistry::Get()->ParseDynamicSourcePartitionSpec(sourceSpec, incoming->DynamicPartitionSpec));
        YT_UNUSED_FUTURE(BIND([weakThis, options = std::move(options), partitionSpec = std::move(partitionSpec)] {
            if (auto source = weakThis.Lock()) {
                source->Engine_->Reconfigure(options);
                if (source->AppliedPartitionSpec_) {
                    source->ApplyPartitionSpec(partitionSpec);
                }
            }
        })
                .AsyncVia(invoker)
                .Run());
    }));
}

void TYdbSourceBase::DoInit()
{
    TopologyErrorState_ = CreateAvailabilityErrorState("/topology");
    ApplyPartitionSpec(GetDynamicPartitionSpec());
    Engine_->Init(
        CreateAvailabilityErrorState("/read"),
        CreateAvailabilityErrorState("/read_session_liveness"));
}

void TYdbSourceBase::ApplyPartitionSpec(TYdbSourcePartitionSpecPtr spec)
{
    Engine_->SetReadBlocked(spec->ReadBlocked || spec->Retired);
    if (spec->TopologyUnavailable) {
        TopologyErrorState_->SetError(TError("Cluster of the partition fails to describe the topic"));
    } else if (TopologyErrorState_->GetStatus().IsOK.has_value()) {
        // Before any failure the state has no verdict, which keeps an unavailability inherited from a
        // previous instance until the source speaks.
        TopologyErrorState_->ClearError();
    }
    if (spec->Retired) {
        // Reaching an end below an offset this partition already read would complete it before the outputs
        // of that read are persisted.
        const auto endOffsetExclusive = IntToOffset(std::max(
            spec->FinalOffsetExclusive.value_or(0),
            OffsetToInt(GetKnownMaxOffsetExclusive())));
        UpdatePartitionInfo(TPartitionInfoUpdate{
            .CommittedOffsetExclusive = endOffsetExclusive,
            .MaxOffsetExclusive = endOffsetExclusive,
        });
    } else if (spec->FinalOffsetExclusive) {
        UpdatePartitionInfo(TPartitionInfoUpdate{
            .MaxOffsetExclusive = IntToOffset(*spec->FinalOffsetExclusive),
        });
    }
    AppliedPartitionSpec_ = std::move(spec);
}

bool TYdbSourceBase::IsFinite()
{
    if (AppliedPartitionSpec_->ReadBlocked) {
        return false;
    }
    return AppliedPartitionSpec_->Retired ||
        AppliedPartitionSpec_->FinalOffsetExclusive.has_value() ||
        Engine_->GetEndOffsetExclusive().has_value() ||
        TIntegerOffsetOrderedSourceBase::IsFinite();
}

void TYdbSourceBase::AdjustInflight(const TInflightStreamTraverseDataPtr& inflight)
{
    InputExhausted_ = inflight->Empty;
    const bool completionHeld = inflight->Empty &&
        (!AppliedPartitionSpec_->CompletionAllowed || Engine_->IsCommitBehindPartitionEnd());
    if (completionHeld) {
        inflight->Empty = false;
        inflight->Suspended = inflight->Suspended && IsDraining();
    }
    // A partition waiting for other partitions is not idle: idleness would hide the watermark it holds.
    if (completionHeld || AppliedPartitionSpec_->ReadBlocked) {
        auto& metrics = inflight->InflightMetrics;
        metrics->Count = std::max<i64>(metrics->Count, 1);
        metrics->ReadyCount = 0;
        metrics->ReadyByteSize = 0;
        metrics->IdleDuration.reset();
        metrics->LastIdleTimestamp.reset();
    }
}

IMapNodePtr TYdbSourceBase::GetPartitionStatus()
{
    auto status = New<TYdbSourcePartitionStatus>();
    status->InputExhausted = InputExhausted_.load();
    return ConvertTo<IMapNodePtr>(status);
}

void TYdbSourceBase::DoTerminate()
{
    Engine_->Terminate();
}

TFuture<std::vector<TYdbSourceBase::TRecord>> TYdbSourceBase::DoReadNextBatch(
    const TMessageBatcherSettingsPtr& settings,
    TOffset nextOffset,
    std::optional<TOffset> offsetLimitExclusive)
{
    return Engine_->ReadNextBatch(
        settings,
        OffsetToInt(nextOffset),
        offsetLimitExclusive ? std::optional<i64>(OffsetToInt(*offsetLimitExclusive)) : std::nullopt);
}

TFuture<std::vector<TYdbSourceBase::TRecord>> TYdbSourceBase::DoReadRange(
    const TMessageBatcherSettingsPtr& settings,
    TOffset nextOffset,
    TOffset endExclusive)
{
    return Engine_->ReadRange(settings, OffsetToInt(nextOffset), OffsetToInt(endExclusive));
}

bool TYdbSourceBase::IsReadPaused() const
{
    return AppliedPartitionSpec_->ReadBlocked || AppliedPartitionSpec_->Retired;
}

TFuture<std::optional<TOffset>> TYdbSourceBase::DoFetchTrimmedOffset()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    if (IsReadPaused()) {
        return MakeFuture(std::optional<TOffset>());
    }
    auto trimmedOffsetOrError = WaitFor(Engine_->FetchTrimmedOffset());
    if (!trimmedOffsetOrError.IsOK()) {
        return MakeFuture<std::optional<TOffset>>(TError(trimmedOffsetOrError));
    }
    const auto& trimmedOffset = trimmedOffsetOrError.Value();
    return MakeFuture(!IsReadPaused() && trimmedOffset
            ? std::optional<TOffset>(IntToOffset(*trimmedOffset))
            : std::nullopt);
}

NYson::TYsonString TYdbSourceBase::CaptureReadSettings()
{
    return MakeMalformedMessagePolicyReadSettings(Engine_->GetMalformedMessagePolicy());
}

void TYdbSourceBase::ApplyReadSettings(const NYson::TYsonString& settings)
{
    static const auto RegisteredDefault = New<TDynamicYdbSourceParameters>()->MalformedMessagePolicy;
    Engine_->SetAppliedMalformedMessagePolicy(ParseMalformedMessagePolicyReadSettings(settings, RegisteredDefault));
}

void TYdbSourceBase::DoReportPersistedOffset(TOffset offset)
{
    Engine_->ReportPersistedOffset(OffsetToInt(offset));
}

void TYdbSourceBase::ApplyPartitionInfoUpdate(const TYdbPartitionInfoUpdate& update)
{
    UpdatePartitionInfo(TPartitionInfoUpdate{
        .CommittedOffsetExclusive = update.CommittedOffsetExclusive
            ? std::optional<TOffset>(IntToOffset(*update.CommittedOffsetExclusive))
            : std::nullopt,
        .MaxOffsetExclusive = update.MaxOffsetExclusive
            ? std::optional<TOffset>(IntToOffset(*update.MaxOffsetExclusive))
            : std::nullopt,
        .UpdateInstant = update.UpdateInstant,
        .Repositioned = update.Repositioned,
    });
}

////////////////////////////////////////////////////////////////////////////////

TYdbSource::TYdbSource(
    TSourceContextPtr context,
    TDynamicSourceContextPtr dynamicContext)
    : TYdbSourceBase(std::move(context), std::move(dynamicContext), &CreateWholePayloadSplitter)
{ }

TYdbFramingSource::TYdbFramingSource(
    TSourceContextPtr context,
    TDynamicSourceContextPtr dynamicContext)
    : TYdbSourceBase(std::move(context), std::move(dynamicContext), &CreateFramingPayloadSplitter)
{ }

TYdbMultiLineSource::TYdbMultiLineSource(
    TSourceContextPtr context,
    TDynamicSourceContextPtr dynamicContext)
    : TYdbSourceBase(std::move(context), std::move(dynamicContext), &CreateMultiLinePayloadSplitter)
{ }

////////////////////////////////////////////////////////////////////////////////

TYdbSourceController::TYdbSourceController(
    TSourceControllerContextPtr context,
    TDynamicSourceControllerContextPtr dynamicContext)
    : TSourceControllerBase(std::move(context), std::move(dynamicContext))
    , Info_(New<TYdbTopicInfoController>(
        MakeSourceTopologyOptions(*GetParameters()),
        GetContext()->GetStaticResource(YdbDriverDefaultResourceId)->As<IYdbClientFactory>(),
        GetContext()->Invoker,
        Logger,
        GetContext()->StatusProfiler->WithPrefix("/ydb_info_controller"),
        CreateYdbStateAccess<TYdbTopicInfoControllerState, TYdbTopologySnapshot>("v0")))
    , PartitionGraphState_(CreateYdbStateAccess<TYdbPartitionGraphState, TYdbPartitionGraphSnapshot>("v0"))
    , ResolvePartitionsErrorState_(GetContext()->StatusProfiler->ErrorState("/ydb_info_controller/resolve_partitions"))
{ }

void TYdbSourceController::Init(IInitContextPtr initContext)
{
    PartitionGraphState_.Init(initContext->WithPrefix("ydb_partition_graph"));
    Info_->SubscribeTopicDescribed(BIND(&TYdbSourceController::OnTopicDescribed, MakeWeak(this)));
    Info_->Init(initContext->WithPrefix("ydb_info"));
    // The first description is scheduled on this serialized invoker and may overwrite the counts.
    SeedLegacyPartitions();
}

void TYdbSourceController::SeedLegacyPartitions()
{
    auto snapshot = PartitionGraphState_.Read();
    for (const auto& [endpoint, cluster] : Info_->GetClusters()) {
        auto& graph = snapshot.Clusters[endpoint];
        if (!graph.TopologyKnown && cluster.PartitionCount) {
            SeedLegacyYdbPartitions(&graph, *cluster.PartitionCount);
        }
    }
    PartitionGraphState_.Write(snapshot);
}

void TYdbSourceController::OnTopicDescribed(
    const std::string& endpoint,
    const NYdb::NTopic::TTopicDescription& description)
{
    auto describedGraph = PartitionGraphState_.Read().Clusters[endpoint];
    MergeYdbTopicDescription(&describedGraph, description);

    const auto topicClient = GetOrDefault(Info_->GetTopicClients(), endpoint);
    std::vector<NYdb::NTopic::TPartitionInfo> describedPartitions;
    std::vector<i64> removedPartitionIds;
    std::vector<TError> errors;
    if (topicClient) {
        for (auto partitionId : GetUnresolvedYdbPartitions(describedGraph)) {
            try {
                if (auto partition = DescribeMissingPartition(topicClient, partitionId)) {
                    describedPartitions.push_back(std::move(*partition));
                } else {
                    removedPartitionIds.push_back(partitionId);
                }
            } catch (const std::exception& ex) {
                errors.push_back(TError(ex));
            }
        }
    }

    // Probing yields, so the description is merged into the graph as it is now.
    auto snapshot = PartitionGraphState_.Read();
    auto& graph = snapshot.Clusters[endpoint];
    MergeYdbTopicDescription(&graph, description);
    for (const auto& partition : describedPartitions) {
        MergeYdbPartitionInfo(&graph, partition);
    }
    for (auto partitionId : removedPartitionIds) {
        auto& node = graph.Partitions[partitionId];
        if (!node.Present) {
            YT_TLOG_INFO("Partition was removed from the topic")
                .With("Endpoint", endpoint)
                .With("PartitionId", partitionId);
            node.Removed = true;
        }
    }
    PartitionGraphState_.Write(snapshot);

    if (errors.empty()) {
        ResolvePartitionsErrorState_->ClearError();
    } else {
        ResolvePartitionsErrorState_->SetError(TError("Failed to resolve partitions missing from the topic")
                .With("endpoint", endpoint)
                .With(std::move(errors)));
    }
}

std::optional<NYdb::NTopic::TPartitionInfo> TYdbSourceController::DescribeMissingPartition(
    const std::shared_ptr<NYdb::NTopic::TTopicClient>& topicClient,
    i64 partitionId)
{
    const auto& parameters = GetParameters();
    auto result = WaitFor(FromArcadiaFuture(topicClient->DescribePartition(
        parameters->Topic,
        partitionId,
        NYdb::NTopic::TDescribePartitionSettings()
            .IncludeStats(true)
            .ClientTimeout(parameters->ClientControllerTimeout))))
        .ValueOrThrow();
    if (result.IsSuccess()) {
        YT_TLOG_WARNING("Partition missing from the topic description is still described on its own")
            .With("PartitionId", partitionId);
        return result.GetPartitionDescription().GetPartition();
    }
    const auto answer = ToString(static_cast<const NYdb::TStatus&>(result));
    if (result.GetStatus() == NYdb::EStatus::BAD_REQUEST &&
        answer.find(Format("No partition %v in topic", partitionId)) != std::string::npos)
    {
        return std::nullopt;
    }
    THROW_ERROR_EXCEPTION("Failed to describe partition %v", partitionId)
        .With("answer", answer);
}

void TYdbSourceController::ProcessPartitionStatuses(const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& statuses)
{
    auto snapshot = PartitionGraphState_.Read();
    auto completed = RecordYdbPartitionStatuses(&snapshot, statuses, GetSourceIdentity());
    for (const auto& [endpoint, partitionId] : completed) {
        YT_TLOG_INFO("Partition is completed")
            .With("Endpoint", endpoint)
            .With("PartitionId", partitionId);
    }
    PartitionGraphState_.Write(snapshot);
}

void TYdbSourceController::Sync()
{
    Info_->Sync();
}

void TYdbSourceController::Commit()
{
    Info_->Commit();
}

void TYdbSourceController::ProcessSuppressedGroups(const THashSet<std::string>& groups)
{
    // GetGroup() names a group after its endpoint, so the two sets are the same thing.
    Info_->SetUnavailableEndpoints(THashSet<std::string>(groups.begin(), groups.end()));
}

std::optional<THashMap<TKey, IMapNodePtr>> TYdbSourceController::ListKeys()
{
    ReadAlignmentExcludedKeys_.clear();
    const auto clusters = Info_->GetClusters();
    if (clusters.empty()) {
        return std::nullopt;
    }
    auto snapshot = PartitionGraphState_.Read();
    THashMap<TKey, IMapNodePtr> keys;
    THashSet<TKey> waitingKeys;
    for (const auto& [endpoint, cluster] : clusters) {
        auto& graph = snapshot.Clusters[endpoint];
        if (!graph.TopologyKnown) {
            if (!cluster.PartitionCount) {
                return std::nullopt;
            }
            SeedLegacyYdbPartitions(&graph, *cluster.PartitionCount);
        }
        auto clusterKeys = ListYdbPartitionKeys(
            endpoint,
            &graph,
            GetSourceIdentity(),
            GetParameters()->PartitionFilter,
            GetParameters()->Finite,
            cluster.DescriptionFailed);
        keys.insert(clusterKeys.Keys.begin(), clusterKeys.Keys.end());
        waitingKeys.insert(clusterKeys.WaitingForOtherPartitions.begin(), clusterKeys.WaitingForOtherPartitions.end());
    }
    PartitionGraphState_.Write(snapshot);
    ReadAlignmentExcludedKeys_ = std::move(waitingKeys);
    return keys;
}

bool TYdbSourceController::IsExcludedFromReadAlignment(const TKey& key) const
{
    return ReadAlignmentExcludedKeys_.contains(key);
}

std::optional<TStreamTraverseDataPtr> TYdbSourceController::GetFutureKeysStreamTraverseData()
{
    // The universal controller supports future keys only for computations without timers and inputs.
    const auto& computationSpec = GetContext()->ComputationSpec;
    if (!computationSpec->TimerStreams.empty() || !computationSpec->InputStreamIds.empty()) {
        return std::nullopt;
    }
    const auto clusters = Info_->GetClusters();
    const auto snapshot = PartitionGraphState_.Read();
    const auto& partitionFilter = GetParameters()->PartitionFilter;
    const bool allPartitionsScheduled = !clusters.empty() && AllOf(clusters, [&] (const auto& item) {
        const auto* graph = snapshot.Clusters.FindPtr(item.first);
        return graph && AreAllYdbPartitionsScheduled(*graph, partitionFilter);
    });
    const bool existingDataIsListed = !clusters.empty() && AllOf(clusters, [&] (const auto& item) {
        const auto* graph = snapshot.Clusters.FindPtr(item.first);
        return graph && HasYdbPartitionListingStarted(*graph, partitionFilter);
    });
    const auto now = WaitFor(GetContext()->TimeProvider->GetTimestamp(/*barrier*/ false))
        .ValueOrThrow();
    auto stream = New<TStreamTraverseData>();
    stream->Epoch = -1;
    // A split creates partitions that are listed only once their ancestors complete, so only a finite
    // source whose every known partition has been listed has no keys to come.
    stream->State = GetParameters()->Finite && allPartitionsScheduled ? EStreamState::Completed : EStreamState::Drained;
    stream->SystemWatermark = now;
    stream->EventWatermark = existingDataIsListed ? now : ZeroSystemTimestamp;
    stream->InflightMetrics->Count = 0;
    stream->InflightMetrics->ByteSize = 0;
    return stream;
}

std::string TYdbSourceController::GetGroup(const TKey& key)
{
    return GetYdbTopicKeyGroup(key);
}

std::string TYdbSourceController::GetSourceIdentity() const
{
    const auto& parameters = GetParameters();
    return MakeSourceIdentity({YdbTopicSourceIdentityNamespace, parameters->Database, parameters->Topic});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
