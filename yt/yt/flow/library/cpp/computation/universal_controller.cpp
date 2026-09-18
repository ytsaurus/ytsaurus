#include "universal_controller.h"

#include "computation_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/sink_controller.h>
#include <yt/yt/flow/library/cpp/common/source_controller.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NFlow {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TKey MakeUniversalPartitionKey(const TStreamId& streamId, const TKey& sourceKey)
{
    NTableClient::TUnversionedOwningRowBuilder builder;
    builder.AddValue(NTableClient::MakeUnversionedStringValue(streamId.Underlying(), 0));
    for (auto value : sourceKey.Underlying()) {
        value.Id += 1;
        builder.AddValue(value);
    }
    return TKey(TKey::TUnderlying(builder.FinishRow()));
}

std::pair<TStreamId, TKey> SplitUniversalPartitionKey(const TKey& partitionKey)
{
    auto streamId = NTableClient::FromUnversionedValue<TStreamId>(partitionKey.Underlying()[0]);
    NTableClient::TUnversionedOwningRowBuilder builder;
    for (int i = 1; i < partitionKey.Underlying().GetCount(); ++i) {
        auto value = partitionKey.Underlying()[i];
        value.Id -= 1;
        builder.AddValue(value);
    }
    return {streamId, TKey(TKey::TUnderlying(builder.FinishRow()))};
}

////////////////////////////////////////////////////////////////////////////////

namespace {

std::string MakeLegacyAvailabilityGroupName(const TAvailabilityGroupOrigin& origin)
{
    return Format("%v-%v", origin.StreamId, origin.Group);
}

} // namespace

THashMap<TStreamId, THashSet<std::string>> MigrateLegacySuppressedAvailabilityGroups(
    const THashSet<std::string>& legacySuppressedAvailabilityGroups,
    const std::vector<TAvailabilityGroupOrigin>& currentOrigins)
{
    THashMap<std::string, TAvailabilityGroupOrigin> originsByKey;
    THashSet<std::string> ambiguousKeys;

    for (const auto& origin : currentOrigins) {
        auto [it, inserted] = originsByKey.emplace(MakeLegacyAvailabilityGroupName(origin), origin);
        if (!inserted && it->second != origin) {
            ambiguousKeys.insert(it->first);
        }
    }

    for (const auto& key : ambiguousKeys) {
        originsByKey.erase(key);
    }

    THashMap<TStreamId, THashSet<std::string>> groupsByStream;
    for (const auto& availabilityGroup : legacySuppressedAvailabilityGroups) {
        if (const auto* origin = originsByKey.FindPtr(availabilityGroup)) {
            groupsByStream[origin->StreamId].insert(origin->Group);
        }
    }
    return groupsByStream;
}

////////////////////////////////////////////////////////////////////////////////

void TUniversalComputationControllerState::Register(TRegistrar registrar)
{
    registrar.Parameter("sources", &TThis::Sources)
        .Default();
    registrar.Parameter("sinks", &TThis::Sinks)
        .Default();
}

void TUniversalComputationControllerPartitioningState::Register(TRegistrar registrar)
{
    registrar.Parameter("sink_channel_counts", &TThis::SinkChannelCounts)
        .DefaultNew();
    registrar.Parameter("suppressed_availability_groups", &TThis::SuppressedAvailabilityGroups)
        .Default();
    registrar.Parameter("suppressed_availability_groups_by_source", &TThis::SuppressedAvailabilityGroupsBySource)
        .Default();
}

void TUniversalComputationController::TExtendedParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TUniversalComputationController::TExtendedDynamicParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

TUniversalComputationController::TUniversalComputationController(
    TComputationControllerContextPtr context,
    TDynamicComputationControllerContextPtr dynamicContext)
    : TComputationControllerBase(std::move(context), std::move(dynamicContext))
    , Sources_(CreateSources(GetContext(), GetSpec(), GetDynamicSpec()))
    , Sinks_(CreateSinks(GetContext(), GetSpec(), GetDynamicSpec()))
{
    if (GetSpec()->InputStreamIds.size() > 0 && GetSpec()->SourceStreams.size() > 0) {
        THROW_ERROR_EXCEPTION("Computation with inputs and sources is not supported");
    }

    if (UsesRangePartitioning()) {
        if (GetSpec()->GroupBySchema->GetColumnCount() < 1) {
            THROW_ERROR_EXCEPTION("Range-partitioned computation should have non-empty group-by schema");
        }
        const auto& firstColumn = GetSpec()->GroupBySchema->Columns()[0];
        if (!GetSpec()->ExperimentalEnableNonUintKey.value_or(TComputationSpec::ExperimentalEnableNonUintKeyDefault)) {
            const auto requiredType = NTableClient::SimpleLogicalType(NTableClient::ESimpleLogicalValueType::Uint64);
            if (*firstColumn.LogicalType() != *requiredType) {
                THROW_ERROR_EXCEPTION("First column in GroupBySchema is expected to have type %Qv but it has %Qv",
                    *requiredType,
                    *firstColumn.LogicalType());
            }
        }
        if (const auto& expression = firstColumn.Expression()) {
            static const THashSet<std::string> forbiddenTokens = {
                "bigb_hash",
                "%",
            };
            for (const auto& token : forbiddenTokens) {
                if (expression->find(token) != std::string::npos) {
                    THROW_ERROR_EXCEPTION("%Qv is not allowed for usage in expression of the first column", token);
                }
            }
        }
    }

    SubscribeReconfigured(BIND([this] (const TDynamicComputationControllerContextPtr& /*dynamicContext*/) {
        for (const auto& [streamId, source] : Sources_) {
            auto dynamicSourceContext = New<TDynamicSourceControllerContext>();
            dynamicSourceContext->DynamicSourceSpec = GetOrDefault(GetDynamicSpec()->SourceStreams, streamId, New<TDynamicSourceSpec>());
            source->Reconfigure(dynamicSourceContext);
        }
        for (const auto& [sinkId, sink] : Sinks_) {
            auto dynamicSinkContext = New<TDynamicSinkControllerContext>();
            dynamicSinkContext->DynamicSinkSpec = GetOrDefault(GetDynamicSpec()->Sinks, sinkId, New<TDynamicSinkSpec>());
            sink->Reconfigure(dynamicSinkContext);
        }
    }));
}

void TUniversalComputationController::Init(IInitContextPtr initContext)
{
    initContext->InitClient(PartitioningState_, "partitioning/v0");
    for (const auto& [streamId, source] : Sources_) {
        auto sourceInitContext = initContext->WithPrefix(Format("sources/%v", streamId));
        // Nest the source state under its identity - the same string that versions its partition
        // keys - so changing the identifying params orphans the previous state (it is then reclaimed
        // by the state manager) and the source starts fresh.
        if (auto identity = source->GetSourceIdentity(); !identity.empty()) {
            sourceInitContext = sourceInitContext->WithPrefix(identity);
        }
        source->Init(std::move(sourceInitContext));
    }
    for (const auto& [sinkId, sink] : Sinks_) {
        sink->Init(initContext->WithPrefix(Format("sinks/%v", sinkId)));
    }
}

void TUniversalComputationController::Sync()
{
    for (const auto& [streamId, source] : Sources_) {
        source->Sync();
    }
    for (const auto& [sinkId, sink] : Sinks_) {
        sink->Sync();
    }
}

void TUniversalComputationController::Commit()
{
    for (const auto& [streamId, source] : Sources_) {
        source->Commit();
    }
    for (const auto& [sinkId, sink] : Sinks_) {
        sink->Commit();
    }
}

void TUniversalComputationController::UpdateWatermarkState(TWatermarkStatePtr watermarkState)
{
    WatermarkState_ = watermarkState;
    for (const auto& sink : GetValues(Sinks_)) {
        sink->UpdateWatermarkState(watermarkState);
    }
}

TWatermarkStatePtr TUniversalComputationController::GetWatermarkState()
{
    return WatermarkState_.Acquire();
}

bool TUniversalComputationController::UsesRangePartitioning() const
{
    const auto& spec = GetSpec();
    const bool hasInputStreams = !spec->InputStreamIds.empty();
    const bool keyVisitorOnly = !spec->KeyVisitorStreams.empty() && spec->SourceStreams.empty();
    return hasInputStreams || keyVisitorOnly;
}

void TUniversalComputationController::NotifySourcesAboutSuppressedGroups(
    const TSuppressedAvailabilityGroupsBySource& groupsByStream)
{
    for (const auto& [streamId, source] : Sources_) {
        if (const auto* groups = groupsByStream.FindPtr(streamId)) {
            source->ProcessSuppressedGroups(*groups);
        } else {
            source->ProcessSuppressedGroups({});
        }
    }
}

IComputationController::TPartitioningTopology TUniversalComputationController::DescribePartitioningTopology()
{
    if (UsesRangePartitioning()) {
        return {.Value = TPartitioningTopology::TRange{}};
    }
    if (GetSpec()->SourceStreams.empty()) {
        THROW_ERROR_EXCEPTION("Computation with no inputs and no sources is not supported");
    }

    TPartitioningTopology::TSource source;
    if (auto expectedKeys = GetSourcePartitionKeys()) {
        source.ExpectedKeys.emplace();
        source.ExpectedKeys->reserve(expectedKeys->size());
        for (const auto& [key, _] : *expectedKeys) {
            source.ExpectedKeys->insert(key);
        }
    }
    return {.Value = std::move(source)};
}

IComputationController::TPartitioningDescription TUniversalComputationController::DescribePartitioning(
    const TPartitioningStatus& status)
{
    if (UsesRangePartitioning()) {
        auto sinkChannelCounts = GetSinkChannelCounts();
        auto lastKnownSinkChannelCounts = PartitioningState_->SinkChannelCounts->GetValue();
        for (const auto& [sinkId, channelCount] : sinkChannelCounts) {
            lastKnownSinkChannelCounts[sinkId] = channelCount;
        }

        std::optional<i64> maxSinkChannelCount;
        for (const auto& [sinkId, _] : Sinks_) {
            if (const auto* channelCount = lastKnownSinkChannelCounts.FindPtr(sinkId)) {
                maxSinkChannelCount = std::max(maxSinkChannelCount.value_or(0), *channelCount);
            }
        }
        PartitioningState_->SinkChannelCounts->TrySetValue(
            std::move(lastKnownSinkChannelCounts),
            GetContext()->VersionProvider);

        const auto dynamicParameters = GetDynamicParameters();
        TPartitioningDescription::TRange range{
            .PartitioningSpec = dynamicParameters,
            .MaxSinkChannelCount = maxSinkChannelCount,
            .SinkTopologyVersion = PartitioningState_->SinkChannelCounts->GetVersion(),
        };
        return {.Value = range};
    }

    if (GetSpec()->SourceStreams.empty()) {
        THROW_ERROR_EXCEPTION("Computation with no inputs and no sources is not supported");
    }

    THashMap<TStreamId, THashMap<TKey, TExtendedSourcePartitionStatusPtr>> statusesByStream;
    for (const auto& [sourceKey, sourceStatus] : status.SourcePartitions) {
        if (!sourceStatus) {
            continue;
        }
        auto [streamId, innerSourceKey] = SplitUniversalPartitionKey(sourceKey);
        statusesByStream[streamId][innerSourceKey] = sourceStatus;
    }
    for (const auto& [streamId, statuses] : statusesByStream) {
        if (const auto* source = Sources_.FindPtr(streamId)) {
            (*source)->ProcessPartitionStatuses(statuses);
        }
    }

    if (const auto& suppressedAvailabilityGroupsBySource = GetSuppressedAvailabilityGroupsBySource()) {
        PartitioningState_->SuppressedAvailabilityGroupsBySource = *suppressedAvailabilityGroupsBySource;
        PartitioningState_->SuppressedAvailabilityGroups.clear();
    }

    TPartitioningDescription::TSource source;
    const bool needsLegacySuppressionMigration =
        !GetSuppressedAvailabilityGroupsBySource() &&
        PartitioningState_->SuppressedAvailabilityGroupsBySource.empty() &&
        !PartitioningState_->SuppressedAvailabilityGroups.empty();
    if (!needsLegacySuppressionMigration) {
        NotifySourcesAboutSuppressedGroups(PartitioningState_->SuppressedAvailabilityGroupsBySource);
        source.ExpectedKeys = GetSourcePartitionKeys();
    } else {
        source.ExpectedKeys = GetSourcePartitionKeys();
        std::vector<TAvailabilityGroupOrigin> availabilityGroupOrigins;
        availabilityGroupOrigins.reserve(
            status.SourcePartitions.size() + (source.ExpectedKeys ? source.ExpectedKeys->size() : 0));
        for (const auto& sourceKey : GetKeys(status.SourcePartitions)) {
            if (auto origin = GetAvailabilityGroupOrigin(sourceKey)) {
                availabilityGroupOrigins.push_back(std::move(*origin));
            }
        }
        if (source.ExpectedKeys) {
            for (const auto& sourceKey : GetKeys(*source.ExpectedKeys)) {
                if (auto origin = GetAvailabilityGroupOrigin(sourceKey)) {
                    availabilityGroupOrigins.push_back(std::move(*origin));
                }
            }
        }
        PartitioningState_->SuppressedAvailabilityGroupsBySource = MigrateLegacySuppressedAvailabilityGroups(
            PartitioningState_->SuppressedAvailabilityGroups,
            availabilityGroupOrigins);
        PartitioningState_->SuppressedAvailabilityGroups.clear();
        NotifySourcesAboutSuppressedGroups(PartitioningState_->SuppressedAvailabilityGroupsBySource);
    }
    const auto& suppressedGroupsByStream = PartitioningState_->SuppressedAvailabilityGroupsBySource;

    if (!source.ExpectedKeys) {
        return {.Value = std::move(source)};
    }

    THashSet<TKey> checkedKeys;
    auto updateAvailability = [&] (const TKey& key) {
        if (!checkedKeys.insert(key).second) {
            return;
        }
        const auto origin = GetAvailabilityGroupOrigin(key);
        const auto* suppressedGroups = origin
            ? suppressedGroupsByStream.FindPtr(origin->StreamId)
            : nullptr;
        if (suppressedGroups && suppressedGroups->contains(origin->Group)) {
            source.UnavailableKeys.insert(key);
        }
    };

    for (const auto& sourcePartition : status.SourcePartitions) {
        updateAvailability(sourcePartition.first);
    }
    for (const auto& key : GetKeys(*source.ExpectedKeys)) {
        updateAvailability(key);
    }

    return {.Value = std::move(source)};
}

std::optional<TAvailabilityGroupOrigin>
TUniversalComputationController::GetAvailabilityGroupOrigin(const TKey& partitionKey) const
{
    auto [streamId, sourceKey] = SplitUniversalPartitionKey(partitionKey);
    const auto* source = Sources_.FindPtr(streamId);
    if (!source) {
        return std::nullopt;
    }
    return TAvailabilityGroupOrigin{
        .StreamId = streamId,
        .Group = (*source)->GetGroup(sourceKey),
    };
}

TNodesByAvailabilityGroupBySource TUniversalComputationController::GetNodesByAvailabilityGroupBySource(
    const THashMap<TPartitionId, TNodeTraverseDataPtr>& traverseData,
    const TFlowViewPtr& flowView)
{
    TNodesByAvailabilityGroupBySource result;
    for (const auto& [partitionId, node] : traverseData) {
        const auto partition = GetOrCrash(flowView->State->ExecutionSpec->Layout->Partitions, partitionId);
        // Stale range partitions may remain interrupting while an input-to-source spec change is reconciled.
        if (!partition->SourceKey) {
            continue;
        }
        // Source stream renamed in spec - the partition's encoded stream id no longer maps to a live
        // source controller. Skip; orphan partitions are eventually pruned by the coordinator.
        auto origin = GetAvailabilityGroupOrigin(*partition->SourceKey);
        if (!origin) {
            continue;
        }
        result[origin->StreamId][origin->Group].push_back(node);
    }
    return result;
}

std::optional<TNodeTraverseDataPtr> TUniversalComputationController::GetFuturePartitionsNodeTraverseData(
    const TFlowViewPtr& flowView)
{
    auto node = New<TNodeTraverseData>();
    node->ReportTime = flowView->State->CurrentTimestamp;

    bool hasNotTrivialTraverse = false;
    for (const auto& [streamId, source] : Sources_) {
        auto traverse = source->GetFutureKeysStreamTraverseData();
        if (traverse.has_value()) {
            auto traverseCopy = CloneYsonStruct(*traverse);
            traverseCopy->Epoch = flowView->State->ExecutionSpec->GetEpoch();

            hasNotTrivialTraverse = true;


            node->Streams[streamId] = std::move(traverseCopy);
        }
    }
    if (!hasNotTrivialTraverse) {
        return std::nullopt;
    }

    YT_VERIFY(GetSpec()->TimerStreams.size() == 0);
    YT_VERIFY(GetSpec()->InputStreamIds.size() == 0);

    for (const auto& outputStreamId : GetSpec()->OutputStreamIds) {
        EStreamState streamState = EStreamState::Completed;
        TSystemTimestamp eventTimestamp = flowView->State->CurrentTimestamp;
        for (const auto& sourceStreamId : GetOrCrash(GetSpec()->StreamsDependency, outputStreamId)) {
            auto sourceTraverseIt = node->Streams.find(sourceStreamId);
            if (sourceTraverseIt == node->Streams.end()) {
                continue;
            }
            streamState = std::min(streamState, sourceTraverseIt->second->State);
            eventTimestamp = std::min(eventTimestamp, sourceTraverseIt->second->EventWatermark);
        }
        auto outputStreamTraverse = New<TStreamTraverseData>();
        outputStreamTraverse->Epoch = flowView->State->ExecutionSpec->GetEpoch();
        outputStreamTraverse->State = streamState;
        outputStreamTraverse->SystemWatermark = flowView->State->CurrentTimestamp;
        outputStreamTraverse->EventWatermark = eventTimestamp;
        node->Streams[outputStreamId] = std::move(outputStreamTraverse);
    }

    return node;
}

THashMap<TStreamId, ISourceControllerPtr> TUniversalComputationController::CreateSources(
    const TComputationControllerContextPtr& context,
    const TComputationSpecPtr& spec,
    const TDynamicComputationSpecPtr& dynamicSpec)
{
    THashMap<TStreamId, ISourceControllerPtr> sources;
    for (const auto& [streamId, sourceSpec] : spec->SourceStreams) {
        auto dynamicSourceSpec = GetOrDefault(dynamicSpec->SourceStreams, streamId, New<TDynamicSourceSpec>());
        auto sourceContext = New<TSourceControllerContext>();
        static_cast<TComputationControllerContextBase&>(*sourceContext) = *context;
        sourceContext->SourceStreamId = streamId;
        sourceContext->SourceSpec = sourceSpec;
        sourceContext->Profiler = sourceContext->Profiler.WithPrefix("/source").WithTag("stream_id", streamId.Underlying());
        sourceContext->StatusProfiler = sourceContext->StatusProfiler->WithPrefix(Format("/sources/%v", streamId));
        sourceContext->Logger = sourceContext->Logger.WithTag("SourceStreamId", streamId);
        auto dynamicSourceContext = New<TDynamicSourceControllerContext>();
        dynamicSourceContext->DynamicSourceSpec = dynamicSourceSpec;
        sources[streamId] = TRegistry::Get()->CreateSourceController(sourceContext, dynamicSourceContext);
    }
    return sources;
}

THashMap<TSinkId, ISinkControllerPtr> TUniversalComputationController::CreateSinks(
    const TComputationControllerContextPtr& context,
    const TComputationSpecPtr& spec,
    const TDynamicComputationSpecPtr& dynamicSpec)
{
    THashMap<TSinkId, ISinkControllerPtr> sinks;
    for (const auto& [sinkId, sinkSpec] : spec->Sinks) {
        auto dynamicSinkSpec = GetOrDefault(dynamicSpec->Sinks, sinkId, New<TDynamicSinkSpec>());
        auto sinkContext = New<TSinkControllerContext>();
        static_cast<TComputationControllerContextBase&>(*sinkContext) = *context;
        sinkContext->SinkId = sinkId;
        sinkContext->SinkSpec = sinkSpec;
        sinkContext->Profiler = sinkContext->Profiler.WithPrefix("/sink").WithTag("sink_id", sinkId.Underlying());
        sinkContext->StatusProfiler = sinkContext->StatusProfiler->WithPrefix(Format("/sinks/%v", sinkId));
        sinkContext->Logger = sinkContext->Logger.WithTag("SinkId", sinkId);
        auto dynamicSinkContext = New<TDynamicSinkControllerContext>();
        dynamicSinkContext->DynamicSinkSpec = dynamicSinkSpec;
        sinks[sinkId] = TRegistry::Get()->CreateSinkController(sinkContext, dynamicSinkContext);
    }
    return sinks;
}

THashMap<TSinkId, i64>
TUniversalComputationController::GetSinkChannelCounts() const
{
    THashMap<TSinkId, i64> result;
    for (const auto& [sinkId, sinkController] : Sinks_) {
        if (auto channelCount = sinkController->GetReceiverChannelCount()) {
            result.emplace(sinkId, *channelCount);
        }
    }
    return result;
}

std::optional<THashMap<TKey, NYTree::IMapNodePtr>> TUniversalComputationController::GetSourcePartitionKeys() const
{
    THashMap<TKey, NYTree::IMapNodePtr> keys;
    for (const auto& [streamId, source] : Sources_) {
        auto sourceKeys = source->ListKeys();
        if (!sourceKeys) {
            YT_TLOG_WARNING("No info about source keys for stream")
                .With("Stream", streamId);
            return std::nullopt;
        }
        for (const auto& [key, dynamicSourcePartitionSpec] : *sourceKeys) {
            keys[MakeUniversalPartitionKey(streamId, key)] = dynamicSourcePartitionSpec;
        }
    }
    return keys;
}

} // namespace NYT::NFlow
