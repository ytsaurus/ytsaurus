#include "state.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TUnparsedYdbPayload::Register(TRegistrar registrar)
{
    registrar.Parameter("data", &TThis::Data)
        .Default();
}

NYson::TYsonString SerializeUnparsedYdbPayload(const std::string& data)
{
    auto unparsed = New<TUnparsedYdbPayload>();
    unparsed->Data = data;
    return NYson::ConvertToYsonString(unparsed);
}

////////////////////////////////////////////////////////////////////////////////

void TYdbClusterInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("partition_count", &TThis::PartitionCount)
        .Default();
    registrar.Parameter("read_only_at", &TThis::ReadOnlyAt)
        .Default();
}

void TYdbTopicInfoControllerState::Register(TRegistrar registrar)
{
    registrar.Parameter("clusters", &TThis::Clusters)
        .Default();
    registrar.Parameter("cached_partition_counts", &TThis::CachedPartitionCounts)
        .Default();
}

TYdbTopologySnapshot ReadYdbStateSnapshot(const TYdbTopicInfoControllerState& state)
{
    TYdbTopologySnapshot snapshot;
    for (const auto& [endpoint, cluster] : state.Clusters) {
        snapshot.Clusters.emplace(endpoint, TYdbClusterSnapshot{
                .PartitionCount = cluster->PartitionCount,
                .ReadOnlyAt = cluster->ReadOnlyAt,
                                            });
    }
    snapshot.CachedPartitionCounts = state.CachedPartitionCounts;
    return snapshot;
}

void WriteYdbStateSnapshot(const TYdbTopologySnapshot& snapshot, TYdbTopicInfoControllerState* state)
{
    state->Clusters.clear();
    for (const auto& [endpoint, clusterSnapshot] : snapshot.Clusters) {
        auto cluster = New<TYdbClusterInfo>();
        cluster->PartitionCount = clusterSnapshot.PartitionCount;
        cluster->ReadOnlyAt = clusterSnapshot.ReadOnlyAt;
        state->Clusters.emplace(endpoint, std::move(cluster));
    }
    state->CachedPartitionCounts = snapshot.CachedPartitionCounts;
}

////////////////////////////////////////////////////////////////////////////////

void TYdbPartitionNodeState::Register(TRegistrar registrar)
{
    registrar.Parameter("id", &TThis::Id);
    registrar.Parameter("parent_ids", &TThis::ParentIds)
        .Default();
    registrar.Parameter("active", &TThis::Active)
        .Default(true);
    registrar.Parameter("end_offset_exclusive", &TThis::EndOffsetExclusive)
        .Default();
    registrar.Parameter("described", &TThis::Described)
        .Default(false);
    registrar.Parameter("present", &TThis::Present)
        .Default(false);
    registrar.Parameter("removed", &TThis::Removed)
        .Default(false);
    registrar.Parameter("scheduled", &TThis::Scheduled)
        .Default(false);
    registrar.Parameter("input_exhausted", &TThis::InputExhausted)
        .Default(false);
    registrar.Parameter("completed", &TThis::Completed)
        .Default(false);
}

void TYdbPartitionGraphClusterState::Register(TRegistrar registrar)
{
    registrar.Parameter("topology_known", &TThis::TopologyKnown)
        .Default(false);
    registrar.Parameter("partitions", &TThis::Partitions)
        .Default();
}

void TYdbPartitionGraphState::Register(TRegistrar registrar)
{
    registrar.Parameter("clusters", &TThis::Clusters)
        .Default();
}

TYdbPartitionGraphSnapshot ReadYdbStateSnapshot(const TYdbPartitionGraphState& state)
{
    TYdbPartitionGraphSnapshot snapshot;
    for (const auto& [endpoint, clusterState] : state.Clusters) {
        auto& graph = snapshot.Clusters[endpoint];
        graph.TopologyKnown = clusterState->TopologyKnown;
        for (const auto& nodeState : clusterState->Partitions) {
            auto& node = graph.Partitions[nodeState->Id];
            node.ParentIds = nodeState->ParentIds;
            node.Active = nodeState->Active;
            node.EndOffsetExclusive = nodeState->EndOffsetExclusive;
            node.Described = nodeState->Described;
            node.Present = nodeState->Present;
            node.Removed = nodeState->Removed;
            node.Scheduled = nodeState->Scheduled;
            node.InputExhausted = nodeState->InputExhausted;
            node.Completed = nodeState->Completed;
        }
    }
    return snapshot;
}

void WriteYdbStateSnapshot(const TYdbPartitionGraphSnapshot& snapshot, TYdbPartitionGraphState* state)
{
    state->Clusters.clear();
    for (const auto& [endpoint, graph] : snapshot.Clusters) {
        auto clusterState = New<TYdbPartitionGraphClusterState>();
        clusterState->TopologyKnown = graph.TopologyKnown;
        for (const auto& [id, node] : graph.Partitions) {
            auto nodeState = New<TYdbPartitionNodeState>();
            nodeState->Id = id;
            nodeState->ParentIds = node.ParentIds;
            nodeState->Active = node.Active;
            nodeState->EndOffsetExclusive = node.EndOffsetExclusive;
            nodeState->Described = node.Described;
            nodeState->Present = node.Present;
            nodeState->Removed = node.Removed;
            nodeState->Scheduled = node.Scheduled;
            nodeState->InputExhausted = node.InputExhausted;
            nodeState->Completed = node.Completed;
            clusterState->Partitions.push_back(std::move(nodeState));
        }
        state->Clusters.emplace(endpoint, std::move(clusterState));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TYdbHeartbeatState::Register(TRegistrar registrar)
{
    registrar.Parameter("producer_id_base", &TThis::ProducerIdBase)
        .Default();
}

TYdbHeartbeatSnapshot ReadYdbStateSnapshot(const TYdbHeartbeatState& state)
{
    return {.ProducerIdBase = state.ProducerIdBase};
}

void WriteYdbStateSnapshot(const TYdbHeartbeatSnapshot& snapshot, TYdbHeartbeatState* state)
{
    state->ProducerIdBase = snapshot.ProducerIdBase;
}

////////////////////////////////////////////////////////////////////////////////

void TYdbTwoPhaseCommitSinkState::Register(TRegistrar registrar)
{
    registrar.Parameter("producer_id", &TThis::ProducerId)
        .Default();
    registrar.Parameter("max_persisted_message_id", &TThis::MaxPersistedMessageId)
        .Default();
    registrar.Parameter("max_persisted_seq_no", &TThis::MaxPersistedSeqNo)
        .Default(0);
}

TYdbSinkCheckpoint ReadYdbStateSnapshot(const TYdbTwoPhaseCommitSinkState& state)
{
    return {
        .ProducerId = state.ProducerId,
        .MaxPersistedMessageId = state.MaxPersistedMessageId,
        .MaxPersistedSeqNo = state.MaxPersistedSeqNo,
    };
}

void WriteYdbStateSnapshot(const TYdbSinkCheckpoint& snapshot, TYdbTwoPhaseCommitSinkState* state)
{
    state->ProducerId = snapshot.ProducerId;
    state->MaxPersistedMessageId = snapshot.MaxPersistedMessageId;
    state->MaxPersistedSeqNo = snapshot.MaxPersistedSeqNo;
}

////////////////////////////////////////////////////////////////////////////////

void TYdbPendingPublicationState::Register(TRegistrar registrar)
{
    registrar.Parameter("int_publication_id", &TThis::IntPublicationId)
        .Optional();
    registrar.Parameter("endpoint", &TThis::Endpoint)
        .Optional();
    registrar.Parameter("database", &TThis::Database)
        .Optional();
}

void TYdbDeferredPublicationState::Register(TRegistrar registrar)
{
    registrar.Parameter("pending_publications", &TThis::PendingPublications)
        .Default();
}

TYdbPublicationSnapshot ReadYdbStateSnapshot(const TYdbDeferredPublicationState& state)
{
    TYdbPublicationSnapshot snapshot;
    for (const auto& pending : state.PendingPublications) {
        snapshot.PendingPublications.push_back({
            .IntPublicationId = pending->IntPublicationId,
            .Endpoint = pending->Endpoint,
            .Database = pending->Database,
        });
    }
    return snapshot;
}

void WriteYdbStateSnapshot(const TYdbPublicationSnapshot& snapshot, TYdbDeferredPublicationState* state)
{
    state->PendingPublications.clear();
    for (const auto& pendingSnapshot : snapshot.PendingPublications) {
        auto pending = New<TYdbPendingPublicationState>();
        pending->IntPublicationId = pendingSnapshot.IntPublicationId;
        pending->Endpoint = pendingSnapshot.Endpoint;
        pending->Database = pendingSnapshot.Database;
        state->PendingPublications.push_back(std::move(pending));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
