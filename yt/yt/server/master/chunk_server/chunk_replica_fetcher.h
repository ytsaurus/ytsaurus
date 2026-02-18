#pragma once

#include "public.h"
#include "private.h"
#include "chunk_replica.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/ytlib/sequoia_client/records/chunk_refresh_queue.record.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct IChunkReplicaFetcher
    : public virtual TRefCounted
{
    virtual bool CanHaveSequoiaReplicas(TChunkId chunkId) const = 0;
    virtual bool CanHaveSequoiaReplicas(TChunkId chunkId, int probability) const = 0;

    virtual TChunkLocationPtrWithReplicaInfoList FilterAliveReplicas(const std::vector<TSequoiaChunkReplica>& replicas) const = 0;

    virtual TFuture<std::vector<NSequoiaClient::NRecords::TLocationReplicas>> GetSequoiaLocationReplicas(
        TNodeId nodeId,
        NNodeTrackerClient::TChunkLocationIndex locationIndex) const = 0;
    virtual TFuture<std::vector<NSequoiaClient::NRecords::TLocationReplicas>> GetSequoiaNodeReplicas(TNodeId nodeId) const = 0;

    using TChunkToLocationPtrWithReplicaInfoList = THashMap<TChunkId, TErrorOr<TChunkLocationPtrWithReplicaInfoList>>;
    // TODO(aleksandra-zh): Let both of these helpers (future and non-future version) live for now, one will take over eventually.
    virtual TErrorOr<TChunkLocationPtrWithReplicaInfoList> GetChunkReplicas(
        const NObjectServer::TEphemeralObjectPtr<TChunk>& chunk,
        bool includeUnapproved = false) const = 0;
    virtual TChunkToLocationPtrWithReplicaInfoList GetChunkReplicas(
        const std::vector<NObjectServer::TEphemeralObjectPtr<TChunk>>& chunks,
        bool includeUnapproved = false) const = 0;

    virtual TFuture<std::vector<TSequoiaChunkReplica>> GetChunkReplicasAsync(
        NObjectServer::TEphemeralObjectPtr<TChunk> chunk,
        bool includeUnapproved = false) const = 0;
    virtual TFuture<THashMap<TChunkId, TErrorOr<std::vector<TSequoiaChunkReplica>>>> GetChunkReplicasAsync(
        std::vector<NObjectServer::TEphemeralObjectPtr<TChunk>> chunks,
        bool includeUnapproved = false) const = 0;

    virtual TFuture<std::vector<TNodeId>> GetLastSeenReplicas(
        const NObjectServer::TEphemeralObjectPtr<TChunk>& chunk) const = 0;

    virtual TFuture<THashMap<TChunkId, std::vector<TSequoiaChunkReplica>>> GetOnlySequoiaChunkReplicas(
        const std::vector<TChunkId>& chunkIds,
        bool includeUnapproved = false,
        bool force = false) const = 0;
    virtual TFuture<std::vector<TSequoiaChunkReplica>> GetApprovedSequoiaChunkReplicas(
        const std::vector<TChunkId>& chunkIds,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp) const = 0;
    virtual TFuture<std::vector<TSequoiaChunkReplica>> GetApprovedSequoiaChunkReplicas(
        const std::vector<TChunkId>& chunkIds,
        NSequoiaClient::ISequoiaTransactionPtr transaction) const = 0;

    virtual TFuture<std::vector<TSequoiaChunkReplica>> GetUnapprovedSequoiaChunkReplicas(
        const std::vector<TChunkId>& chunkIds,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp) const = 0;

    virtual TFuture<std::vector<NSequoiaClient::NRecords::TChunkRefreshQueue>> GetChunksToRefresh(
        int replicatorShard,
        int limit) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReplicaFetcher)

////////////////////////////////////////////////////////////////////////////////

IChunkReplicaFetcherPtr CreateChunkReplicaFetcher(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
