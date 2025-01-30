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
    virtual bool CanHaveSequoiaReplicas(TChunkLocation* location) const = 0;

    virtual bool CanHaveSequoiaReplicas(TChunkId chunkId) const = 0;
    virtual bool CanHaveSequoiaReplicas(TChunkId chunkId, int probability) const = 0;

    virtual bool IsSequoiaChunkReplica(TChunkId chunkId, TChunkLocationUuid locationUuid) const = 0;
    virtual bool IsSequoiaChunkReplica(TChunkId chunkId, TChunkLocation* location) const = 0;

    virtual TFuture<std::vector<NSequoiaClient::NRecords::TLocationReplicas>> GetSequoiaLocationReplicas(
        TNodeId nodeId,
        TChunkLocationUuid locationUuid) const = 0;
    virtual TFuture<std::vector<NSequoiaClient::NRecords::TLocationReplicas>> GetSequoiaNodeReplicas(TNodeId nodeId) const = 0;

    using TChunkToLocationPtrWithReplicaInfoList = THashMap<TChunkId, TErrorOr<TChunkLocationPtrWithReplicaInfoList>>;
    // TODO(aleksandra-zh): Let both of these helpers (future and non-future version) live for now, one will take over eventually.
    virtual TErrorOr<TChunkLocationPtrWithReplicaInfoList> GetChunkReplicas(
        const NObjectServer::TEphemeralObjectPtr<TChunk>& chunk,
        bool includeUnapproved = false) const = 0;
    virtual TChunkToLocationPtrWithReplicaInfoList GetChunkReplicas(
        const std::vector<NObjectServer::TEphemeralObjectPtr<TChunk>>& chunks,
        bool includeUnapproved = false) const = 0;

    // Do not apply anything to these futures using AsyncVia, it will break everything!
    virtual TFuture<TChunkLocationPtrWithReplicaInfoList> GetChunkReplicasAsync(
        NObjectServer::TEphemeralObjectPtr<TChunk> chunk,
        bool includeUnapproved = false) const = 0;
    virtual TFuture<TChunkToLocationPtrWithReplicaInfoList> GetChunkReplicasAsync(
        std::vector<NObjectServer::TEphemeralObjectPtr<TChunk>> chunks,
        bool includeUnapproved = false) const = 0;

    virtual TFuture<std::vector<TNodeId>> GetLastSeenReplicas(
        const NObjectServer::TEphemeralObjectPtr<TChunk>& chunk) const = 0;

    virtual TFuture<std::vector<TSequoiaChunkReplica>> GetSequoiaChunkReplicas(
        const std::vector<TChunkId>& chunkIds) const = 0;
    virtual TFuture<THashMap<TChunkId, TChunkLocationPtrWithReplicaInfoList>> GetOnlySequoiaChunkReplicas(
        const std::vector<TChunkId>& chunkIds,
        bool includeUnapproved = false) const = 0;

    virtual TFuture<std::vector<TSequoiaChunkReplica>> GetUnapprovedSequoiaChunkReplicas(
        const std::vector<TChunkId>& chunkIds) const = 0;

    virtual TFuture<std::vector<NSequoiaClient::NRecords::TChunkRefreshQueue>> GetChunksToRefresh(
        int replicatorShard,
        int limit) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReplicaFetcher)

////////////////////////////////////////////////////////////////////////////////

IChunkReplicaFetcherPtr CreateChunkReplicaFetcher(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
