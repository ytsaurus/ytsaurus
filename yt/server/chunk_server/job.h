#pragma once

#include "public.h"

#include <yt/server/chunk_server/chunk_replica.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/core/erasure/public.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/property.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TJobId, JobId);
    DEFINE_BYVAL_RO_PROPERTY(EJobType, Type);
    //! Can't make it TChunkPtrWithIndexes since removal jobs may refer to nonexistent chunks.
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TChunkIdWithIndexes, ChunkIdWithIndexes);
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerServer::TNode*, Node);
    DEFINE_BYREF_RO_PROPERTY(TNodePtrWithIndexesList, TargetReplicas);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
    DEFINE_BYREF_RO_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceUsage);

    //! Current state (as reported by node).
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);
    //! Failure reason (as reported by node).
    DEFINE_BYREF_RW_PROPERTY(TError, Error);

public:
    static TJobPtr CreateReplicate(
        const TJobId& jobId,
        TChunkPtrWithIndexes chunkWithIndexes,
        NNodeTrackerServer::TNode* node,
        const TNodePtrWithIndexesList& targetReplicas);

    static TJobPtr CreateRemove(
        const TJobId& jobId,
        const NChunkClient::TChunkIdWithIndexes& chunkIdWithIndexes,
        NNodeTrackerServer::TNode* node);

    static TJobPtr CreateRepair(
        const TJobId& jobId,
        TChunk* chunk,
        NNodeTrackerServer::TNode* node,
        const TNodePtrWithIndexesList& targetReplicas,
        i64 memoryUsage);

    static TJobPtr CreateSeal(
        const TJobId& jobId,
        TChunkPtrWithIndexes chunkWithIndexes,
        NNodeTrackerServer::TNode* node);

private:
    TJob(
        EJobType type,
        const TJobId& jobId,
        const NChunkClient::TChunkIdWithIndexes& chunkIdWithIndexes,
        NNodeTrackerServer::TNode* node,
        const TNodePtrWithIndexesList& targetReplicas,
        TInstant startTime,
        const NNodeTrackerClient::NProto::TNodeResources& resourceUsage);
    DECLARE_NEW_FRIEND();

};

DEFINE_REFCOUNTED_TYPE(TJob)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
