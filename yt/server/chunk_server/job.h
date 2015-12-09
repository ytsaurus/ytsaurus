#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <yt/ytlib/chunk_client/chunk_replica.h>

#include <yt/ytlib/node_tracker_client/node.pb.h>

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
    /*!
     *  Can't make it TChunkPtrWithIndex since removal jobs may refer to nonexistent chunks.
     */
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TChunkIdWithIndex, ChunkIdWithIndex);
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerServer::TNode*, Node);
    DEFINE_BYREF_RO_PROPERTY(std::vector<Stroka>, TargetAddresses);
    DEFINE_BYREF_RO_PROPERTY(NErasure::TPartIndexList, ErasedIndexes);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
    DEFINE_BYREF_RO_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceUsage);

    //! Current state (as reported by node).
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);
    //! Failure reason (as reported by node).
    DEFINE_BYREF_RW_PROPERTY(TError, Error);

public:
    static TJobPtr CreateReplicate(
        const TJobId& jobId,
        TChunkPtrWithIndex chunkWithIndex,
        NNodeTrackerServer::TNode* node,
        const TNodeList& targets);

    static TJobPtr CreateRemove(
        const TJobId& jobId,
        const NChunkClient::TChunkIdWithIndex& chunkIdWithIndex,
        NNodeTrackerServer::TNode* node);

    static TJobPtr CreateRepair(
        const TJobId& jobId,
        TChunk* chunk,
        NNodeTrackerServer::TNode* node,
        const TNodeList& targets,
        const NErasure::TPartIndexList& erasedIndexes,
        i64 memoryUsage);

    static TJobPtr CreateSeal(
        const TJobId& jobId,
        TChunk* chunk,
        NNodeTrackerServer::TNode* node);

    TJob(
        EJobType type,
        const TJobId& jobId,
        const NChunkClient::TChunkIdWithIndex& chunkIdWithIndex,
        NNodeTrackerServer::TNode* node,
        const TNodeList& targets,
        const NErasure::TPartIndexList& erasedIndexes,
        TInstant startTime,
        const NNodeTrackerClient::NProto::TNodeResources& resourceUsage);

};

DEFINE_REFCOUNTED_TYPE(TJob)

////////////////////////////////////////////////////////////////////////////////

class TJobList
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TJobPtr>, Jobs);

};

DEFINE_REFCOUNTED_TYPE(TJobList)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
