#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/error.h>

#include <ytlib/erasure/public.h>

#include <ytlib/node_tracker_client/node.pb.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(TJobId, JobId);
    DEFINE_BYVAL_RO_PROPERTY(EJobType, Type);
    //! Encoded chunk id for for the job is running.
    /*!
     *  Don't try making it TChunk*.
     *  Removal jobs may refer to nonexistent chunks.
     */
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, ChunkId);
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerServer::TNode*, Node);
    DEFINE_BYREF_RO_PROPERTY(std::vector<Stroka>, TargetAddresses);
    DEFINE_BYREF_RO_PROPERTY(NErasure::TPartIndexList, ErasedIndexes);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceUsage);
    
    // Current state (as reported by node).
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);
    // Failure reason (as reported by node).
    DEFINE_BYREF_RW_PROPERTY(TError, Error);

public:
    static TJobPtr CreateForeign(
        const TJobId& jobId,
        const NNodeTrackerClient::NProto::TNodeResources& resourceUsage);

    static TJobPtr CreateReplicate(
        const TChunkId& chunkId,
        NNodeTrackerServer::TNode* node,
        const std::vector<Stroka>& targetAddresses,
        const NNodeTrackerClient::NProto::TNodeResources& resourceUsage);

    static TJobPtr CreateRemove(
        const TChunkId& chunkId,
        NNodeTrackerServer::TNode* node,
        const NNodeTrackerClient::NProto::TNodeResources& resourceUsage);

    static TJobPtr CreateRepair(
        const TChunkId& chunkId,
        NNodeTrackerServer::TNode* node,
        const std::vector<Stroka>& targetAddresses,
        const NErasure::TPartIndexList& erasedIndexes,
        const NNodeTrackerClient::NProto::TNodeResources& resourceUsage);

    TJob(
        EJobType type,
        const TJobId& jobId,
        const TChunkId& chunkId,
        NNodeTrackerServer::TNode* node,
        const std::vector<Stroka>& targetAddresses,
        const NErasure::TPartIndexList& erasedIndexes,
        TInstant startTime,
        const NNodeTrackerClient::NProto::TNodeResources& resourceUsage);

};

////////////////////////////////////////////////////////////////////////////////

class TJobList
    : public TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, ChunkId);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TJobPtr>, Jobs);

public:
    explicit TJobList(const TChunkId& chunkId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
