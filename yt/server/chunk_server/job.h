#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/error.h>

#include <ytlib/node_tracker_client/node.pb.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(TJobId, JobId);
    DEFINE_BYVAL_RO_PROPERTY(EJobType, Type);
    // Don't try making it TChunk*.
    // Removal jobs may refer to nonexistent chunks.
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, ChunkId);
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerServer::TNode*, Node);
    DEFINE_BYREF_RO_PROPERTY(std::vector<Stroka>, TargetAddresses);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceLimits);
    
    // Current state (as reported by node).
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);
    // Failure reason (as reported by node).
    DEFINE_BYREF_RW_PROPERTY(TError, Error);

public:
    static TJobPtr CreateReplicate(
        const TChunkId& chunkId,
        NNodeTrackerServer::TNode* node,
        const std::vector<Stroka>& targetAddresses);

    static TJobPtr CreateReplicate(
        const TChunkId& chunkId,
        NNodeTrackerServer::TNode* node,
        const Stroka& targetAddress);

    static TJobPtr CreateRemove(
        const TChunkId& chunkId,
        NNodeTrackerServer::TNode* node);

    static TJobPtr CreateRepair(
        const TChunkId& chunkId,
        NNodeTrackerServer::TNode* node,
        const std::vector<Stroka>& targetAddresses);

    TJob(
        EJobType type,
        const TJobId& jobId,
        const TChunkId& chunkId,
        NNodeTrackerServer::TNode* node,
        const std::vector<Stroka>& targetAddresses,
        TInstant startTime,
        const NNodeTrackerClient::NProto::TNodeResources& resourceLimits);

};

////////////////////////////////////////////////////////////////////////////////

class TJobList
    : public TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, ChunkId);
    DEFINE_BYREF_RO_PROPERTY(yhash_set<TJobPtr>, Jobs);

public:
    explicit TJobList(const TChunkId& chunkId);

    void AddJob(TJobPtr job);
    void RemoveJob(TJobPtr job);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
