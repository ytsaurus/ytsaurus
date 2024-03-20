#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/chunk_replica.h>

#include <yt/yt/server/lib/chunk_server/proto/job.pb.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/property.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TJobId, JobId);
    DEFINE_BYVAL_RO_PROPERTY(EJobType, Type);
    DEFINE_BYVAL_RO_PROPERTY(TJobEpoch, JobEpoch);
    DEFINE_BYREF_RO_PROPERTY(TString, NodeAddress);
    DEFINE_BYREF_RO_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceUsage);

    // NB: This field is used for logging in job tracker, in particular when chunk is already dead,
    // so we store it at the beginning of the job.
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TChunkIdWithIndexes, ChunkIdWithIndexes);

    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
    //! Current state (as reported by node).
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);
    //! Failure reason (as reported by node).
    DEFINE_BYREF_RW_PROPERTY(TError, Error);

    DEFINE_BYVAL_RW_PROPERTY(THeartbeatSequenceNumber, SequenceNumber, InvalidHeartbeatSequenceNumber);

    DEFINE_BYREF_RW_PROPERTY(NProto::TJobResult, Result);

public:
    TJob(
        TJobId jobId,
        EJobType type,
        TJobEpoch jobEpoch,
        NNodeTrackerServer::TNode* node,
        const NNodeTrackerClient::NProto::TNodeResources& resourceUsage,
        NChunkClient::TChunkIdWithIndexes chunkIdWithIndexes);

    explicit TJob(const TJob& other);

    [[nodiscard]] virtual bool FillJobSpec(
        NCellMaster::TBootstrap* bootstrap,
        NProto::TJobSpec* jobSpec) const = 0;
};

DEFINE_REFCOUNTED_TYPE(TJob)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
