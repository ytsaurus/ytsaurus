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
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerServer::TNode*, Node);
    DEFINE_BYREF_RO_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceUsage);

    // NB: This field is used for logging in job tracker, in particular when chunk is already dead,
    // so we store it at the beginning of the job.
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TChunkIdWithIndexes, ChunkIdWithIndexes);

    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
    //! Current state (as reported by node).
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);
    //! Failure reason (as reported by node).
    DEFINE_BYREF_RW_PROPERTY(TError, Error);

public:
    TJob(
        TJobId jobId,
        EJobType type,
        NNodeTrackerServer::TNode* node,
        const NNodeTrackerClient::NProto::TNodeResources& resourceUsage,
        NChunkClient::TChunkIdWithIndexes chunkIdWithIndexes);

    virtual void FillJobSpec(NCellMaster::TBootstrap* bootstrap, NJobTrackerClient::NProto::TJobSpec* jobSpec) const = 0;
};

DEFINE_REFCOUNTED_TYPE(TJob)

////////////////////////////////////////////////////////////////////////////////

class TReplicationJob
    : public TJob
{
public:
    DEFINE_BYREF_RO_PROPERTY(TNodePtrWithIndexesList, TargetReplicas);

public:
    TReplicationJob(
        TJobId jobId,
        NNodeTrackerServer::TNode* node,
        TChunkPtrWithIndexes chunkWithIndexes,
        const TNodePtrWithIndexesList& targetReplicas);

    virtual void FillJobSpec(NCellMaster::TBootstrap* bootstrap, NJobTrackerClient::NProto::TJobSpec* jobSpec) const override;

private:
    static NNodeTrackerClient::NProto::TNodeResources GetResourceUsage(TChunk* chunk);
};

DEFINE_REFCOUNTED_TYPE(TReplicationJob)

////////////////////////////////////////////////////////////////////////////////

class TRemovalJob
    : public TJob
{
public:
    TRemovalJob(
        TJobId jobId,
        NNodeTrackerServer::TNode* node,
        TChunk* chunk,
        const NChunkClient::TChunkIdWithIndexes& chunkIdWithIndexes);

    virtual void FillJobSpec(NCellMaster::TBootstrap* bootstrap, NJobTrackerClient::NProto::TJobSpec* jobSpec) const override;

private:
    TChunk* Chunk_;

    static NNodeTrackerClient::NProto::TNodeResources GetResourceUsage();
};

DEFINE_REFCOUNTED_TYPE(TRemovalJob)

////////////////////////////////////////////////////////////////////////////////

class TRepairJob
    : public TJob
{
public:
    DEFINE_BYREF_RO_PROPERTY(TNodePtrWithIndexesList, TargetReplicas);

public:
    TRepairJob(
        TJobId jobId,
        NNodeTrackerServer::TNode* node,
        i64 jobMemoryUsage,
        TChunk* chunk,
        const TNodePtrWithIndexesList& targetReplicas,
        bool decommission);

    virtual void FillJobSpec(NCellMaster::TBootstrap* bootstrap, NJobTrackerClient::NProto::TJobSpec* jobSpec) const override;

private:
    TChunk* Chunk_;
    bool Decommission_;

    static NNodeTrackerClient::NProto::TNodeResources GetResourceUsage(TChunk* chunk, i64 jobMemoryUsage);
};

DEFINE_REFCOUNTED_TYPE(TRepairJob)

////////////////////////////////////////////////////////////////////////////////

class TSealJob
    : public TJob
{
public:
    TSealJob(
        TJobId jobId,
        NNodeTrackerServer::TNode* node,
        TChunkPtrWithIndexes chunkWithIndexes);

    virtual void FillJobSpec(NCellMaster::TBootstrap* bootstrap, NJobTrackerClient::NProto::TJobSpec* jobSpec) const override;

private:
    TChunkPtrWithIndexes ChunkWithIndexes_;

    static NNodeTrackerClient::NProto::TNodeResources GetResourceUsage();
};

DEFINE_REFCOUNTED_TYPE(TSealJob)

////////////////////////////////////////////////////////////////////////////////

class TMergeJob
    : public TJob
{
public:
    DEFINE_BYREF_RO_PROPERTY(TNodePtrWithIndexesList, TargetReplicas);

public:
    using TChunkVector = SmallVector<TChunk*, 16>;
    TMergeJob(
        TJobId jobId,
        NNodeTrackerServer::TNode* node,
        NChunkClient::TChunkIdWithIndexes chunkIdWithIndexes,
        TChunkVector inputChunks,
        NChunkClient::NProto::TChunkMergerWriterOptions chunkMergerWriterOptions,
        TNodePtrWithIndexesList targetReplicas);

    virtual void FillJobSpec(NCellMaster::TBootstrap* bootstrap, NJobTrackerClient::NProto::TJobSpec* jobSpec) const override;

private:
    TChunkVector InputChunks_;
    NChunkClient::NProto::TChunkMergerWriterOptions ChunkMergerWriterOptions_;

    static NNodeTrackerClient::NProto::TNodeResources GetResourceUsage();
};

DEFINE_REFCOUNTED_TYPE(TMergeJob)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
