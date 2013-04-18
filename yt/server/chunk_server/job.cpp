#include "stdafx.h"
#include "job.h"

namespace NYT {
namespace NChunkServer {

using namespace NNodeTrackerServer;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    EJobType type,
    const TJobId& jobId,
    const TChunkId& chunkId,
    TNode* node,
    const std::vector<Stroka>& targetAddresses,
    TInstant startTime,
    const TNodeResources& resourceLimits)
    : JobId_(jobId)
    , Type_(type)
    , ChunkId_(chunkId)
    , Node_(node)
    , TargetAddresses_(targetAddresses)
    , StartTime_(startTime)
    , ResourceLimits_(resourceLimits)
    , State_(EJobState::Running)
{ }

TJobPtr TJob::CreateReplicate(
    const TChunkId& chunkId,
    TNode* node,
    const std::vector<Stroka>& targetAddresses)
{
    TNodeResources resourceLimits;
    resourceLimits.set_replication_slots(1);
    return New<TJob>(
        EJobType::ReplicateChunk,
        TJobId::Create(),
        chunkId,
        node,
        targetAddresses,
        TInstant::Now(),
        resourceLimits);
}

TJobPtr TJob::CreateReplicate(
    const TChunkId& chunkId,
    TNode* node,
    const Stroka& targetAddress)
{
    return CreateReplicate(
        chunkId,
        node,
        std::vector<Stroka>(1, targetAddress));
}

TJobPtr TJob::CreateRemove(
    const TChunkId& chunkId,
    NNodeTrackerServer::TNode* node)
{
    TNodeResources resourceLimits;
    resourceLimits.set_removal_slots(1);
    return New<TJob>(
        EJobType::RemoveChunk,
        TJobId::Create(),
        chunkId,
        node,
        std::vector<Stroka>(),
        TInstant::Now(),
        resourceLimits);
}

TJobPtr TJob::CreateRepair(
    const TChunkId& chunkId,
    NNodeTrackerServer::TNode* node)
{
    TNodeResources resourceLimits;
    resourceLimits.set_repair_slots(1);
    return New<TJob>(
        EJobType::RepairChunk,
        TJobId::Create(),
        chunkId,
        node,
        std::vector<Stroka>(),
        TInstant::Now(),
        resourceLimits);
}

////////////////////////////////////////////////////////////////////////////////

TJobList::TJobList(const TChunkId& chunkId)
    : ChunkId_(chunkId)
{ }

void TJobList::AddJob(TJobPtr job)
{
    YCHECK(Jobs_.insert(job).second);
}

void TJobList::RemoveJob(TJobPtr job)
{
    YCHECK(Jobs_.erase(job) == 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
