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
    const TNodeResources& resourceUsage)
    : JobId_(jobId)
    , Type_(type)
    , ChunkId_(chunkId)
    , Node_(node)
    , TargetAddresses_(targetAddresses)
    , StartTime_(startTime)
    , ResourceUsage_(resourceUsage)
    , State_(EJobState::Running)
{ }

TJobPtr TJob::CreateForeign(
    const TJobId& jobId,
    const NNodeTrackerClient::NProto::TNodeResources& resourceUsage)
{
    return New<TJob>(
        EJobType::Foreign,
        jobId,
        NullChunkId,
        nullptr,
        std::vector<Stroka>(),
        TInstant::Zero(),
        resourceUsage);
}

TJobPtr TJob::CreateReplicate(
    const TChunkId& chunkId,
    TNode* node,
    const std::vector<Stroka>& targetAddresses)
{
    TNodeResources resourceUsage;
    resourceUsage.set_replication_slots(1);
    return New<TJob>(
        EJobType::ReplicateChunk,
        TJobId::Create(),
        chunkId,
        node,
        targetAddresses,
        TInstant::Now(),
        resourceUsage);
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
    TNodeResources resourceUsage;
    resourceUsage.set_removal_slots(1);
    return New<TJob>(
        EJobType::RemoveChunk,
        TJobId::Create(),
        chunkId,
        node,
        std::vector<Stroka>(),
        TInstant::Now(),
        resourceUsage);
}

TJobPtr TJob::CreateRepair(
    const TChunkId& chunkId,
    NNodeTrackerServer::TNode* node,
    const std::vector<Stroka>& targetAddresses)
{
    TNodeResources resourceUsage;
    resourceUsage.set_repair_slots(1);
    return New<TJob>(
        EJobType::RepairChunk,
        TJobId::Create(),
        chunkId,
        node,
        targetAddresses,
        TInstant::Now(),
        resourceUsage);
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
