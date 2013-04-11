#include "stdafx.h"
#include "job.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    EJobType type,
    const TJobId& jobId,
    const TChunkId& chunkId,
    NNodeTrackerServer::TNode* node,
    const std::vector<Stroka>& targetAddresses,
    TInstant startTime)
    : JobId_(jobId)
    , Type_(type)
    , ChunkId_(chunkId)
    , Node_(node)
    , TargetAddresses_(targetAddresses)
    , StartTime_(startTime)
    , State_(EJobState::Running)
{ }

TJobPtr TJob::CreateReplicate(
    const TChunkId& chunkId,
    NNodeTrackerServer::TNode* node,
    const std::vector<Stroka>& targetAddresses)
{
    return New<TJob>(
        EJobType::Replicate,
        TJobId::Create(),
        chunkId,
        node,
        targetAddresses,
        TInstant::Now());
}

TJobPtr TJob::CreateReplicate(
    const TChunkId& chunkId,
    NNodeTrackerServer::TNode* node,
    const Stroka& targetAddress)
{
    return New<TJob>(
        EJobType::Replicate,
        TJobId::Create(),
        chunkId,
        node,
        std::vector<Stroka>(1, targetAddress),
        TInstant::Now());
}

TJobPtr TJob::CreateRemove(
    const TChunkId& chunkId,
    NNodeTrackerServer::TNode* node)
{
    return New<TJob>(
        EJobType::Remove,
        TJobId::Create(),
        chunkId,
        node,
        std::vector<Stroka>(),
        TInstant::Now());
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
