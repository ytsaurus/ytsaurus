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
    const NErasure::TPartIndexList& erasedIndexes,
    TInstant startTime,
    const TNodeResources& resourceUsage)
    : JobId_(jobId)
    , Type_(type)
    , ChunkId_(chunkId)
    , Node_(node)
    , TargetAddresses_(targetAddresses)
    , ErasedIndexes_(erasedIndexes)
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
        static_cast<TNode*>(nullptr),
        std::vector<Stroka>(),
        NErasure::TPartIndexList(),
        TInstant::Zero(),
        resourceUsage);
}

TJobPtr TJob::CreateReplicate(
    const TChunkId& chunkId,
    TNode* node,
    const std::vector<Stroka>& targetAddresses,
    const NNodeTrackerClient::NProto::TNodeResources& resourceUsage)
{
    return New<TJob>(
        EJobType::ReplicateChunk,
        TJobId::Create(),
        chunkId,
        node,
        targetAddresses,
        NErasure::TPartIndexList(),
        TInstant::Now(),
        resourceUsage);
}

TJobPtr TJob::CreateRemove(
    const TChunkId& chunkId,
    NNodeTrackerServer::TNode* node,
    const NNodeTrackerClient::NProto::TNodeResources& resourceUsage)
{
    return New<TJob>(
        EJobType::RemoveChunk,
        TJobId::Create(),
        chunkId,
        node,
        std::vector<Stroka>(),
        NErasure::TPartIndexList(),
        TInstant::Now(),
        resourceUsage);
}

TJobPtr TJob::CreateRepair(
    const TChunkId& chunkId,
    NNodeTrackerServer::TNode* node,
    const std::vector<Stroka>& targetAddresses,
    const NErasure::TPartIndexList& erasedIndexes,
    const NNodeTrackerClient::NProto::TNodeResources& resourceUsage)
{
    return New<TJob>(
        EJobType::RepairChunk,
        TJobId::Create(),
        chunkId,
        node,
        targetAddresses,
        erasedIndexes,
        TInstant::Now(),
        resourceUsage);
}

////////////////////////////////////////////////////////////////////////////////

TJobList::TJobList(const TChunkId& chunkId)
    : ChunkId_(chunkId)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
