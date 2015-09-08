#include "stdafx.h"
#include "job.h"

#include <core/misc/string.h>

#include <server/node_tracker_server/node.h>

namespace NYT {
namespace NChunkServer {

using namespace NErasure;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    EJobType type,
    const TJobId& jobId,
    const TChunkIdWithIndex& chunkIdWithIndex,
    TNode* node,
    const TNodeList& targets,
    const TPartIndexList& erasedIndexes,
    TInstant startTime,
    const TNodeResources& resourceUsage)
    : JobId_(jobId)
    , Type_(type)
    , ChunkIdWithIndex_(chunkIdWithIndex)
    , Node_(node)
    , TargetAddresses_(ConvertToStrings(targets, TNodePtrAddressFormatter()))
    , ErasedIndexes_(erasedIndexes)
    , StartTime_(startTime)
    , ResourceUsage_(resourceUsage)
    , State_(EJobState::Running)
{ }

TJobPtr TJob::CreateReplicate(
    const TJobId& jobId,
    const TChunkIdWithIndex& chunkIdWithIndex,
    TNode* node,
    const TNodeList& targets,
    const NNodeTrackerClient::NProto::TNodeResources& resourceUsage)
{
    return New<TJob>(
        EJobType::ReplicateChunk,
        jobId,
        chunkIdWithIndex,
        node,
        targets,
        TPartIndexList(),
        TInstant::Now(),
        resourceUsage);
}

TJobPtr TJob::CreateRemove(
    const TJobId& jobId,
    const TChunkIdWithIndex& chunkIdWithIndex,
    NNodeTrackerServer::TNode* node,
    const NNodeTrackerClient::NProto::TNodeResources& resourceUsage)
{
    return New<TJob>(
        EJobType::RemoveChunk,
        jobId,
        chunkIdWithIndex,
        node,
        TNodeList(),
        TPartIndexList(),
        TInstant::Now(),
        resourceUsage);
}

TJobPtr TJob::CreateRepair(
    const TJobId& jobId,
    const TChunkId& chunkId,
    NNodeTrackerServer::TNode* node,
    const TNodeList& targets,
    const TPartIndexList& erasedIndexes,
    const NNodeTrackerClient::NProto::TNodeResources& resourceUsage)
{
    return New<TJob>(
        EJobType::RepairChunk,
        jobId,
        TChunkIdWithIndex(chunkId, 0),
        node,
        targets,
        erasedIndexes,
        TInstant::Now(),
        resourceUsage);
}

TJobPtr TJob::CreateSeal(
    const TJobId& jobId,
    const TChunkId& chunkId,
    TNode* node,
    const TNodeResources& resourceUsage)
{
    return New<TJob>(
        EJobType::SealChunk,
        jobId,
        TChunkIdWithIndex(chunkId, 0),
        node,
        TNodeList(),
        TPartIndexList(),
        TInstant::Now(),
        resourceUsage);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
