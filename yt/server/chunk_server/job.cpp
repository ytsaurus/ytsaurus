#include "job.h"
#include "chunk.h"

#include <yt/server/node_tracker_server/node.h>

#include <yt/core/misc/string.h>

#include <yt/core/erasure/codec.h>

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
    , Targets_(targets)
    , ErasedIndexes_(erasedIndexes)
    , StartTime_(startTime)
    , ResourceUsage_(resourceUsage)
    , State_(EJobState::Running)
{ }

TJobPtr TJob::CreateReplicate(
    const TJobId& jobId,
    TChunkPtrWithIndex chunkWithIndex,
    TNode* node,
    const TNodeList& targets)
{
    auto* chunk = chunkWithIndex.GetPtr();
    i64 dataSize = chunk->ChunkInfo().disk_space();

    TNodeResources resourceUsage;
    resourceUsage.set_replication_slots(1);
    resourceUsage.set_replication_data_size(dataSize);

    return New<TJob>(
        EJobType::ReplicateChunk,
        jobId,
        TChunkIdWithIndex(chunk->GetId(), chunkWithIndex.GetIndex()),
        node,
        targets,
        TPartIndexList(),
        TInstant::Now(),
        resourceUsage);
}

TJobPtr TJob::CreateRemove(
    const TJobId& jobId,
    const TChunkIdWithIndex& chunkIdWithIndex,
    NNodeTrackerServer::TNode* node)
{
    TNodeResources resourceUsage;
    resourceUsage.set_removal_slots(1);

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
    TChunk* chunk,
    NNodeTrackerServer::TNode* node,
    const TNodeList& targets,
    const TPartIndexList& erasedIndexes,
    i64 memoryUsage)
{
    auto codecId = chunk->GetErasureCodec();
    auto* codec = NErasure::GetCodec(codecId);
    i64 dataSize = chunk->ChunkInfo().disk_space() / codec->GetTotalPartCount();

    TNodeResources resourceUsage;
    resourceUsage.set_repair_slots(1);
    resourceUsage.set_memory(memoryUsage);
    resourceUsage.set_repair_data_size(dataSize);

    return New<TJob>(
        EJobType::RepairChunk,
        jobId,
        TChunkIdWithIndex(chunk->GetId(), 0),
        node,
        targets,
        erasedIndexes,
        TInstant::Now(),
        resourceUsage);
}

TJobPtr TJob::CreateSeal(
    const TJobId& jobId,
    TChunk* chunk,
    TNode* node)
{
    TNodeResources resourceUsage;
    resourceUsage.set_seal_slots(1);

    return New<TJob>(
        EJobType::SealChunk,
        jobId,
        TChunkIdWithIndex(chunk->GetId(), 0),
        node,
        TNodeList(),
        TPartIndexList(),
        TInstant::Now(),
        resourceUsage);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
