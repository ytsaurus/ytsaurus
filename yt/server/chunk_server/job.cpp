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
    const TChunkIdWithIndexes& chunkIdWithIndexes,
    TNode* node,
    const TNodeList& targets,
    const TPartIndexList& erasedIndexes,
    TInstant startTime,
    const TNodeResources& resourceUsage)
    : JobId_(jobId)
    , Type_(type)
    , ChunkIdWithIndexes_(chunkIdWithIndexes)
    , Node_(node)
    , Targets_(targets)
    , ErasedIndexes_(erasedIndexes)
    , StartTime_(startTime)
    , ResourceUsage_(resourceUsage)
    , State_(EJobState::Running)
{ }

TJobPtr TJob::CreateReplicate(
    const TJobId& jobId,
    TChunkPtrWithIndexes chunkWithIndexes,
    TNode* node,
    const TNodeList& targets)
{
    auto* chunk = chunkWithIndexes.GetPtr();
    i64 dataSize = chunk->ChunkInfo().disk_space();

    auto codecId = chunk->GetErasureCodec();
    if (codecId != ECodec::None) {
        auto* codec = NErasure::GetCodec(codecId);
        dataSize /= codec->GetTotalPartCount();
    }

    TNodeResources resourceUsage;
    resourceUsage.set_replication_slots(1);
    resourceUsage.set_replication_data_size(dataSize);

    return New<TJob>(
        EJobType::ReplicateChunk,
        jobId,
        TChunkIdWithIndexes(
            chunk->GetId(),
            chunkWithIndexes.GetReplicaIndex(),
            chunkWithIndexes.GetMediumIndex()),
        node,
        targets,
        TPartIndexList(),
        TInstant::Now(),
        resourceUsage);
}

TJobPtr TJob::CreateRemove(
    const TJobId& jobId,
    const TChunkIdWithIndexes& chunkIdWithIndexes,
    NNodeTrackerServer::TNode* node)
{
    TNodeResources resourceUsage;
    resourceUsage.set_removal_slots(1);

    return New<TJob>(
        EJobType::RemoveChunk,
        jobId,
        chunkIdWithIndexes,
        node,
        TNodeList(),
        TPartIndexList(),
        TInstant::Now(),
        resourceUsage);
}

TJobPtr TJob::CreateRepair(
    const TJobId& jobId,
    TChunk* chunk,
    int mediumIndex,
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
        TChunkIdWithIndexes(chunk->GetId(), 0, mediumIndex),
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
        TChunkIdWithIndexes(chunk->GetId(), 0, DefaultMediumIndex),
        node,
        TNodeList(),
        TPartIndexList(),
        TInstant::Now(),
        resourceUsage);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
