#include "job.h"
#include "chunk.h"
#include "public.h"

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/core/misc/string.h>

namespace NYT::NChunkServer {

using namespace NErasure;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NChunkClient;
using namespace NTableServer;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    EJobType type,
    TJobId jobId,
    TChunk* chunk,
    const TChunkIdWithIndexes& chunkIdWithIndexes,
    TNode* node,
    const TNodePtrWithIndexesList& targetReplicas,
    TInstant startTime,
    const TNodeResources& resourceUsage,
    TChunkVector chunks,
    TChunkMergerWriterOptions chunkMergerWriterOptions,
    bool decommission)
    : JobId_(jobId)
    , Type_(type)
    , Decommission_(decommission)
    , Chunk_(chunk)
    , ChunkIdWithIndexes_(chunkIdWithIndexes)
    , Node_(node)
    , TargetReplicas_(targetReplicas)
    , StartTime_(startTime)
    , ResourceUsage_(resourceUsage)
    , Chunks_(std::move(chunks))
    , ChunkMergerWriterOptions_(std::move(chunkMergerWriterOptions))
    , State_(EJobState::Running)
{ }

TJobPtr TJob::CreateReplicate(
    TJobId jobId,
    TChunkPtrWithIndexes chunkWithIndexes,
    TNode* node,
    const TNodePtrWithIndexesList& targetReplicas)
{
    auto* chunk = chunkWithIndexes.GetPtr();
    auto dataSize = chunk->GetPartDiskSpace();

    TNodeResources resourceUsage;
    resourceUsage.set_replication_slots(1);
    resourceUsage.set_replication_data_size(dataSize);

    return New<TJob>(
        EJobType::ReplicateChunk,
        jobId,
        chunk,
        TChunkIdWithIndexes(chunk->GetId(), chunkWithIndexes.GetReplicaIndex(), chunkWithIndexes.GetMediumIndex()),
        node,
        targetReplicas,
        TInstant::Now(),
        resourceUsage,
        TChunkVector(),
        TChunkMergerWriterOptions());
}

TJobPtr TJob::CreateRemove(
    TJobId jobId,
    TChunk* chunk,
    const TChunkIdWithIndexes& chunkIdWithIndexes,
    TNode* node)
{
    TNodeResources resourceUsage;
    resourceUsage.set_removal_slots(1);

    return New<TJob>(
        EJobType::RemoveChunk,
        jobId,
        chunk,
        chunkIdWithIndexes,
        node,
        TNodePtrWithIndexesList(),
        TInstant::Now(),
        resourceUsage,
        TChunkVector(),
        TChunkMergerWriterOptions());
}

TJobPtr TJob::CreateRepair(
    TJobId jobId,
    TChunk* chunk,
    TNode* node,
    const TNodePtrWithIndexesList& targetReplicas,
    i64 memoryUsage,
    bool decommisssion)
{
    auto dataSize = chunk->GetPartDiskSpace();

    TNodeResources resourceUsage;
    resourceUsage.set_repair_slots(1);
    resourceUsage.set_system_memory(memoryUsage);
    resourceUsage.set_repair_data_size(dataSize);

    return New<TJob>(
        EJobType::RepairChunk,
        jobId,
        chunk,
        TChunkIdWithIndexes(chunk->GetId(), GenericChunkReplicaIndex, GenericMediumIndex),
        node,
        targetReplicas,
        TInstant::Now(),
        resourceUsage,
        TChunkVector(),
        TChunkMergerWriterOptions(),
        decommisssion);
}

TJobPtr TJob::CreateSeal(
    TJobId jobId,
    TChunkPtrWithIndexes chunkWithIndexes,
    TNode* node)
{
    auto* chunk = chunkWithIndexes.GetPtr();

    TNodeResources resourceUsage;
    resourceUsage.set_seal_slots(1);

    return New<TJob>(
        EJobType::SealChunk,
        jobId,
        chunk,
        TChunkIdWithIndexes(chunk->GetId(), chunkWithIndexes.GetReplicaIndex(), chunkWithIndexes.GetMediumIndex()),
        node,
        TNodePtrWithIndexesList(),
        TInstant::Now(),
        resourceUsage,
        TChunkVector(),
        TChunkMergerWriterOptions());
}


TJobPtr TJob::CreateMerge(
    TJobId jobId,
    TChunkId chunkId,
    int mediumIndex,
    TChunkVector chunks,
    NNodeTrackerServer::TNode* node,
    TChunkMergerWriterOptions chunkMergeTableOptions)
{
    TNodeResources resourceUsage;
    resourceUsage.set_merge_slots(1);

    return New<TJob>(
        EJobType::MergeChunks,
        jobId,
        nullptr,
        TChunkIdWithIndexes(chunkId, GenericChunkReplicaIndex, mediumIndex),
        node,
        TNodePtrWithIndexesList(),
        TInstant::Now(),
        resourceUsage,
        std::move(chunks),
        std::move(chunkMergeTableOptions));
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
