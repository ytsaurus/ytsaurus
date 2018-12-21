#include "job.h"
#include "chunk.h"

#include <yt/server/node_tracker_server/node.h>

#include <yt/core/misc/string.h>

namespace NYT::NChunkServer {

using namespace NErasure;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    EJobType type,
    TJobId jobId,
    const TChunkIdWithIndexes& chunkIdWithIndexes,
    TNode* node,
    const TNodePtrWithIndexesList& targetReplicas,
    TInstant startTime,
    const TNodeResources& resourceUsage,
    bool decommission)
    : JobId_(jobId)
    , Type_(type)
    , Decommission_(decommission)
    , ChunkIdWithIndexes_(chunkIdWithIndexes)
    , Node_(node)
    , TargetReplicas_(targetReplicas)
    , StartTime_(startTime)
    , ResourceUsage_(resourceUsage)
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
        TChunkIdWithIndexes(chunkWithIndexes.GetPtr()->GetId(), chunkWithIndexes.GetReplicaIndex(), chunkWithIndexes.GetMediumIndex()),
        node,
        targetReplicas,
        TInstant::Now(),
        resourceUsage);
}

TJobPtr TJob::CreateRemove(
    TJobId jobId,
    const TChunkIdWithIndexes& chunkIdWithIndexes,
    TNode* node)
{
    TNodeResources resourceUsage;
    resourceUsage.set_removal_slots(1);

    return New<TJob>(
        EJobType::RemoveChunk,
        jobId,
        chunkIdWithIndexes,
        node,
        TNodePtrWithIndexesList(),
        TInstant::Now(),
        resourceUsage);
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
        TChunkIdWithIndexes(chunk->GetId(), GenericChunkReplicaIndex, InvalidMediumIndex),
        node,
        targetReplicas,
        TInstant::Now(),
        resourceUsage,
        decommisssion);
}

TJobPtr TJob::CreateSeal(
    TJobId jobId,
    TChunkPtrWithIndexes chunkWithIndexes,
    TNode* node)
{
    YCHECK(chunkWithIndexes.GetReplicaIndex() == GenericChunkReplicaIndex);

    TNodeResources resourceUsage;
    resourceUsage.set_seal_slots(1);

    return New<TJob>(
        EJobType::SealChunk,
        jobId,
        TChunkIdWithIndexes(chunkWithIndexes.GetPtr()->GetId(), GenericChunkReplicaIndex, chunkWithIndexes.GetMediumIndex()),
        node,
        TNodePtrWithIndexesList(),
        TInstant::Now(),
        resourceUsage);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
