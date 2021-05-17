#include "job.h"
#include "chunk.h"
#include "helpers.h"
#include "public.h"

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/core/misc/string.h>

namespace NYT::NChunkServer {

using namespace NErasure;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NChunkClient;
using namespace NTableServer;
using namespace NObjectClient;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient::NProto;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    TJobId jobId,
    EJobType type,
    NNodeTrackerServer::TNode* node,
    const TNodeResources& resourceUsage)
    : JobId_(jobId)
    , Type_(type)
    , Node_(node)
    , ResourceUsage_(resourceUsage)
    , StartTime_(TInstant::Now())
    , State_(EJobState::Running)
{ }

////////////////////////////////////////////////////////////////////////////////

TReplicationJob::TReplicationJob(
    TJobId jobId,
    NNodeTrackerServer::TNode* node,
    TChunkPtrWithIndexes chunkWithIndexes,
    const TNodePtrWithIndexesList& targetReplicas)
    : TJob(jobId, EJobType::ReplicateChunk, node, TReplicationJob::GetResourceUsage(chunkWithIndexes.GetPtr()))
    , TargetReplicas_(targetReplicas)
    , ChunkIdWithIndexes_(ToChunkIdWithIndexes(chunkWithIndexes))
{ }

void TReplicationJob::FillJobSpec(NCellMaster::TBootstrap* /*bootstrap*/, TJobSpec* jobSpec) const
{
    auto* jobSpecExt = jobSpec->MutableExtension(TReplicateChunkJobSpecExt::replicate_chunk_job_spec_ext);
    ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(ChunkIdWithIndexes_));
    jobSpecExt->set_source_medium_index(ChunkIdWithIndexes_.MediumIndex);

    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());
    for (auto replica : TargetReplicas_) {
        jobSpecExt->add_target_replicas(ToProto<ui64>(replica));
        builder.Add(replica);
    }
}

NChunkClient::TChunkIdWithIndexes TReplicationJob::GetChunkIdWithIndexes() const
{
    return ChunkIdWithIndexes_;
}

TNodeResources TReplicationJob::GetResourceUsage(TChunk* chunk)
{
    auto dataSize = chunk->GetPartDiskSpace();

    TNodeResources resourceUsage;
    resourceUsage.set_replication_slots(1);
    resourceUsage.set_replication_data_size(dataSize);

    return resourceUsage;
}

////////////////////////////////////////////////////////////////////////////////

TRemovalJob::TRemovalJob(
    TJobId jobId,
    NNodeTrackerServer::TNode* node,
    TChunk* chunk,
    const NChunkClient::TChunkIdWithIndexes& chunkIdWithIndexes)
    : TJob(jobId, EJobType::RemoveChunk, node, TRemovalJob::GetResourceUsage())
    , Chunk_(chunk)
    , ChunkIdWithIndexes_(chunkIdWithIndexes)
{ }

void TRemovalJob::FillJobSpec(NCellMaster::TBootstrap* bootstrap, TJobSpec* jobSpec) const
{
    auto* jobSpecExt = jobSpec->MutableExtension(TRemoveChunkJobSpecExt::remove_chunk_job_spec_ext);
    ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(ChunkIdWithIndexes_));
    jobSpecExt->set_medium_index(ChunkIdWithIndexes_.MediumIndex);
    if (!Chunk_) {
        return;
    }

    bool isErasure = Chunk_->IsErasure();
    for (auto replica : Chunk_->StoredReplicas()) {
        if (replica.GetPtr() == Node_) {
            continue;
        }
        if (isErasure && replica.GetReplicaIndex() != ChunkIdWithIndexes_.ReplicaIndex) {
            continue;
        }
        jobSpecExt->add_replicas(ToProto<ui32>(replica));
    }


    const auto& configManager = bootstrap->GetConfigManager();
    const auto& config = configManager->GetConfig()->ChunkManager;
    auto chunkRemovalJobExpirationDeadline = TInstant::Now() + config->ChunkRemovalJobReplicasExpirationTime;

    jobSpecExt->set_replicas_expiration_deadline(ToProto<ui64>(chunkRemovalJobExpirationDeadline));
}

NChunkClient::TChunkIdWithIndexes TRemovalJob::GetChunkIdWithIndexes() const
{
    return ChunkIdWithIndexes_;
}

TNodeResources TRemovalJob::GetResourceUsage()
{
    TNodeResources resourceUsage;
    resourceUsage.set_removal_slots(1);

    return resourceUsage;
}

////////////////////////////////////////////////////////////////////////////////

TRepairJob::TRepairJob(
    TJobId jobId,
    NNodeTrackerServer::TNode* node,
    i64 jobMemoryUsage,
    TChunk* chunk,
    const TNodePtrWithIndexesList& targetReplicas,
    bool decommission)
    : TJob(
        jobId,
        EJobType::RepairChunk,
        node,
        TRepairJob::GetResourceUsage(chunk, jobMemoryUsage))
    , TargetReplicas_(targetReplicas)
    , Chunk_(chunk)
    , Decommission_(decommission)
{ }

void TRepairJob::FillJobSpec(NCellMaster::TBootstrap* /*bootstrap*/, TJobSpec* jobSpec) const
{
    auto* jobSpecExt = jobSpec->MutableExtension(TRepairChunkJobSpecExt::repair_chunk_job_spec_ext);
    jobSpecExt->set_erasure_codec(static_cast<int>(Chunk_->GetErasureCodec()));
    ToProto(jobSpecExt->mutable_chunk_id(), Chunk_->GetId());
    jobSpecExt->set_decommission(Decommission_);

    if (Chunk_->IsJournal()) {
        YT_VERIFY(Chunk_->IsSealed());
        jobSpecExt->set_row_count(Chunk_->GetPhysicalSealedRowCount());
    }

    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());

    const auto& sourceReplicas = Chunk_->StoredReplicas();
    builder.Add(sourceReplicas);
    ToProto(jobSpecExt->mutable_source_replicas(), sourceReplicas);

    for (auto replica : TargetReplicas_) {
        jobSpecExt->add_target_replicas(ToProto<ui64>(replica));
        builder.Add(replica);
    }
}

NChunkClient::TChunkIdWithIndexes TRepairJob::GetChunkIdWithIndexes() const
{
    return {Chunk_->GetId(), GenericChunkReplicaIndex, GenericMediumIndex};
}

TNodeResources TRepairJob::GetResourceUsage(TChunk* chunk, i64 jobMemoryUsage)
{
    auto dataSize = chunk->GetPartDiskSpace();

    TNodeResources resourceUsage;
    resourceUsage.set_repair_slots(1);
    resourceUsage.set_system_memory(jobMemoryUsage);
    resourceUsage.set_repair_data_size(dataSize);

    return resourceUsage;
}

////////////////////////////////////////////////////////////////////////////////

TSealJob::TSealJob(
    TJobId jobId,
    NNodeTrackerServer::TNode* node,
    TChunkPtrWithIndexes chunkWithIndexes)
    : TJob(jobId, EJobType::SealChunk, node, TSealJob::GetResourceUsage())
    , ChunkWithIndexes_(chunkWithIndexes)
{ }

void TSealJob::FillJobSpec(NCellMaster::TBootstrap* /*bootstrap*/, TJobSpec* jobSpec) const
{
    auto* chunk = ChunkWithIndexes_.GetPtr();
    auto chunkId = GetChunkIdWithIndexes();

    auto* jobSpecExt = jobSpec->MutableExtension(TSealChunkJobSpecExt::seal_chunk_job_spec_ext);
    ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(chunkId));
    jobSpecExt->set_codec_id(ToProto<int>(chunk->GetErasureCodec()));
    jobSpecExt->set_medium_index(chunkId.MediumIndex);
    jobSpecExt->set_row_count(chunk->GetPhysicalSealedRowCount());

    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());
    const auto& replicas = chunk->StoredReplicas();
    builder.Add(replicas);
    ToProto(jobSpecExt->mutable_source_replicas(), replicas);
}

NChunkClient::TChunkIdWithIndexes TSealJob::GetChunkIdWithIndexes() const
{
    return ToChunkIdWithIndexes(ChunkWithIndexes_);
}

TNodeResources TSealJob::GetResourceUsage()
{
    TNodeResources resourceUsage;
    resourceUsage.set_seal_slots(1);

    return resourceUsage;
}

////////////////////////////////////////////////////////////////////////////////

TMergeJob::TMergeJob(
    TJobId jobId,
    NNodeTrackerServer::TNode* node,
    TChunkIdWithIndexes chunkIdWithIndexes,
    TChunkVector inputChunks,
    NChunkClient::NProto::TChunkMergerWriterOptions chunkMergerWriterOptions)
    : TJob(jobId, EJobType::MergeChunks, node, TMergeJob::GetResourceUsage())
    , ChunkIdWithIndexes_(chunkIdWithIndexes)
    , InputChunks_(std::move(inputChunks))
    , ChunkMergerWriterOptions_(chunkMergerWriterOptions)
{ }

void TMergeJob::FillJobSpec(NCellMaster::TBootstrap* bootstrap, TJobSpec* jobSpec) const
{
    auto* jobSpecExt = jobSpec->MutableExtension(TMergeChunksJobSpecExt::merge_chunks_job_spec_ext);

    jobSpecExt->set_cell_tag(bootstrap->GetCellTag());

    ToProto(jobSpecExt->mutable_output_chunk_id(), ChunkIdWithIndexes_.Id);
    jobSpecExt->set_medium_index(ChunkIdWithIndexes_.MediumIndex);
    *jobSpecExt->mutable_chunk_merger_writer_options() = ChunkMergerWriterOptions_;

    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());

    for (auto* chunk : InputChunks_) {
        auto* protoChunk = jobSpecExt->add_input_chunks();
        ToProto(protoChunk->mutable_id(), chunk->GetId());

        const auto& replicas = chunk->StoredReplicas();
        ToProto(protoChunk->mutable_source_replicas(), replicas);
        builder.Add(replicas);

        protoChunk->set_erasure_codec(ToProto<int>(chunk->GetErasureCodec()));
        protoChunk->set_row_count(chunk->MiscExt().row_count());
    }
}

NChunkClient::TChunkIdWithIndexes TMergeJob::GetChunkIdWithIndexes() const
{
    return ChunkIdWithIndexes_;
}


TNodeResources TMergeJob::GetResourceUsage()
{
    TNodeResources resourceUsage;
    resourceUsage.set_merge_slots(1);

    return resourceUsage;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
