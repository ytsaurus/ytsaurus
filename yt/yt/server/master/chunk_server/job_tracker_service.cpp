#include "job_tracker_service.h"
#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "job.h"
#include "config.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/master/table_server/shared_table_schema.h>

#include <yt/yt/server/lib/chunk_server/proto/job.pb.h>

#include <yt/yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/string.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NChunkServer {

using namespace NHydra;
using namespace NJobTrackerClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerServer::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NChunkClient::NProto;
using namespace NCellMaster;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TJobTrackerService
    : public NCellMaster::TMasterHydraServiceBase
{
public:
    explicit TJobTrackerService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TJobTrackerServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::JobTrackerService,
            ChunkServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NJobTrackerClient::NProto, Heartbeat)
    {
        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        auto nodeId = request->node_id();

        const auto& resourceLimits = request->resource_limits();
        auto resourceUsage = request->resource_usage();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        context->SetRequestInfo("NodeId: %v, Address: %v, ResourceUsage: %v",
            nodeId,
            node->GetDefaultAddress(),
            FormatResourceUsage(resourceUsage, resourceLimits));

        if (!node->ReportedDataNodeHeartbeat()) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Cannot process a job heartbeat unless data node heartbeat is reported");
        }

        const auto& configManager = Bootstrap_->GetConfigManager();
        const auto& config = configManager->GetConfig()->ChunkManager;
        auto chunkRemovalJobExpirationDeadline = TInstant::Now() + config->ChunkRemovalJobReplicasExpirationTime;

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        std::vector<TJobPtr> currentJobs;
        for (const auto& jobStatus : request->jobs()) {
            auto jobId = FromProto<TJobId>(jobStatus.job_id());
            auto state = EJobState(jobStatus.state());
            auto job = node->FindJob(jobId);
            if (job) {
                job->SetState(state);
                if (state == EJobState::Completed || state == EJobState::Failed) {
                    job->Error() = FromProto<TError>(jobStatus.result().error());
                }
                currentJobs.push_back(job);
            } else {
                switch (state) {
                    case EJobState::Completed:
                        YT_LOG_DEBUG("Unknown job has completed, removal scheduled (JobId: %v)",
                            jobId);
                        ToProto(response->add_jobs_to_remove(), {jobId});
                        break;

                    case EJobState::Failed:
                        YT_LOG_DEBUG("Unknown job has failed, removal scheduled (JobId: %v)",
                            jobId);
                        ToProto(response->add_jobs_to_remove(), {jobId});
                        break;

                    case EJobState::Aborted:
                        YT_LOG_DEBUG("Job aborted, removal scheduled (JobId: %v)",
                            jobId);
                        ToProto(response->add_jobs_to_remove(), {jobId});
                        break;

                    case EJobState::Running:
                        YT_LOG_DEBUG("Unknown job is running, abort scheduled (JobId: %v)",
                            jobId);
                        ToProto(response->add_jobs_to_abort(), jobId);
                        break;

                    case EJobState::Waiting:
                        YT_LOG_DEBUG("Unknown job is waiting, abort scheduled (JobId: %v)",
                            jobId);
                        ToProto(response->add_jobs_to_abort(), jobId);
                        break;

                    default:
                        YT_ABORT();
                }
            }
        }

        std::vector<TJobPtr> jobsToStart;
        std::vector<TJobPtr> jobsToAbort;
        std::vector<TJobPtr> jobsToRemove;
        chunkManager->ScheduleJobs(
            node,
            resourceUsage,
            resourceLimits,
            currentJobs,
            &jobsToStart,
            &jobsToAbort,
            &jobsToRemove);

        for (const auto& job : jobsToStart) {
            resourceUsage += job->ResourceUsage();
        }

        for (const auto& job : jobsToStart) {
            auto chunkIdWithIndexes = job->GetChunkIdWithIndexes();
            auto chunkId = chunkIdWithIndexes.Id;

            auto* jobInfo = response->add_jobs_to_start();
            ToProto(jobInfo->mutable_job_id(), job->GetJobId());
            *jobInfo->mutable_resource_limits() = job->ResourceUsage();

            TJobSpec jobSpec;
            jobSpec.set_type(static_cast<int>(job->GetType()));

            switch (job->GetType()) {
                case EJobType::ReplicateChunk: {
                    auto* jobSpecExt = jobSpec.MutableExtension(TReplicateChunkJobSpecExt::replicate_chunk_job_spec_ext);
                    ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(chunkIdWithIndexes));
                    jobSpecExt->set_source_medium_index(chunkIdWithIndexes.MediumIndex);

                    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());
                    for (auto replica : job->TargetReplicas()) {
                        jobSpecExt->add_target_replicas(ToProto<ui64>(replica));
                        builder.Add(replica);
                    }
                    break;
                }

                case EJobType::RemoveChunk: {
                    auto* jobSpecExt = jobSpec.MutableExtension(TRemoveChunkJobSpecExt::remove_chunk_job_spec_ext);
                    ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(chunkIdWithIndexes));
                    jobSpecExt->set_medium_index(chunkIdWithIndexes.MediumIndex);
                    if (auto* chunk = job->GetChunk()) {
                        bool isErasure = chunk->IsErasure();
                        for (auto replica : chunk->StoredReplicas()) {
                            if (replica.GetPtr() == node) {
                                continue;
                            }
                            if (isErasure && replica.GetReplicaIndex() != chunkIdWithIndexes.ReplicaIndex) {
                                continue;
                            }
                            jobSpecExt->add_replicas(ToProto<ui32>(replica));
                        }
                        jobSpecExt->set_replicas_expiration_deadline(ToProto<ui64>(chunkRemovalJobExpirationDeadline));
                    }
                    break;
                }

                case EJobType::RepairChunk: {
                    auto* chunk = chunkManager->GetChunk(chunkId);

                    auto* jobSpecExt = jobSpec.MutableExtension(TRepairChunkJobSpecExt::repair_chunk_job_spec_ext);
                    jobSpecExt->set_erasure_codec(static_cast<int>(chunk->GetErasureCodec()));
                    ToProto(jobSpecExt->mutable_chunk_id(), chunkId);
                    jobSpecExt->set_decommission(job->GetDecommission());

                    if (chunk->IsJournal()) {
                        YT_VERIFY(chunk->IsSealed());
                        jobSpecExt->set_row_count(chunk->GetPhysicalSealedRowCount());
                    }

                    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());

                    const auto& sourceReplicas = chunk->StoredReplicas();
                    builder.Add(sourceReplicas);
                    ToProto(jobSpecExt->mutable_source_replicas(), sourceReplicas);

                    for (auto replica : job->TargetReplicas()) {
                        jobSpecExt->add_target_replicas(ToProto<ui64>(replica));
                        builder.Add(replica);
                    }
                    break;
                }

                case EJobType::SealChunk: {
                    auto* chunk = chunkManager->GetChunk(chunkId);

                    auto* jobSpecExt = jobSpec.MutableExtension(TSealChunkJobSpecExt::seal_chunk_job_spec_ext);
                    ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(chunkIdWithIndexes));
                    jobSpecExt->set_codec_id(ToProto<int>(chunk->GetErasureCodec()));
                    jobSpecExt->set_medium_index(chunkIdWithIndexes.MediumIndex);
                    jobSpecExt->set_row_count(chunk->GetPhysicalSealedRowCount());

                    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());
                    const auto& replicas = chunk->StoredReplicas();
                    builder.Add(replicas);
                    ToProto(jobSpecExt->mutable_source_replicas(), replicas);
                    break;
                }

                case EJobType::MergeChunks: {
                    auto* jobSpecExt = jobSpec.MutableExtension(TMergeChunksJobSpecExt::merge_chunks_job_spec_ext);

                    jobSpecExt->set_cell_tag(Bootstrap_->GetCellTag());

                    ToProto(jobSpecExt->mutable_output_chunk_id(), EncodeChunkId(chunkIdWithIndexes));
                    jobSpecExt->set_medium_index(chunkIdWithIndexes.MediumIndex);
                    *jobSpecExt->mutable_chunk_merger_writer_options() = job->ChunkMergerWriterOptions();

                    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());

                    for (auto* chunk : job->InputChunks()) {
                        auto* protoChunk = jobSpecExt->add_input_chunks();
                        ToProto(protoChunk->mutable_id(), chunk->GetId());

                        const auto& replicas = chunk->StoredReplicas();
                        ToProto(protoChunk->mutable_source_replicas(), replicas);
                        builder.Add(replicas);
                    }

                    break;
                }
                default:
                    YT_ABORT();
            }

            auto serializedJobSpec = SerializeProtoToRefWithEnvelope(jobSpec);
            response->Attachments().push_back(serializedJobSpec);
        }

        for (const auto& job : jobsToAbort) {
            ToProto(response->add_jobs_to_abort(), job->GetJobId());
        }

        for (const auto& job : jobsToRemove) {
            ToProto(response->add_jobs_to_remove(), {job->GetJobId()});
        }

        if (node->ResourceUsage() != resourceUsage || node->ResourceLimits() != resourceLimits) {
            TReqUpdateNodeResources request;
            request.set_node_id(node->GetId());
            request.mutable_resource_usage()->CopyFrom(resourceUsage);
            request.mutable_resource_limits()->CopyFrom(resourceLimits);

            nodeTracker->CreateUpdateNodeResourcesMutation(request)
                ->CommitAndLog(Logger);
        }

        context->Reply();
    }
};

NRpc::IServicePtr CreateJobTrackerService(TBootstrap* boostrap)
{
    return New<TJobTrackerService>(boostrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
