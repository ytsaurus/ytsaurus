#include "job_tracker_service.h"
#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "job.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/master_hydra_service.h>

#include <yt/server/node_tracker_server/node.h>
#include <yt/server/node_tracker_server/node_directory_builder.h>
#include <yt/server/node_tracker_server/node_tracker.h>

#include <yt/ytlib/chunk_client/job.pb.h>

#include <yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/string.h>

#include <yt/core/rpc/helpers.h>

namespace NYT {
namespace NChunkServer {

using namespace NHydra;
using namespace NJobTrackerClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NJobTrackerClient::NProto;
using namespace NChunkClient::NProto;
using namespace NCellMaster;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TJobTrackerService
    : public NCellMaster::TMasterHydraServiceBase
{
public:
    explicit TJobTrackerService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TJobTrackerServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::JobTracker,
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
        const auto& resourceUsage = request->resource_usage();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        context->SetRequestInfo("NodeId: %v, Address: %v, ResourceUsage: %v",
            nodeId,
            node->GetDefaultAddress(),
            FormatResourceUsage(resourceUsage, resourceLimits));

        if (node->GetLocalState() != ENodeState::Online) {
            THROW_ERROR_EXCEPTION(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Cannot process a heartbeat in %Qlv state",
                node->GetLocalState());
        }

        node->ResourceLimits() = resourceLimits;
        node->ResourceUsage() = resourceUsage;

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        std::vector<TJobPtr> currentJobs;
        for (const auto& jobStatus : request->jobs()) {
            auto jobId = FromProto<TJobId>(jobStatus.job_id());
            auto state = EJobState(jobStatus.state());
            auto job = chunkManager->FindJob(jobId);
            if (job) {
                job->SetState(state);
                if (state == EJobState::Completed || state == EJobState::Failed) {
                    job->Error() = FromProto<TError>(jobStatus.result().error());
                }
                currentJobs.push_back(job);
            } else {
                switch (state) {
                    case EJobState::Completed:
                        LOG_DEBUG("Unknown job has completed, removal scheduled (JobId: %v)",
                            jobId);
                        ToProto(response->add_jobs_to_remove(), jobId);
                        break;

                    case EJobState::Failed:
                        LOG_DEBUG("Unknown job has failed, removal scheduled (JobId: %v)",
                            jobId);
                        ToProto(response->add_jobs_to_remove(), jobId);
                        break;

                    case EJobState::Aborted:
                        LOG_DEBUG("Job aborted, removal scheduled (JobId: %v)",
                            jobId);
                        ToProto(response->add_jobs_to_remove(), jobId);
                        break;

                    case EJobState::Running:
                        LOG_DEBUG("Unknown job is running, abort scheduled (JobId: %v)",
                            jobId);
                        ToProto(response->add_jobs_to_abort(), jobId);
                        break;

                    case EJobState::Waiting:
                        LOG_DEBUG("Unknown job is waiting, abort scheduled (JobId: %v)",
                            jobId);
                        ToProto(response->add_jobs_to_abort(), jobId);
                        break;

                    default:
                        Y_UNREACHABLE();
                }
            }
        }

        std::vector<TJobPtr> jobsToStart;
        std::vector<TJobPtr> jobsToAbort;
        std::vector<TJobPtr> jobsToRemove;
        chunkManager->ScheduleJobs(
            node,
            currentJobs,
            &jobsToStart,
            &jobsToAbort,
            &jobsToRemove);

        for (const auto& job : jobsToStart) {
            const auto& chunkIdWithIndexes = job->GetChunkIdWithIndexes();

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
                        jobSpecExt->add_target_replicas(ToProto<ui32>(replica));
                        builder.Add(replica);
                    }
                    break;
                }

                case EJobType::RemoveChunk: {
                    auto* jobSpecExt = jobSpec.MutableExtension(TRemoveChunkJobSpecExt::remove_chunk_job_spec_ext);
                    ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(chunkIdWithIndexes));
                    jobSpecExt->set_medium_index(chunkIdWithIndexes.MediumIndex);
                    break;
                }

                case EJobType::RepairChunk: {
                    auto* chunk = chunkManager->GetChunk(chunkIdWithIndexes.Id);

                    auto* jobSpecExt = jobSpec.MutableExtension(TRepairChunkJobSpecExt::repair_chunk_job_spec_ext);
                    jobSpecExt->set_erasure_codec(static_cast<int>(chunk->GetErasureCodec()));
                    ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(chunkIdWithIndexes));

                    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());

                    const auto& sourceReplicas = chunk->StoredReplicas();
                    builder.Add(sourceReplicas);
                    ToProto(jobSpecExt->mutable_source_replicas(), sourceReplicas);

                    for (auto replica: job->TargetReplicas()) {
                        jobSpecExt->add_target_replicas(ToProto<ui32>(replica));
                        builder.Add(replica);
                    }
                    break;
                }

                case EJobType::SealChunk: {
                    auto* chunk = chunkManager->GetChunk(chunkIdWithIndexes.Id);

                    auto* jobSpecExt = jobSpec.MutableExtension(TSealChunkJobSpecExt::seal_chunk_job_spec_ext);
                    ToProto(jobSpecExt->mutable_chunk_id(), EncodeChunkId(chunkIdWithIndexes));
                    jobSpecExt->set_medium_index(chunkIdWithIndexes.MediumIndex);
                    jobSpecExt->set_row_count(chunk->GetSealedRowCount());

                    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());
                    const auto& replicas = chunk->StoredReplicas();
                    builder.Add(replicas);
                    ToProto(jobSpecExt->mutable_source_replicas(), replicas);
                    break;
                }

                default:
                    Y_UNREACHABLE();
            }

            auto serializedJobSpec = SerializeProtoToRefWithEnvelope(jobSpec);
            response->Attachments().push_back(serializedJobSpec);
        }

        for (const auto& job : jobsToAbort) {
            ToProto(response->add_jobs_to_abort(), job->GetJobId());
        }

        for (const auto& job : jobsToRemove) {
            ToProto(response->add_jobs_to_remove(), job->GetJobId());
        }

        context->Reply();
    }
};

NRpc::IServicePtr CreateJobTrackerService(TBootstrap* boostrap)
{
    return New<TJobTrackerService>(boostrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
