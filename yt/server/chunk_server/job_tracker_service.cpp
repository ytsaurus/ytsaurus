#include "stdafx.h"
#include "job_tracker_service.h"
#include "chunk_manager.h"
#include "job.h"
#include "node_directory_builder.h"
#include "chunk.h"
#include "private.h"

#include <core/misc/string.h>
#include <core/misc/protobuf_helpers.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/chunk_client/job.pb.h>

#include <server/cell_master/meta_state_service.h>

#include <server/node_tracker_server/node_tracker.h>
#include <server/node_tracker_server/node.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NJobTrackerClient::NProto;
using namespace NChunkClient::NProto;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobTrackerService
    : public NCellMaster::TMetaStateServiceBase
{
public:
    explicit TJobTrackerService(TBootstrap* bootstrap)
        : TMetaStateServiceBase(
            bootstrap,
            TJobTrackerServiceProxy::GetServiceName(),
            ChunkServerLogger.GetCategory())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
    }

private:
    typedef TJobTrackerService TThis;

    DECLARE_RPC_SERVICE_METHOD(NJobTrackerClient::NProto, Heartbeat)
    {
        ValidateActiveLeader();

        auto nodeId = request->node_id();
        const auto& resourceLimits = request->resource_limits();
        const auto& resourceUsage = request->resource_usage();

        context->SetRequestInfo("NodeId: %d, ResourceUsage: {%s}",
            nodeId,
            ~FormatResourceUsage(resourceUsage, resourceLimits));

        auto nodeTracker = Bootstrap->GetNodeTracker();
        auto chunkManager = Bootstrap->GetChunkManager();

        auto* node = nodeTracker->GetNodeOrThrow(nodeId);

        if (node->GetState() != ENodeState::Online) {
            context->Reply(TError(
                NNodeTrackerClient::EErrorCode::InvalidState,
                "Cannot process a heartbeat in %s state",
                ~FormatEnum(node->GetState())));
            return;
        }

        node->ResourceLimits() = resourceLimits;
        node->ResourceUsage() = resourceUsage;

        std::vector<TJobPtr> currentJobs;
        FOREACH (const auto& jobStatus, request->jobs()) {
            auto jobId = FromProto<TJobId>(jobStatus.job_id());
            auto state = EJobState(jobStatus.state());
            auto jobType = EJobType(jobStatus.job_type());
            if (jobType <= EJobType::MasterFirst || jobType >= EJobType::MasterLast) {
                // Create a foreign job.
                auto job = TJob::CreateForeign(
                    jobId,
                    jobStatus.resource_usage());
                currentJobs.push_back(job);
            } else {
                // Lookup the master job.
                auto job = chunkManager->FindJob(jobId);
                if (job) {
                    job->SetState(state);
                    if (state == EJobState::Completed || state == EJobState::Failed) {
                        job->Error() = FromProto(jobStatus.result().error());
                    }
                    currentJobs.push_back(job);
                } else {
                    switch (state) {
                        case EJobState::Completed:
                            LOG_WARNING("Unknown job has completed, removal scheduled");
                            ToProto(response->add_jobs_to_remove(), jobId);
                            break;

                        case EJobState::Failed:
                            LOG_INFO("Unknown job has failed, removal scheduled");
                            ToProto(response->add_jobs_to_remove(), jobId);
                            break;

                        case EJobState::Aborted:
                            LOG_INFO("Job aborted, removal scheduled");
                            ToProto(response->add_jobs_to_remove(), jobId);
                            break;

                        case EJobState::Running:
                            LOG_WARNING("Unknown job is running, abort scheduled");
                            ToProto(response->add_jobs_to_abort(), jobId);
                            break;

                        case EJobState::Waiting:
                            LOG_WARNING("Unknown job is waiting, abort scheduled");
                            ToProto(response->add_jobs_to_abort(), jobId);
                            break;

                        default:
                            YUNREACHABLE();
                    }
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

        FOREACH (auto job, jobsToStart) {
            const auto& chunkIdWithIndex = job->GetChunkIdWithIndex();

            auto* jobInfo = response->add_jobs_to_start();
            ToProto(jobInfo->mutable_job_id(), job->GetJobId());
            *jobInfo->mutable_resource_limits() = job->ResourceUsage();

            auto* jobSpec = jobInfo->mutable_spec();
            jobSpec->set_type(job->GetType());

            auto* chunkjobSpecExt = jobSpec->MutableExtension(TChunkJobSpecExt::chunk_job_spec_ext);
            ToProto(chunkjobSpecExt->mutable_chunk_id(), EncodeChunkId(chunkIdWithIndex));

            switch (job->GetType()) {
                case EJobType::ReplicateChunk: {
                    auto* replicationJobSpecExt = jobSpec->MutableExtension(TReplicationJobSpecExt::replication_job_spec_ext);
                    SerializeDescriptors(replicationJobSpecExt->mutable_target_descriptors(), job->TargetAddresses());
                    break;
                }

                case EJobType::RemoveChunk:
                    break;

                case EJobType::RepairChunk: {
                    auto chunk = chunkManager->GetChunk(chunkIdWithIndex.Id);

                    auto* repairJobSpecExt = jobSpec->MutableExtension(TRepairJobSpecExt::repair_job_spec_ext);
                    repairJobSpecExt->set_erasure_codec(chunk->GetErasureCodec());
                    ToProto(repairJobSpecExt->mutable_erased_indexes(), job->ErasedIndexes());

                    TNodeDirectoryBuilder builder(repairJobSpecExt->mutable_node_directory());
                    const auto& replicas = chunk->StoredReplicas();
                    builder.Add(replicas);
                    ToProto(repairJobSpecExt->mutable_replicas(), replicas);

                    SerializeDescriptors(repairJobSpecExt->mutable_target_descriptors(), job->TargetAddresses());
                    break;
                }

                default:
                    YUNREACHABLE();
            }
        }

        FOREACH (auto job, jobsToAbort) {
            ToProto(response->add_jobs_to_abort(), job->GetJobId());
        }

        FOREACH (auto job, jobsToRemove) {
            ToProto(response->add_jobs_to_remove(), job->GetJobId());
        }

        context->Reply();
    }

    void SerializeDescriptors(
        ::google::protobuf::RepeatedPtrField<NNodeTrackerClient::NProto::TNodeDescriptor>* protoDescriptors,
        const std::vector<Stroka>& addresses)
    {
        auto nodeTracker = Bootstrap->GetNodeTracker();
        FOREACH (const auto& address, addresses) {
            auto* target = nodeTracker->GetNodeByAddress(address);
            NNodeTrackerClient::ToProto(protoDescriptors->Add(), target->GetDescriptor());
        }
    }

};

NRpc::IServicePtr CreateJobTrackerService(TBootstrap* boostrap)
{
    return New<TJobTrackerService>(boostrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
