#include "stdafx.h"
#include "job_tracker_service.h"
#include "chunk_manager.h"
#include "job.h"
#include "private.h"

#include <ytlib/misc/string.h>

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

static NLog::TLogger& Logger = ChunkServerLogger;

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

        context->SetRequestInfo("NodeId: %d", nodeId);

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

        node->ResourceLimits() = request->resource_limits();
        node->ResourceUsage() = request->resource_usage();

        std::vector<TJobPtr> currentJobs;
        FOREACH (const auto& jobStatus, request->jobs()) {
            auto jobType = EJobType(jobStatus.job_type());
            // Skip jobs that are not issued by the scheduler.
            if (jobType <= EJobType::MasterFirst || jobType >= EJobType::MasterLast)
                continue;

            auto jobId = FromProto<TJobId>(jobStatus.job_id());
            auto job = chunkManager->FindJob(jobId);
            if (job) {
                auto state = EJobState(jobStatus.state());
                job->SetState(state);
                if (state == EJobState::Completed || state == EJobState::Failed) {
                    job->Error() = FromProto(jobStatus.result().error());
                }
                currentJobs.push_back(job);
            } else {
                LOG_WARNING("Stopping unknown or obsolete job (JobId: %s, Address: %s)",
                    ~ToString(jobId),
                    ~node->GetAddress());
                ToProto(response->add_jobs_to_abort(), jobId);
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
            auto* jobInfo = response->add_jobs_to_start();
            ToProto(jobInfo->mutable_job_id(), job->GetJobId());
            *jobInfo->mutable_resource_limits() = job->ResourceLimits();

            auto* jobSpec = jobInfo->mutable_spec();
            jobSpec->set_type(job->GetType());

            auto* chunkjobSpecExt = jobSpec->MutableExtension(TChunkJobSpecExt::chunk_job_spec_ext);
            ToProto(chunkjobSpecExt->mutable_chunk_id(), job->GetChunkId());

            switch (job->GetType()) {
                case EJobType::ReplicateChunk: {
                    auto* replicationJobSpecExt = jobSpec->MutableExtension(TReplicationJobSpecExt::replication_job_spec_ext);
                    FOREACH (const auto& targetAddress, job->TargetAddresses()) {
                        auto* target = nodeTracker->FindNodeByAddress(targetAddress);
                        ToProto(replicationJobSpecExt->add_target_descriptors(), target->GetDescriptor());
                    }
                    break;
                }

                case EJobType::RemoveChunk:
                    break;

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

};

NRpc::IServicePtr CreateJobTrackerService(TBootstrap* boostrap)
{
    return New<TJobTrackerService>(boostrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
