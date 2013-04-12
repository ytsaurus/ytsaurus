#include "stdafx.h"
#include "job_tracker_service.h"
#include "chunk_manager.h"
#include "job.h"
#include "private.h"

#include <ytlib/misc/string.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <server/node_tracker_server/node_tracker.h>
#include <server/node_tracker_server/node.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NCellMaster;
using namespace NJobTrackerClient;
using namespace NNodeTrackerServer;

using NJobTrackerClient::NProto::TJobInfo;
using NJobTrackerClient::NProto::TJobStartInfo;
using NJobTrackerClient::NProto::TJobStopInfo;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TJobTrackerService::TJobTrackerService(TBootstrap* bootstrap)
    : TMetaStateServiceBase(
        bootstrap,
        TJobTrackerServiceProxy::GetServiceName(),
        ChunkServerLogger.GetCategory())
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
}

// TODO(babenko): eliminate this copy-paste
TNode* TJobTrackerService::GetNode(TNodeId nodeId)
{
    auto nodeTracker = Bootstrap->GetNodeTracker();
    auto* node = nodeTracker->FindNode(nodeId);
    if (!node) {
        THROW_ERROR_EXCEPTION(
            NNodeTrackerClient::EErrorCode::NoSuchNode,
            "Invalid or expired node id: %d",
            nodeId);
    }
    return node;
}

DEFINE_RPC_SERVICE_METHOD(TJobTrackerService, Heartbeat)
{
    ValidateActiveLeader();

    auto nodeId = request->node_id();

    context->SetRequestInfo("NodeId: %d", nodeId);

    // TODO(babenko): eliminate this copy-paste
    auto* node = GetNode(nodeId);
    if (node->GetState() != ENodeState::Online) {
        context->Reply(TError(
            NNodeTrackerClient::EErrorCode::InvalidState,
            "Cannot process a heartbeat in %s state",
            ~FormatEnum(node->GetState())));
        return;
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    auto nodeTracker = Bootstrap->GetNodeTracker();
    
    std::vector<TJobPtr> currentJobs;
    FOREACH (const auto& jobInfo, request->jobs()) {
        auto jobId = FromProto<TJobId>(jobInfo.job_id());
        auto job = chunkManager->FindJob(jobId);
        if (job) {
            job->SetState(EJobState(jobInfo.state()));
            if (jobInfo.has_error()) {
                job->Error() = FromProto(jobInfo.error());
            }
            currentJobs.push_back(job);
        } else {
            LOG_WARNING("Stopping unknown or obsolete job (JobId: %s, Address: %s)",
                ~ToString(jobId),
                ~node->GetAddress());
            auto* jobInfo = response->add_jobs_to_stop();
            ToProto(jobInfo->mutable_job_id(), jobId);
        }
    }

    std::vector<TJobPtr> jobsToStart;
    std::vector<TJobPtr> jobsToStop;
    chunkManager->ScheduleJobs(
        node,
        currentJobs,
        &jobsToStart,
        &jobsToStop);

    FOREACH (auto job, jobsToStart) {
        auto* jobInfo = response->add_jobs_to_start();
        ToProto(jobInfo->mutable_job_id(), job->GetJobId());
        jobInfo->set_type(job->GetType());
        ToProto(jobInfo->mutable_chunk_id(), job->GetChunkId());
        FOREACH (const auto& targetAddress, job->TargetAddresses()) {
            auto* target = nodeTracker->FindNodeByAddress(targetAddress);
            ToProto(jobInfo->add_targets(), target->GetDescriptor());
        }
    }

    FOREACH (auto job, jobsToStop) {
        auto* jobInfo = response->add_jobs_to_stop();
        ToProto(jobInfo->mutable_job_id(), job->GetJobId());
    }

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
