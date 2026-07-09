#include "worker_tracker_service.h"

#include "controller.h"
#include "private.h"
#include "worker.h"
#include "worker_tracker.h"

#include <yt/yt/flow/library/cpp/common/authenticator.h>
#include <yt/yt/flow/library/cpp/common/controller/worker_tracker_service_proxy.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/build/build.h>

#include <yt/yt/library/orchid/orchid_ypath_service.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

using namespace NOrchid;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

using NConcurrency::WaitFor;
using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TWorkerTrackerService
    : public TServiceBase
{
public:
    TWorkerTrackerService(
        IControllerPtr controller,
        IWorkerTrackerPtr workerTracker,
        IInvokerPtr invoker,
        IChannelFactoryPtr channelFactory,
        IPipelineAuthenticatorPtr authenticator)
        : NRpc::TServiceBase(
            std::move(invoker),
            TWorkerTrackerServiceProxy::GetDescriptor(),
            ControllerLogger(),
            TServiceOptions{
                .Authenticator = authenticator->CreateSelfRpcAuthenticator(),
            })
        , Controller_(std::move(controller))
        , WorkerTracker_(std::move(workerTracker))
        , WorkerChannelFactory_(authenticator->CreateSelfCredentialsInjectingChannelFactory(channelFactory))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Handshake));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
    }

private:
    const IControllerPtr Controller_;
    const IWorkerTrackerPtr WorkerTracker_;
    const IChannelFactoryPtr WorkerChannelFactory_;

private:
    void CheckWorkerNodeIncarnationId(const TNodeInfoBase& workerNodeInfo)
    {
        try {
            auto ypathService = CreateOrchidYPathService(TOrchidOptions{.Channel = WorkerChannelFactory_->CreateChannel(workerNodeInfo.RpcAddress)});
            auto orchidRequestResult = WaitFor(ExecuteVerb(ypathService, TYPathProxy::Get(TYPath("/node_info/incarnation_id"))));
            THROW_ERROR_EXCEPTION_UNLESS(orchidRequestResult.IsOK(), "Cannot fetch worker node incarnation id via provided rpc address");
            auto actualIncarnationId = ConvertTo<TIncarnationId>(TYsonStringBuf(orchidRequestResult.Value()->value()));
            if (actualIncarnationId != workerNodeInfo.IncarnationId) {
                THROW_ERROR_EXCEPTION("Worker node incarnation ids mismatch")
                    << TErrorAttribute("actual_incarnation_id", actualIncarnationId)
                    << TErrorAttribute("expected_incarnation_id", workerNodeInfo.IncarnationId);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Worker node incarnation id check failed")
                << TError(ex)
                << TErrorAttribute("worker_identifying_string", workerNodeInfo.GetIdentifyingString());
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Handshake)
    {
        auto workerNodeInfo = FromProto<TNodeInfoBase>(request->node_info());

        auto workerGroups = FromProto<std::vector<TWorkerGroupId>>(request->worker_groups());
        THashMap<std::string, ssize_t> workerCapabilities;
        for (const auto& capability : request->worker_capabilities()) {
            EmplaceOrCrash(workerCapabilities, capability.key(), capability.value());
        }

        context->SetRequestInfo("WorkerIdentifyingString: %v, WorkerBuildVersion: %v, WorkerGroups: %v, WorkerCapabilities: %v",
            workerNodeInfo.GetIdentifyingString(),
            workerNodeInfo.BuildVersion,
            workerGroups,
            workerCapabilities);

        CheckWorkerNodeIncarnationId(workerNodeInfo);

        const auto workerInfo = WorkerTracker_->RegisterWorker(workerNodeInfo, std::move(workerGroups), std::move(workerCapabilities));

        context->SetResponseInfo(
            "ConnectionIncarnationId: %v",
            workerInfo.ConnectionIncarnationId);

        ToProto(response->mutable_connection_incarnation_id(), workerInfo.ConnectionIncarnationId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Heartbeat)
    {
        TInstant prepareStart = TInstant::Now();

        const auto& workerAddress = request->worker_rpc_address();
        const auto connectionIncarnationId = FromProto<NWorker::TIncarnationId>(request->connection_incarnation_id());
        const auto& heartbeatSeqNo = request->heartbeat_seq_no();

        context->SetRequestInfo("Worker: %v, ConnectionIncarnationId: %v",
            workerAddress,
            connectionIncarnationId);

        const auto workerInfo = WorkerTracker_->HandleWorkerHeartbeat(workerAddress, connectionIncarnationId, heartbeatSeqNo);

        TFlowViewPtr flowView;
        try {
            flowView = Controller_->GetFlowViewKeeper()->GetFlowView();
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            if (error.FindMatching(NFlow::EErrorCode::FlowViewKeeperIsNotInitialized)) {
                YT_LOG_WARNING(ex, "Sending empty heartbeat response since flow view keeper is not initialized yet");
                context->Reply();
                return;
            } else {
                throw;
            }
        }

        TDuration prepareDuration = TInstant::Now() - prepareStart;

        const auto& flowCoreTarget = flowView->State->ExecutionSpec->FlowCoreTarget->GetValue().Underlying();
        if (!flowCoreTarget.empty() && flowCoreTarget != workerInfo.FlowCoreVersion) {
            response->set_flow_core_target_mismatch(flowCoreTarget);
        }

        if (auto flowWorker = GetOrDefault(flowView->State->Workers, workerAddress, nullptr); flowWorker && flowWorker->IncarnationId == workerInfo.IncarnationId) {
            TInstant collectSpecUpdateStart = TInstant::Now();
            auto executionSpecVersions = ConvertTo<TExecutionSpecVersionsPtr>(TYsonStringBuf(request->execution_spec_versions()));
            auto [executionSpecUpdate, stateUpdate] = PrepareExecutionSpecUpdate(flowView->State->ExecutionSpec, executionSpecVersions);
            // TODO(mikari): filter job leases?
            response->set_execution_spec(ToProto(ConvertToYsonString(executionSpecUpdate)));
            for (const auto& src : stateUpdate) {
                auto* dst = response->add_state_update();
                dst->set_sequence_id(src.SequenceId.Underlying());
                dst->set_name(src.Name);
                dst->set_key_left(src.KeyLeft);
                dst->set_key_right(src.KeyRight);
                dst->set_value(src.Value);
            }
            TDuration collectSpecUpdateDuration = TInstant::Now() - collectSpecUpdateStart;
            ssize_t stateUpdateSize = stateUpdate.size();

            TInstant gatherStatusesStart = TInstant::Now();
            THashMap<TJobId, TJobStatusPtr> statuses;
            for (const auto& job : request->running_jobs()) {
                TJobId jobId;
                FromProto(&jobId, job.job_id());
                try {
                    auto jobStatus = ConvertTo<TJobStatusPtr>(TYsonStringBuf(job.job_status()));
                    statuses[jobId] = jobStatus;
                } catch (const std::exception& ex) {
                    // Job would be dropped anyway after LostJobTimeout.
                    YT_LOG_EVENT(
                        PublicControllerLogger,
                        NLogging::ELogLevel::Error,
                        ex,
                        "Job sent invalid status, ignored (WorkerIdentifyingString: %v, JobId: %v)",
                        workerInfo.GetIdentifyingString(),
                        jobId);
                }
            }
            TDuration gatherStatusesDuration = TInstant::Now() - gatherStatusesStart;
            ssize_t statusesSize = statuses.size();

            TInstant setDynamicSpec = TInstant::Now();
            if (const auto* jobIds = flowView->EphemeralState->WorkerIncarnationsJobs.FindPtr(flowWorker->IncarnationId)) {
                for (const auto& jobId : *jobIds) {
                    const auto& partitionId = GetOrCrash(flowView->State->ExecutionSpec->Layout->Jobs, jobId)->PartitionId;
                    if (const auto* partitionState = flowView->EphemeralState->Partitions.FindPtr(partitionId); partitionState && (*partitionState)->DynamicPartitionSpec) {
                        auto* jobDynamicComputationPartitionSpec = response->add_jobs_dynamic_computation_partition_specs();
                        ToProto(jobDynamicComputationPartitionSpec->mutable_job_id(), jobId);
                        jobDynamicComputationPartitionSpec->set_spec(ToProto(ConvertToYsonString((*partitionState)->DynamicPartitionSpec)));
                    }
                }
            }
            TDuration setDynamicSpecDuration = TInstant::Now() - setDynamicSpec;

            response->set_message_transfering_info(ToProto(ConvertToYsonString(flowView->EphemeralState->MessageTransferingInfo)));

            auto workerStatus = ConvertTo<TWorkerStatusPtr>(TYsonStringBuf(request->worker_status()));

            context->Reply();

            TInstant registerStatusesStart = TInstant::Now();
            try {
                for (const auto& [jobId, jobStatus] : statuses) {
                    Controller_->RegisterJobStatus(jobId, jobStatus);
                }
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Failed to register job statuses after responding to worker heartbeat (WorkerAddress: %v)", workerAddress);
            }
            TDuration registerStatusesDuration = TInstant::Now() - registerStatusesStart;

            YT_LOG_INFO("Worker heartbeat timings, milliseconds (PrepareDuration: %Qv, CollectSpecUpdateDuration: %Qv, StateUpdateSize: %Qv, GatherStatusesDuration: %Qv, StatusesSize: %Qv, SetDynamicSpecDuration: %Qv ms, RegisterStatusesDuration: %Qv)",
                prepareDuration.MilliSeconds(),
                collectSpecUpdateDuration.MilliSeconds(),
                stateUpdateSize,
                gatherStatusesDuration.MilliSeconds(),
                statusesSize,
                setDynamicSpecDuration.MilliSeconds(),
                registerStatusesDuration.MilliSeconds());

            try {
                Controller_->RegisterWorkerStatus(workerAddress, workerStatus);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Failed to register worker status after responding to worker heartbeat (WorkerAddress: %v)", workerAddress);
            }
        } else {
            context->Reply();
        }
    }
};

IServicePtr CreateWorkerTrackerService(
    IControllerPtr controller,
    IWorkerTrackerPtr workerTracker,
    IInvokerPtr invoker,
    IChannelFactoryPtr channelFactory,
    IPipelineAuthenticatorPtr authenticator)
{
    return New<TWorkerTrackerService>(
        std::move(controller),
        std::move(workerTracker),
        std::move(invoker),
        std::move(channelFactory),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
