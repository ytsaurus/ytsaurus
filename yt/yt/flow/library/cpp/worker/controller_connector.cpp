#include "controller_connector.h"

#include "buffer_state_manager.h"
#include "config.h"
#include "job.h"
#include "job_tracker.h"
#include "message_distributor.h"
#include "private.h"

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/service_combiner.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <yt/yt/build/build.h>

#include <yt/yt/flow/library/cpp/common/controller/worker_tracker_service_proxy.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/yt_connector.h>

#include <yt/yt/flow/library/cpp/misc/crash_recorder.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/rpc/bus/channel.h>

namespace NYT::NFlow::NWorker {

using namespace NController;
using namespace NConcurrency;
using namespace NTracing;
using namespace NYTree;
using namespace NProfiling;
using namespace NYson;
using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = WorkerLogger;

////////////////////////////////////////////////////////////////////////////////

class TControllerConnector
    : public IControllerConnector
{
public:
    TControllerConnector(
        TNodeInfoPtr nodeInfo,
        std::vector<std::string> groups,
        THashMap<std::string, ssize_t> capabilities,
        ICommonYTConnectorPtr ytConnector,
        IInvokerPtr controlInvoker,
        IChannelFactoryPtr channelFactory,
        IMessageDistributorPtr messageDistributor,
        IJobDirectoryPtr jobDirectory,
        IJobTrackerPtr jobTracker,
        TStreamSpecStoragePtr streamSpecStorage,
        bool ignoreSingletonsDynamicConfig,
        IStatusProfilerPtr rootStatusProfiler)
        : NodeInfo_(std::move(nodeInfo))
        , YTConnector_(std::move(ytConnector))
        , ControlInvoker_(std::move(controlInvoker))
        , ChannelFactory_(std::move(channelFactory))
        , MessageDistributor_(std::move(messageDistributor))
        , JobDirectory_(std::move(jobDirectory))
        , JobTracker_(std::move(jobTracker))
        , StreamSpecStorage_(std::move(streamSpecStorage))
        , Groups_(std::move(groups))
        , Capabilities_(std::move(capabilities))
        , IgnoreSingletonsDynamicConfig_(ignoreSingletonsDynamicConfig)
        , RootStatusProfiler_(std::move(rootStatusProfiler))
        , ExecutionSpec_(New<TExecutionSpec>())
        , Profiler_(WorkerProfiler().WithPrefix("/controller_connector"))
        , PrepareHeartbeatRequestTimer_(Profiler_.Timer("/heartbeat/prepare_request_time"))
        , WaitHeartbeatResponseTimer_(Profiler_.Timer("/heartbeat/wait_response_time"))
        , ProcessHeartbeatResponseTimer_(Profiler_.Timer("/heartbeat/process_response_time"))
    {
        YT_ASSERT_INVOKER_THREAD_AFFINITY(ControlInvoker_, ControlThread);
    }

    void Initialize() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        ScheduleConnect(true);
    }

    IYPathServicePtr CreateOrchidService() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto staticOrchidProducer = BIND_NO_PROPAGATE(&TControllerConnector::BuildStaticOrchid, MakeStrong(this));
        auto staticOrchidService = IYPathService::FromProducer(staticOrchidProducer, GetControllerConnectorSpec()->OrchidUpdatePeriod);

        StaticOrchidService_ = DynamicPointerCast<ICachedYPathService>(staticOrchidService);
        YT_VERIFY(StaticOrchidService_);

        auto combinedOrchidService = CreateServiceCombiner(
            {
                staticOrchidService->Via(ControlInvoker_),
            },
            GetControllerConnectorSpec()->OrchidUpdatePeriod);
        CombinedOrchidService_.Reset(combinedOrchidService.Get());
        YT_VERIFY(CombinedOrchidService_);

        return combinedOrchidService;
    }

    TIncarnationId GetIncarnationId() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return NodeInfo_->IncarnationId;
    }

    NRpc::IChannelPtr GetControllerChannel() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return AtomicControllerChannel_.Acquire();
    }

    void BuildStaticOrchid(IYsonConsumer* consumer)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        // clang-format off
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("service").BeginMap()
                    // XXX(babenko): move elsewhere
                    .Item("connected").Value(Connected_)
                    .Item("last_connection_time").Value(ConnectionTime_)
                    .Item("last_heartbeat_time").Value(LastHeartbeatTime_)
                .EndMap()
                .DoIf(Connected_, [&] (TFluentMap fluent) {
                    fluent
                        .Item("connection_incarnation_id")
                            .Value(ConnectionIncarnationId_)
                        .Item("heartbeat_seq_no")
                            .Value(HeartbeatSeqNo_);
                })
                .Item("execution_spec_epoch").Value(ExecutionSpec_->GetEpoch())
            .EndMap();
        // clang-format on
    }

    void Disconnect(const TError& error) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        DoDisconnect(error);
    }

    const TDynamicControllerConnectorSpecPtr GetControllerConnectorSpec() const
    {
        return ExecutionSpec_->DynamicPipelineSpec->GetValue()->ControllerConnector;
    }

    void Reconfigure(
        const TExecutionSpecPtr& newExecutionSpec,
        const THashMap<TJobId, NYTree::IMapNodePtr>& dynamicComputationPartitionSpecs)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        TForbidContextSwitchGuard contextSwitchGuard;

        auto oldExecutionSpec = ExecutionSpec_;
        ExecutionSpec_ = newExecutionSpec;
        auto oldExecutionSpecEpoch = ExecutionSpecEpoch_;
        ExecutionSpecEpoch_ = newExecutionSpec->GetEpoch();

        if (StreamSpecStorage_->GetVersion() != ExecutionSpec_->StreamSpecStorageState->GetVersion()) {
            YT_TLOG_INFO("Reconfigure stream spec storage")
                .With("CurrentVersion", StreamSpecStorage_->GetVersion())
                .With("NewVersion", ExecutionSpec_->StreamSpecStorageState->GetVersion());
            StreamSpecStorage_->Reconfigure(ExecutionSpec_->StreamSpecStorageState);
        }

        if (oldExecutionSpecEpoch != ExecutionSpecEpoch_) {
            YT_TLOG_INFO("Reconfigure dynamic worker spec")
                .With("CurrentEpoch", oldExecutionSpecEpoch)
                .With("NewEpoch", ExecutionSpecEpoch_);

            const auto& dynamicSpec = ExecutionSpec_->DynamicPipelineSpec->GetValue();

            if (!IgnoreSingletonsDynamicConfig_) {
                TSingletonManager::Reconfigure(dynamicSpec->Singletons);
            }

            StaticOrchidService_->SetCachePeriod(dynamicSpec->ControllerConnector->OrchidUpdatePeriod);
            CombinedOrchidService_->SetUpdatePeriod(dynamicSpec->ControllerConnector->OrchidUpdatePeriod);

            if (HeartbeatExecutor_) {
                HeartbeatExecutor_->SetPeriod(dynamicSpec->ControllerConnector->ControllerHeartbeatPeriod);
            }
            if (CheckControllerExecutor_) {
                CheckControllerExecutor_->SetPeriod(dynamicSpec->ControllerConnector->ControllerDiscoverPeriod);
            }

            JobDirectory_->Reconfigure(ExecutionSpec_->Layout, ExecutionSpec_->PipelineSpec->GetValue());
            MessageDistributor_->Reconfigure(ExecutionSpec_->PipelineSpec->GetValue(), dynamicSpec->MessageDistributor);
        }
        JobTracker_->Reconfigure(ExecutionSpec_, dynamicComputationPartitionSpecs);
    }

    DEFINE_SIGNAL_OVERRIDE(void(), ControllerConnecting);
    DEFINE_SIGNAL_OVERRIDE(void(TIncarnationId), ControllerConnected);
    DEFINE_SIGNAL_OVERRIDE(void(), ControllerDisconnected);

private:
    const TNodeInfoPtr NodeInfo_;
    const ICommonYTConnectorPtr YTConnector_;
    const IInvokerPtr ControlInvoker_;

    const IChannelFactoryPtr ChannelFactory_;
    const IMessageDistributorPtr MessageDistributor_;
    const IJobDirectoryPtr JobDirectory_;
    const IJobTrackerPtr JobTracker_;
    const TStreamSpecStoragePtr StreamSpecStorage_;
    const std::vector<std::string> Groups_;
    const THashMap<std::string, ssize_t> Capabilities_;
    const bool IgnoreSingletonsDynamicConfig_;
    const IStatusProfilerPtr RootStatusProfiler_;

    ICachedYPathServicePtr StaticOrchidService_;
    IServiceCombinerPtr CombinedOrchidService_;

    TInstant LastHeartbeatTime_ = TInstant::Zero();

    std::string ControllerAddress_;
    bool Connected_ = false;
    bool ConnectScheduled_ = false;
    TInstant ConnectionTime_ = TInstant::Zero();
    TIncarnationId ConnectionIncarnationId_;
    ui64 HeartbeatSeqNo_ = 0;
    IChannelPtr ControllerChannel_;
    //! Mirror of ControllerChannel_ for out-of-thread readers (e.g. job
    //! throttler clients). Must be updated whenever ControllerChannel_ changes.
    TAtomicIntrusivePtr<NRpc::IChannel> AtomicControllerChannel_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableControlInvoker_;

    TPeriodicExecutorPtr HeartbeatExecutor_;
    TPeriodicExecutorPtr CheckControllerExecutor_;

    TExecutionSpecPtr ExecutionSpec_;
    i64 ExecutionSpecEpoch_ = -1;

    // Tracks the last FlowCoreTarget value the controller reported as a mismatch reason,
    // so we log it only on transitions (not every heartbeat).
    std::optional<std::string> LoggedFlowCoreTargetMismatch_;

    NProfiling::TProfiler Profiler_;
    NProfiling::TEventTimer PrepareHeartbeatRequestTimer_;
    NProfiling::TEventTimer WaitHeartbeatResponseTimer_;
    NProfiling::TEventTimer ProcessHeartbeatResponseTimer_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void ScheduleConnect(bool immediate)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (ConnectScheduled_) {
            return;
        }

        ConnectScheduled_ = true;
        TDelayedExecutor::Submit(
            BIND(&TControllerConnector::DoConnect, MakeStrong(this))
                .Via(ControlInvoker_),
            immediate ? TDuration::Zero() : GetControllerConnectorSpec()->ControllerHandshakeFailureBackoff);
    }

    void DoConnect()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(ConnectScheduled_);
        ConnectScheduled_ = false;

        try {
            OnConnecting();
            EstablishConnectionToController();
            PerformHandshake();
            OnConnected();
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Error connecting to Controller")
                .With(ex);
            ControllerDisconnected_.Fire();
            DoCleanup();
            ScheduleConnect(false);
        }
    }

    void OnConnecting()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        // NB: We cannot be sure the previous incarnation did a proper cleanup due to possible
        // fiber cancelation.
        DoCleanup();

        YT_TLOG_INFO("Connecting to controller");

        YT_VERIFY(!CancelableContext_);
        CancelableContext_ = New<TCancelableContext>();
        CancelableControlInvoker_ = CancelableContext_->CreateInvoker(ControlInvoker_);

        SwitchTo(CancelableControlInvoker_);

        ControllerConnecting_.Fire();
    }

    void EstablishConnectionToController()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_TLOG_INFO("Discovering controller");

        ControllerAddress_ = WaitFor(YTConnector_->GetPipelineAttributes())
            .ValueOrThrow()
            .LeaderControllerAddress;

        ControllerChannel_ = ChannelFactory_->CreateChannel(ControllerAddress_);
        AtomicControllerChannel_.Store(ControllerChannel_);
    }

    void CheckController()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_TLOG_INFO("Checking that controller address was not changed");

        try {
            const auto newAddress = WaitFor(YTConnector_->GetPipelineAttributes())
                .ValueOrThrow()
                .LeaderControllerAddress;
            if (newAddress == ControllerAddress_) {
                YT_TLOG_INFO("Controller address was not changed");
                return;
            }
            Disconnect(TError("Controller address was changed")
                << TErrorAttribute("old_address", ControllerAddress_)
                << TErrorAttribute("new_address", newAddress));
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Failed to check that controller address was not changed")
                .With(ex);
        }
    }

    void PerformHandshake()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_TLOG_INFO("Sending handshake");

        TWorkerTrackerServiceProxy proxy(ControllerChannel_);
        auto req = proxy.Handshake();
        req->SetTimeout(GetControllerConnectorSpec()->ControllerHandshakeRpcTimeout);
        ToProto(req->mutable_node_info(), *NodeInfo_);
        ToProto(req->mutable_worker_groups(), Groups_);

        // Serialize worker capabilities
        for (const auto& [key, value] : Capabilities_) {
            auto* capability = req->add_worker_capabilities();
            capability->set_key(key);
            capability->set_value(value);
        }

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        YT_TLOG_DEBUG("Handshake succeeded");

        ConnectionIncarnationId_ = FromProto<TIncarnationId>(rsp->connection_incarnation_id());
        HeartbeatSeqNo_ = 0;
    }

    void OnConnected()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        Connected_ = true;
        ConnectionTime_ = TInstant::Now();

        YT_TLOG_INFO("Worker connected")
            .With("RpcAddress", NodeInfo_->RpcAddress)
            .With("IncarnationId", NodeInfo_->IncarnationId)
            .With("ConnectionIncarnationId", ConnectionIncarnationId_);

        HeartbeatExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TControllerConnector::SendHeartbeat, MakeWeak(this)),
            TPeriodicExecutorOptions::WithJitter(GetControllerConnectorSpec()->ControllerHeartbeatPeriod));
        HeartbeatExecutor_->Start();

        CheckControllerExecutor_ = New<TPeriodicExecutor>(
            CancelableControlInvoker_,
            BIND(&TControllerConnector::CheckController, MakeWeak(this)),
            TPeriodicExecutorOptions::WithJitter(GetControllerConnectorSpec()->ControllerDiscoverPeriod));
        CheckControllerExecutor_->Start();

        ControllerConnected_.Fire(ConnectionIncarnationId_);
    }

    void DoDisconnect(const TError& error) noexcept
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        TForbidContextSwitchGuard contextSwitchGuard;

        if (Connected_) {
            YT_TLOG_WARNING("Disconnecting controller")
                .With(error);

            ControllerDisconnected_.Fire();

            YT_TLOG_WARNING("Controller disconnected");
        }

        DoCleanup();

        ScheduleConnect(true);
    }

    void DoCleanup()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (CancelableContext_) {
            CancelableContext_->Cancel(TError("Controller disconnected"));
            CancelableContext_.Reset();
        }

        if (HeartbeatExecutor_) {
            YT_UNUSED_FUTURE(HeartbeatExecutor_->Stop());
            HeartbeatExecutor_.Reset();
        }
        if (CheckControllerExecutor_) {
            YT_UNUSED_FUTURE(CheckControllerExecutor_->Stop());
            CheckControllerExecutor_.Reset();
        }
        if (LastHeartbeatTime_ && (LastHeartbeatTime_ + GetControllerConnectorSpec()->ControllerWaitTimeout < TInstant::Now())) {
            YT_TLOG_WARNING("Cancelling all jobs, too long without successful heartbeats")
                .With("LastHeartbeatTime", LastHeartbeatTime_)
                .With("ControllerWaitTimeout", GetControllerConnectorSpec()->ControllerWaitTimeout);
            JobTracker_->CancelAllJobs(TError(EErrorCode::AbandonedJob,
                "Job is abandoned: too long without successful controller heartbeats")
                << TErrorAttribute("last_heartbeat_time", LastHeartbeatTime_)
                << TErrorAttribute("controller_wait_timeout", GetControllerConnectorSpec()->ControllerWaitTimeout));
        }

        Connected_ = false;
        ConnectionTime_ = TInstant::Zero();
        ConnectionIncarnationId_ = {};
        HeartbeatSeqNo_ = 0;
        ControllerChannel_.Reset();
        AtomicControllerChannel_.Reset();
        CancelableControlInvoker_.Reset();
    }

    TWorkerTrackerServiceProxy::TReqHeartbeatPtr PrepareHeartbeatRequest(i64 heartbeatSeqNo)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        TEventTimerGuard timerGuard(PrepareHeartbeatRequestTimer_);

        TWorkerTrackerServiceProxy proxy(ControllerChannel_);
        auto request = proxy.Heartbeat();
        request->SetRequestCodec(NCompression::ECodec::Lz4);
        request->SetResponseCodec(NCompression::ECodec::Lz4);
        request->SetTimeout(GetControllerConnectorSpec()->ControllerHeartbeatRpcTimeout);
        request->set_worker_rpc_address(NodeInfo_->RpcAddress);
        ToProto(request->mutable_connection_incarnation_id(), ConnectionIncarnationId_);
        request->set_heartbeat_seq_no(heartbeatSeqNo);
        request->set_execution_spec_versions(ToProto(ConvertToYsonString(BuildExecutionSpecVersions(ExecutionSpec_))));

        auto statuses = WaitFor(JobTracker_->GetStatuses())
            .ValueOrThrow();

        for (const auto& status : statuses) {
            auto* protoJob = request->add_running_jobs();
            ToProto(protoJob->mutable_job_id(), status->JobId);
            protoJob->set_job_status(ToProto(ConvertToYsonString(status)));
        }

        auto workerStatus = New<TWorkerStatus>();
        if (auto error = GetPreviousCrashError(); TInstant::Now() < error.GetDatetime() + YoungWorkingJobThreshold) {
            workerStatus->PreviousCrashError = error;
        }
        workerStatus->MessageDistributorStatus = MessageDistributor_->GetStatus();
        workerStatus->Errors = std::move(RootStatusProfiler_->GetStatus().Errors);
        workerStatus->ResourceStatuses = JobTracker_->GetResourceStatuses();
        workerStatus->PreloadedResourceStates = JobTracker_->GetPreloadedStates();
        request->set_worker_status(ToProto(ConvertToYsonString(workerStatus)));

        return request;
    }

    void HandleHeartbeatResponse(const TWorkerTrackerServiceProxy::TRspHeartbeatPtr& response)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        TEventTimerGuard timerGuard(ProcessHeartbeatResponseTimer_);

        // Log FlowCoreTarget mismatch on transitions: this worker is being excluded from scheduling.
        if (response->has_flow_core_target_mismatch()) {
            const auto& target = response->flow_core_target_mismatch();
            if (LoggedFlowCoreTargetMismatch_ != target) {
                YT_TLOG_WARNING("Worker is excluded from scheduling: FlowCoreVersion does not match FlowCoreTarget")
                    .With("FlowCoreTarget", target)
                    .With("FlowCoreVersion", NodeInfo_->FlowCoreVersion);
                LoggedFlowCoreTargetMismatch_ = target;
            }
        } else if (LoggedFlowCoreTargetMismatch_.has_value()) {
            YT_TLOG_INFO("Worker FlowCoreVersion now matches FlowCoreTarget, exclusion cleared")
                .With("FlowCoreVersion", NodeInfo_->FlowCoreVersion);
            LoggedFlowCoreTargetMismatch_.reset();
        }

        if (response->execution_spec().length() != 0) {
            auto executionSpecUpdate = ConvertTo<TExecutionSpecPtr>(
                ConvertToNode(TYsonStringBuf(response->execution_spec())));
            std::vector<TPersistedStateStorageRow<std::string>> stateUpdate;
            stateUpdate.resize(response->state_update_size());
            for (int i = 0; i < response->state_update_size(); ++i) {
                stateUpdate[i].SequenceId = TSequenceId{response->state_update(i).sequence_id()};
                stateUpdate[i].Name = response->state_update(i).name();
                stateUpdate[i].KeyLeft = response->state_update(i).key_left();
                stateUpdate[i].KeyRight = response->state_update(i).key_right();
                stateUpdate[i].Value = response->state_update(i).value();
            }

            THashMap<TJobId, NYTree::IMapNodePtr> dynamicComputationPartitionSpecs(response->jobs_dynamic_computation_partition_specs_size());
            for (int i = 0; i < response->jobs_dynamic_computation_partition_specs_size(); ++i) {
                const auto& item = response->jobs_dynamic_computation_partition_specs(i);
                dynamicComputationPartitionSpecs[FromProto<TJobId>(item.job_id())] = ConvertTo<IMapNodePtr>(TYsonStringBuf(item.spec()));
            }

            auto newExecutionSpec = ApplyExecutionSpecUpdate(ExecutionSpec_, executionSpecUpdate, stateUpdate);
            Reconfigure(newExecutionSpec, dynamicComputationPartitionSpecs);
        }

        if (!response->message_transfering_info().empty()) {
            auto messageTransferingInfo = ConvertTo<TMessageTransferingInfoPtr>(TYsonStringBuf(response->message_transfering_info()));
            MessageDistributor_->UpdateMessageTransferingInfo(messageTransferingInfo);
            JobTracker_->UpdateMessageTransferingInfo(messageTransferingInfo);
        }
    }

    void SendHeartbeat()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto traceContext = TTraceContext::NewRoot("YTFlow.Worker.SendHeartbeat");
        TTraceContextGuard traceGuard(traceContext);

        YT_TLOG_DEBUG("Preparing heartbeat request");
        auto incarnationId = ConnectionIncarnationId_;
        HeartbeatSeqNo_ += 1;
        auto heartbeatSeqNo = HeartbeatSeqNo_;

        auto request = PrepareHeartbeatRequest(heartbeatSeqNo);

        auto start = TInstant::Now();
        auto rspOrError = WaitFor(request->Invoke());
        WaitHeartbeatResponseTimer_.Record(TInstant::Now() - start);

        if (ConnectionIncarnationId_ != incarnationId || heartbeatSeqNo != HeartbeatSeqNo_) {
            YT_TLOG_DEBUG("Heartbeat is not actual anymore");
            return;
        }

        if (rspOrError.IsOK()) {
            TForbidContextSwitchGuard contextSwitchGuard;

            LastHeartbeatTime_ = TInstant::Now();
            YT_TLOG_DEBUG("Heartbeat succeeded");
            HandleHeartbeatResponse(rspOrError.Value());
            YT_TLOG_DEBUG("Heartbeat response handled");
        } else {
            // Tolerate transient errors: keep retrying with backoff while we are within the controller
            // wait window, and disconnect only once the error is non-retriable or the window is exhausted.
            // The window is measured from the last successful heartbeat, or from the connection time for a
            // freshly (re)connected worker that has not completed a heartbeat yet, so that a controller
            // leadership change does not make the whole fleet disconnect and re-handshake on the first error.
            auto lastProgressTime = std::max(LastHeartbeatTime_, ConnectionTime_);
            if (NRpc::IsRetriableError(rspOrError) && TInstant::Now() < lastProgressTime + GetControllerConnectorSpec()->ControllerWaitTimeout) {
                YT_TLOG_WARNING("Error reporting heartbeat to controller")
                    .With(rspOrError);
                TDelayedExecutor::WaitForDuration(GetControllerConnectorSpec()->ControllerHeartbeatFailureBackoff);
            } else {
                Disconnect(rspOrError);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IControllerConnectorPtr CreateControllerConnector(
    TNodeInfoPtr nodeInfo,
    std::vector<std::string> groups,
    THashMap<std::string, ssize_t> capabilities,
    ICommonYTConnectorPtr ytConnector,
    IInvokerPtr controlInvoker,
    IChannelFactoryPtr channelFactory,
    IMessageDistributorPtr messageDistributor,
    IJobDirectoryPtr jobDirectory,
    IJobTrackerPtr jobTracker,
    TStreamSpecStoragePtr streamSpecStorage,
    bool ignoreSingletonsDynamicConfig,
    IStatusProfilerPtr rootStatusProfiler)
{
    return New<TControllerConnector>(
        std::move(nodeInfo),
        std::move(groups),
        std::move(capabilities),
        std::move(ytConnector),
        std::move(controlInvoker),
        std::move(channelFactory),
        std::move(messageDistributor),
        std::move(jobDirectory),
        std::move(jobTracker),
        std::move(streamSpecStorage),
        ignoreSingletonsDynamicConfig,
        std::move(rootStatusProfiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
