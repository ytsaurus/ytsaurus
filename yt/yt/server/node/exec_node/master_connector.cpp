#include "master_connector.h"

#include "bootstrap.h"
#include "job_controller.h"
#include "private.h"
#include "slot_location.h"
#include "slot_manager.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/ytlib/exec_node_tracker_client/exec_node_tracker_service_proxy.h>

#include <yt/yt/core/concurrency/retrying_periodic_executor.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NExecNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NExecNodeTrackerClient;
using namespace NExecNodeTrackerClient::NProto;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public IMasterConnector
{
public:
    TMasterConnector(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , DynamicConfig_(New<TMasterConnectorDynamicConfig>())
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
    }

    void Initialize() override
    {
        Bootstrap_->SubscribeMasterConnected(BIND(&TMasterConnector::OnMasterConnected, MakeWeak(this)));
    }

    void OnDynamicConfigChanged(
        const TMasterConnectorDynamicConfigPtr& /*oldConfig*/,
        const TMasterConnectorDynamicConfigPtr& newConfig) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DynamicConfig_ = newConfig;

        //! NB(arkady-e1ppa): HeartbeatExecutor is created once OnMasterRegistrationSignal is fired.
        //! This happens after the first time DynamicConfigManager applies dynamic config.
        //! Therefore we must be ready to encounter null HeartbeatExecutor_.
        if (HeartbeatExecutor_) {
            HeartbeatExecutor_->SetOptions(DynamicConfig_->HeartbeatExecutor);
        }
    }

    TReqHeartbeat GetHeartbeatRequest() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Bootstrap_->IsConnected());

        TReqHeartbeat heartbeat;

        heartbeat.set_node_id(ToProto<ui32>(Bootstrap_->GetNodeId()));

        auto* statistics = heartbeat.mutable_statistics();
        const auto& slotManager = Bootstrap_->GetSlotManager();
        for (const auto& location : slotManager->GetLocations()) {
            auto* locationStatistics = statistics->add_slot_locations();
            *locationStatistics = location->GetSlotLocationStatistics();

            // Slot location statistics might be not computed yet, so we set medium index separately.
            locationStatistics->set_medium_index(location->GetMediumDescriptor().Index);
        }

        if (auto buildInfo = Bootstrap_->GetJobController()->GetBuildInfo()) {
            heartbeat.set_job_proxy_build_version(buildInfo->Version);
        }

        return heartbeat;
    }

    void OnHeartbeatResponse(const TRspHeartbeat& response)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        bool disableSchedulerJobs = response.disable_scheduler_jobs() || Bootstrap_->IsDecommissioned();
        Bootstrap_->GetJobController()->SetJobsDisabledByMaster(disableSchedulerJobs);
    }

private:
    IBootstrap* const Bootstrap_;

    TMasterConnectorDynamicConfigPtr DynamicConfig_;

    TRetryingPeriodicExecutorPtr HeartbeatExecutor_;
    TFuture<void> OldHeartbeatExecutorStoppedEvent_;

    IInvokerPtr HeartbeatInvoker_;

    void OnMasterConnected(TNodeId /*nodeId*/)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (OldHeartbeatExecutorStoppedEvent_ &&
            !OldHeartbeatExecutorStoppedEvent_.IsSet())
        {
            YT_LOG_DEBUG("Waiting for the old heartbeat executor to stop.");
            auto error = WaitFor(std::move(OldHeartbeatExecutorStoppedEvent_));

            YT_LOG_FATAL_UNLESS(
                error.IsOK(),
                error,
                "Unexpected failure while waiting for old heartbeat executor to shut down.");
        }

        //! MasterConnectionInvoker changes after every registration
        //! and so we have to make a new HeartbeatExecutor.
        //! Technically, we could support "UpdateInvoker" method,
        //! but there is no reason to preserve HeartbeatExecutor's state.
        HeartbeatExecutor_ = New<TRetryingPeriodicExecutor>(
            Bootstrap_->GetMasterConnectionInvoker(),
            BIND([this_ = MakeWeak(this)] {
                return this_.Lock()->ReportHeartbeat();
            }),
            DynamicConfig_->HeartbeatExecutor);

        YT_LOG_INFO("Starting exec node heartbeats");
        HeartbeatExecutor_->Start();
    }

    TError ReportHeartbeat()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Exec node heartbeats are required at primary master only.
        auto masterChannel = Bootstrap_->GetMasterChannel(PrimaryMasterCellTagSentinel);
        TExecNodeTrackerServiceProxy proxy(masterChannel);

        auto req = proxy.Heartbeat();
        req->SetTimeout(GetDynamicConfig()->HeartbeatTimeout);

        static_cast<TReqHeartbeat&>(*req) = GetHeartbeatRequest();

        YT_LOG_INFO("Sending exec node heartbeat to master (%v)",
            req->statistics());

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            OnHeartbeatResponse(*rspOrError.Value());

            YT_LOG_INFO("Successfully reported exec node heartbeat to master");

            return TError();
        } else {
            YT_LOG_WARNING(rspOrError, "Error reporting exec node heartbeat to master");
            if (IsRetriableError(rspOrError)) {
                //! TODO(arkady-e1ppa): Maybe backoff in this case?
                return TError();
            } else {
                OldHeartbeatExecutorStoppedEvent_ = HeartbeatExecutor_->Stop();
                Bootstrap_->ResetAndRegisterAtMaster();
                return TError("Unretryable error received from master.");
            }
        }
    }

    TMasterConnectorDynamicConfigPtr GetDynamicConfig() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetDynamicConfigManager()->GetConfig()->ExecNode->MasterConnector;
    }

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap)
{
    return New<TMasterConnector>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
