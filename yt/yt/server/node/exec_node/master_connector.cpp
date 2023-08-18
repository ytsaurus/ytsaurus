#include "master_connector.h"

#include "bootstrap.h"
#include "job_controller.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/exec_node/slot_location.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/ytlib/exec_node_tracker_client/exec_node_tracker_service_proxy.h>

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
        , Config_(Bootstrap_->GetConfig()->ExecNode->MasterConnector)
        , HeartbeatPeriod_(Config_->HeartbeatPeriod)
        , HeartbeatPeriodSplay_(Config_->HeartbeatPeriodSplay)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
    }

    void Initialize() override
    {
        Bootstrap_->SubscribeMasterConnected(BIND(&TMasterConnector::OnMasterConnected, MakeWeak(this)));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TMasterConnector::OnDynamicConfigChanged, MakeWeak(this)));
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
        Bootstrap_->GetJobController()->SetDisableSchedulerJobs(disableSchedulerJobs);
    }

private:
    IBootstrap* const Bootstrap_;

    const TMasterConnectorConfigPtr Config_;

    IInvokerPtr HeartbeatInvoker_;

    TDuration HeartbeatPeriod_;
    TDuration HeartbeatPeriodSplay_;

    void OnMasterConnected(TNodeId /*nodeId*/)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        HeartbeatInvoker_ = Bootstrap_->GetMasterConnectionInvoker();

        StartHeartbeats();
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        HeartbeatPeriod_ = newNodeConfig->ExecNode->MasterConnector->HeartbeatPeriod.value_or(Config_->HeartbeatPeriod);
        HeartbeatPeriodSplay_ = newNodeConfig->ExecNode->MasterConnector->HeartbeatPeriodSplay.value_or(Config_->HeartbeatPeriodSplay);
    }

    void StartHeartbeats()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Starting exec node heartbeats");
        ScheduleHeartbeat(/* immediately */ true);
    }

    void ScheduleHeartbeat(bool immediately)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto delay = immediately ? TDuration::Zero() : HeartbeatPeriod_ + RandomDuration(HeartbeatPeriodSplay_);
        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::ReportHeartbeat, MakeWeak(this)),
            delay,
            HeartbeatInvoker_);
    }

    void ReportHeartbeat()
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

            // Schedule next heartbeat.
            ScheduleHeartbeat(/* immediately */ false);
        } else {
            YT_LOG_WARNING(rspOrError, "Error reporting exec node heartbeat to master");
            if (IsRetriableError(rspOrError)) {
                ScheduleHeartbeat(/* immediately*/ false);
            } else {
                Bootstrap_->ResetAndRegisterAtMaster();
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
