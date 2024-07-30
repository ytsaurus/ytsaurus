#include "master_connector.h"

#include "bootstrap.h"
#include "job_controller.h"
#include "private.h"
#include "slot_location.h"
#include "slot_manager.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>
#include <yt/yt/server/node/cluster_node/master_heartbeat_reporter.h>
#include <yt/yt/server/node/cluster_node/master_heartbeat_reporter_callbacks.h>

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

static constexpr auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public IMasterConnector
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(), MasterConnected);
    DEFINE_SIGNAL_OVERRIDE(void(), MasterDisconnected);

public:
    TMasterConnector(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , DynamicConfig_(New<TMasterConnectorDynamicConfig>())
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
    }

    void Initialize() override
    {
        Bootstrap_->SubscribeMasterConnected(BIND_NO_PROPAGATE(&TMasterConnector::OnMasterConnected, MakeWeak(this)));
        Bootstrap_->SubscribeMasterDisconnected(BIND_NO_PROPAGATE(&TMasterConnector::OnMasterDisconnected, MakeWeak(this)));

        const auto& heartbeatLogger = Logger().WithTag("HeartbeatType: Exec");
        HeartbeatReporter_ = CreateMasterHeartbeatReporter(
            Bootstrap_,
            /*reportHeartbeatsToAllSecondaryMasters*/ false,
            CreateSingleFlavorHeartbeatCallbacks<TMasterConnector, TExecNodeTrackerServiceProxy>(MakeWeak(this), heartbeatLogger),
            DynamicConfig_->HeartbeatExecutor,
            heartbeatLogger);
        HeartbeatReporter_->Initialize();
    }

    void OnDynamicConfigChanged(
        const TMasterConnectorDynamicConfigPtr& /*oldConfig*/,
        const TMasterConnectorDynamicConfigPtr& newConfig) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DynamicConfig_ = newConfig;

        HeartbeatReporter_->Reconfigure(DynamicConfig_->HeartbeatExecutor);

        YT_LOG_DEBUG("Dynamic config changed");
    }

    TExecNodeTrackerServiceProxy::TReqHeartbeatPtr BuildHeartbeatRequest(TCellTag cellTag) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Bootstrap_->IsConnected());

        auto masterChannel = Bootstrap_->GetMasterChannel(cellTag);
        TExecNodeTrackerServiceProxy proxy(std::move(masterChannel));

        auto heartbeat = proxy.Heartbeat();
        heartbeat->SetTimeout(GetDynamicConfig()->HeartbeatTimeout);

        heartbeat->set_node_id(ToProto<ui32>(Bootstrap_->GetNodeId()));

        auto* statistics = heartbeat->mutable_statistics();
        const auto& slotManager = Bootstrap_->GetSlotManager();
        for (const auto& location : slotManager->GetLocations()) {
            auto* locationStatistics = statistics->add_slot_locations();
            *locationStatistics = location->GetSlotLocationStatistics();

            // Slot location statistics might be not computed yet, so we set medium index separately.
            locationStatistics->set_medium_index(location->GetMediumDescriptor().Index);
        }

        if (auto buildInfo = Bootstrap_->GetJobController()->GetBuildInfo()) {
            heartbeat->set_job_proxy_build_version(buildInfo->Version);
        }

        return heartbeat;
    }

    void OnHeartbeatSucceeded(TCellTag /*cellTag*/, const TExecNodeTrackerServiceProxy::TRspHeartbeatPtr& response)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        bool disableSchedulerJobs = response->disable_scheduler_jobs() || Bootstrap_->IsDecommissioned();
        Bootstrap_->GetJobController()->SetJobsDisabledByMaster(disableSchedulerJobs);
    }

private:
    IBootstrap* const Bootstrap_;

    TMasterConnectorDynamicConfigPtr DynamicConfig_;

    IMasterHeartbeatReporterPtr HeartbeatReporter_;

    void OnMasterConnected(TNodeId /*nodeId*/)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        HeartbeatReporter_->StartHeartbeats();

        MasterConnected_.Fire();
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        MasterDisconnected_.Fire();
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
