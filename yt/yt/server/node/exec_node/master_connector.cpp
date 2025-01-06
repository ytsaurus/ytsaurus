#include "master_connector.h"

#include "bootstrap.h"
#include "job_controller.h"
#include "private.h"
#include "slot_location.h"
#include "slot_manager.h"

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>
#include <yt/yt/server/node/cluster_node/master_heartbeat_reporter_base.h>

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
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public IMasterConnector
    , public TMasterHeartbeatReporterBase
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(), MasterConnected);
    DEFINE_SIGNAL_OVERRIDE(void(), MasterDisconnected);

public:
    TMasterConnector(IBootstrap* bootstrap)
        : TMasterHeartbeatReporterBase(
            bootstrap,
            /*reportHeartbeatsToAllSecondaryMasters*/ false,
            ExecNodeLogger().WithTag("HeartbeatType: %v", ENodeHeartbeatType::Exec))
        , Bootstrap_(bootstrap)
        , DynamicConfig_(New<TMasterConnectorDynamicConfig>())
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);
    }

    void Initialize() override
    {
        TMasterHeartbeatReporterBase::Initialize();

        Bootstrap_->SubscribeMasterConnected(BIND_NO_PROPAGATE(&TMasterConnector::OnMasterConnected, MakeWeak(this)));
        Bootstrap_->SubscribeMasterDisconnected(BIND_NO_PROPAGATE(&TMasterConnector::OnMasterDisconnected, MakeWeak(this)));

        Reconfigure(DynamicConfig_->HeartbeatExecutor);
    }

    void OnDynamicConfigChanged(
        const TMasterConnectorDynamicConfigPtr& /*oldConfig*/,
        const TMasterConnectorDynamicConfigPtr& newConfig) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        DynamicConfig_ = newConfig;

        Reconfigure(DynamicConfig_->HeartbeatExecutor);

        YT_LOG_INFO("Dynamic config changed");
    }

    TExecNodeTrackerServiceProxy::TReqHeartbeatPtr BuildHeartbeatRequest(TCellTag cellTag) const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(Bootstrap_->IsConnected());

        auto masterChannel = Bootstrap_->GetMasterChannel(cellTag);
        TExecNodeTrackerServiceProxy proxy(std::move(masterChannel));

        auto heartbeat = proxy.Heartbeat();
        heartbeat->SetTimeout(GetDynamicConfig()->HeartbeatTimeout);

        heartbeat->set_node_id(ToProto(Bootstrap_->GetNodeId()));

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

protected:
    TFuture<void> DoReportHeartbeat(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto req = BuildHeartbeatRequest(cellTag);
        auto rspFuture = req->Invoke();
        EmplaceOrCrash(CellTagToHeartbeatRspFuture_, cellTag, rspFuture);
        return rspFuture.AsVoid();
    }

    void OnHeartbeatSucceeded(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto rspOrError = GetHeartbeatResponseOrError(cellTag);
        YT_VERIFY(rspOrError.IsOK());

        OnHeartbeatSucceeded(rspOrError.Value());
    }

    void OnHeartbeatFailed(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto rspOrError = GetHeartbeatResponseOrError(cellTag);
        YT_VERIFY(!rspOrError.IsOK());
    }

    void ResetState(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        CellTagToHeartbeatRspFuture_.erase(cellTag);
    }

private:
    IBootstrap* const Bootstrap_;

    TMasterConnectorDynamicConfigPtr DynamicConfig_;

    THashMap<TCellTag, TFuture<TExecNodeTrackerServiceProxy::TRspHeartbeatPtr>> CellTagToHeartbeatRspFuture_;

    void OnHeartbeatSucceeded(const TExecNodeTrackerServiceProxy::TRspHeartbeatPtr& response)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        bool disableSchedulerJobs = response->disable_scheduler_jobs() || Bootstrap_->IsDecommissioned();
        Bootstrap_->GetJobController()->SetJobsDisabledByMaster(disableSchedulerJobs);
    }

    void OnMasterConnected(TNodeId /*nodeId*/)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        StartNodeHeartbeats();

        MasterConnected_.Fire();
    }

    void OnMasterDisconnected()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        MasterDisconnected_.Fire();
    }

    TMasterConnectorDynamicConfigPtr GetDynamicConfig() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetDynamicConfigManager()->GetConfig()->ExecNode->MasterConnector;
    }

    TErrorOr<TExecNodeTrackerServiceProxy::TRspHeartbeatPtr> GetHeartbeatResponseOrError(TCellTag cellTag)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto futureIt = GetIteratorOrCrash(CellTagToHeartbeatRspFuture_, cellTag);
        auto future = std::move(futureIt->second);
        CellTagToHeartbeatRspFuture_.erase(futureIt);
        YT_VERIFY(future.IsSet());

        return future.Get();
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
