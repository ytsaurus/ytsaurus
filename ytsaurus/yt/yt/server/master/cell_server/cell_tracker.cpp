#include "cell_tracker.h"
#include "cell_tracker_impl.h"
#include "private.h"
#include "config.h"
#include "cell_base.h"
#include "cell_bundle.h"
#include "tamed_cell_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/numeric_helpers.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NObjectServer;
using namespace NNodeTrackerServer;
using namespace NTabletServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellTracker
    : public ICellTracker
{
public:
    explicit TCellTracker(NCellMaster::TBootstrap* bootstrap);

    void Start() override;
    void Stop() override;

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    TIntrusivePtr<TCellTrackerImpl> CellTrackerImpl_;

    TInstant StartTime_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    std::optional<bool> LastEnabled_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldConfig);
    const TDynamicTabletManagerConfigPtr& GetDynamicConfig();
    bool IsEnabled();
    void ScanCells();
};

////////////////////////////////////////////////////////////////////////////////

TCellTracker::TCellTracker(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{
    YT_VERIFY(Bootstrap_);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(BIND(&TCellTracker::OnDynamicConfigChanged, MakeWeak(this)));
}

void TCellTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    StartTime_ = TInstant::Now();

    CellTrackerImpl_ = New<TCellTrackerImpl>(Bootstrap_, StartTime_);

    YT_VERIFY(!PeriodicExecutor_);
    PeriodicExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletTracker),
        BIND(&TCellTracker::ScanCells, MakeWeak(this)),
        GetDynamicConfig()->CellScanPeriod);
    PeriodicExecutor_->Start();
}

void TCellTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (PeriodicExecutor_) {
        PeriodicExecutor_->Stop();
        PeriodicExecutor_.Reset();
    }

    CellTrackerImpl_.Reset();
}

void TCellTracker::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    if (PeriodicExecutor_) {
        PeriodicExecutor_->SetPeriod(GetDynamicConfig()->CellScanPeriod);
    }
}

const NTabletServer::TDynamicTabletManagerConfigPtr& TCellTracker::GetDynamicConfig()
{
    return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
}

bool TCellTracker::IsEnabled()
{
    // This method also logs state changes.

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();

    int needOnline = GetDynamicConfig()->SafeOnlineNodeCount;
    int gotOnline = nodeTracker->GetOnlineNodeCount();

    if (gotOnline < needOnline) {
        if (!LastEnabled_ || *LastEnabled_) {
            YT_LOG_INFO("Cell tracker disabled: too few online nodes, needed >= %v but got %v",
                needOnline,
                gotOnline);
            LastEnabled_ = false;
        }
        return false;
    }

    if (!LastEnabled_ || !*LastEnabled_) {
        YT_LOG_INFO("Cell tracker enabled");
        LastEnabled_ = true;
    }

    return true;
}

void TCellTracker::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!IsEnabled()) {
        return;
    }

    YT_PROFILE_TIMING("/cell_server/cell_balancer/scan_cells") {
        CellTrackerImpl_->ScanCells();
    }
}

////////////////////////////////////////////////////////////////////////////////

ICellTrackerPtr CreateCellTracker(NCellMaster::TBootstrap* bootstrap)
{
    return New<TCellTracker>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
