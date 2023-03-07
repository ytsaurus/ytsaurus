#include "cell_tracker.h"
#include "cell_tracker_impl_old.h"
#include "cell_tracker_impl.h"
#include "private.h"
#include "config.h"
#include "cell_base.h"
#include "cell_bundle.h"
#include "tamed_cell_manager.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/server/master/table_server/table_node.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NObjectServer;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellTracker::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const NProfiling::TProfiler Profiler;
    TIntrusivePtr<TCellTrackerImpl> CellTrackerImpl_;

    TInstant StartTime_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    std::optional<bool> LastEnabled_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void OnDynamicConfigChanged();
    const TDynamicCellManagerConfigPtr& GetDynamicConfig();
    bool IsEnabled();
    void ScanCells();
};

////////////////////////////////////////////////////////////////////////////////

TCellTracker::TImpl::TImpl(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Profiler("/cell_server/cell_balancer")
{
    YT_VERIFY(Bootstrap_);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));
}

void TCellTracker::TImpl::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    StartTime_ = TInstant::Now();

    CellTrackerImpl_ = New<TCellTrackerImpl>(Bootstrap_, StartTime_);

    YT_VERIFY(!PeriodicExecutor_);
    PeriodicExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletTracker),
        BIND(&TCellTracker::TImpl::ScanCells, MakeWeak(this)),
        GetDynamicConfig()->CellScanPeriod);
    PeriodicExecutor_->Start();
}

void TCellTracker::TImpl::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (PeriodicExecutor_) {
        PeriodicExecutor_->Stop();
        PeriodicExecutor_.Reset();
    }

    CellTrackerImpl_.Reset();
}

void TCellTracker::TImpl::OnDynamicConfigChanged()
{
    if (PeriodicExecutor_) {
        PeriodicExecutor_->SetPeriod(GetDynamicConfig()->CellScanPeriod);
    }
}

const NTabletServer::TDynamicTabletManagerConfigPtr& TCellTracker::TImpl::GetDynamicConfig()
{
    return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
}

bool TCellTracker::TImpl::IsEnabled()
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

void TCellTracker::TImpl::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!IsEnabled())
        return;

    PROFILE_TIMING("/scan_cells") {
        const auto& config = GetDynamicConfig()->TabletCellBalancer;
        if (config->EnableTabletCellBalancer) {
            CellTrackerImpl_->ScanCells();
        } else {
            TCellTrackerImplOld(Bootstrap_, StartTime_)
                .ScanCells();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TCellTracker::TCellTracker(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TCellTracker::~TCellTracker()
{ }

void TCellTracker::Start()
{
    Impl_->Start();
}

void TCellTracker::Stop()
{
    Impl_->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
