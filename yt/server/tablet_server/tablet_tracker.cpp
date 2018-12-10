#include "tablet_tracker.h"
#include "tablet_tracker_impl_old.h"
#include "tablet_tracker_impl.h"
#include "private.h"
#include "config.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "tablet_manager.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/config_manager.h>
#include <yt/server/cell_master/config.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/node_tracker_server/node_tracker.h>

#include <yt/server/table_server/table_node.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NObjectServer;
using namespace NTabletServer::NProto;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletTracker::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

private:
    const TTabletManagerConfigPtr Config_;
    NCellMaster::TBootstrap* const Bootstrap_;
    const NProfiling::TProfiler Profiler;
    std::unique_ptr<TTabletTrackerImpl> TabletTrackerImpl_;

    TInstant StartTime_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    std::optional<bool> LastEnabled_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    bool IsEnabled();
    void ScanCells();
};

////////////////////////////////////////////////////////////////////////////////

TTabletTracker::TImpl::TImpl(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , Profiler("/tablet_server/cell_balancer")
{
    YCHECK(Config_);
    YCHECK(Bootstrap_);
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Default), AutomatonThread);
}

void TTabletTracker::TImpl::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    StartTime_ = TInstant::Now();

    TabletTrackerImpl_ = std::make_unique<TTabletTrackerImpl>(Config_, Bootstrap_, StartTime_);

    YCHECK(!PeriodicExecutor_);
    PeriodicExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletTracker),
        BIND(&TTabletTracker::TImpl::ScanCells, MakeWeak(this)),
        Config_->CellScanPeriod);
    PeriodicExecutor_->Start();
}

void TTabletTracker::TImpl::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (PeriodicExecutor_) {
        PeriodicExecutor_->Stop();
        PeriodicExecutor_.Reset();
    }

    TabletTrackerImpl_.reset();
}

bool TTabletTracker::TImpl::IsEnabled()
{
    // This method also logs state changes.

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();

    int needOnline = Config_->SafeOnlineNodeCount;
    int gotOnline = nodeTracker->GetOnlineNodeCount();

    if (gotOnline < needOnline) {
        if (!LastEnabled_ || *LastEnabled_) {
            LOG_INFO("Tablet tracker disabled: too few online nodes, needed >= %v but got %v",
                needOnline,
                gotOnline);
            LastEnabled_ = false;
        }
        return false;
    }

    if (!LastEnabled_ || !*LastEnabled_) {
        LOG_INFO("Tablet tracker enabled");
        LastEnabled_ = true;
    }

    return true;
}

void TTabletTracker::TImpl::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!IsEnabled())
        return;

    PROFILE_TIMING("/scan_cells") {
        const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->TabletCellBalancer;
        if (config->EnableTabletCellBalancer) {
            TabletTrackerImpl_->ScanCells();
        } else {
            TTabletTrackerImplOld(Config_, Bootstrap_, StartTime_)
                .ScanCells();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TTabletTracker::TTabletTracker(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

void TTabletTracker::Start()
{
    Impl_->Start();
}

void TTabletTracker::Stop()
{
    Impl_->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
