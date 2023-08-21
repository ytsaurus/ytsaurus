#include "cell_tracker.h"

#include "bootstrap.h"
#include "cell_tracker_impl.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NCellBalancer {

using namespace NConcurrency;
using namespace NCellMaster;
using namespace NTabletServer;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TCellTracker
    : public ICellTracker
{
public:
    TCellTracker(IBootstrap* bootstrap, TCellBalancerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
    { }

    void Start() override
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        StartTime_ = TInstant::Now();

        CellTrackerImpl_ = New<TCellTrackerImpl>(Bootstrap_, StartTime_, Config_->TabletManager);

        YT_VERIFY(!PeriodicExecutor_);
        PeriodicExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TCellTracker::ScanCells, MakeWeak(this)),
            GetTabletManagerConfig()->CellScanPeriod);
        PeriodicExecutor_->Start();
    }

    NYTree::IYPathServicePtr CreateOrchidService() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND(&TCellTracker::BuildOrchid, MakeStrong(this)))
            ->Via(Bootstrap_->GetControlInvoker());
    }

private:
    IBootstrap* const Bootstrap_;
    const TCellBalancerConfigPtr Config_;

    TCellTrackerImplPtr CellTrackerImpl_;

    TInstant StartTime_;
    TPeriodicExecutorPtr PeriodicExecutor_;

    const TDynamicTabletManagerConfigPtr& GetTabletManagerConfig()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Config_->TabletManager;
    }

    void ScanCells()
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        YT_PROFILE_TIMING("/cell_balancer/scan_cells") {
            CellTrackerImpl_->ScanCells();
        }
    }

    bool IsLeader()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetElectionManager()->IsLeader();
    }

    void BuildOrchid(IYsonConsumer* consumer)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("service").BeginMap()
                    .Item("connected").Value(IsLeader())
                .EndMap()
                .Item("config").Value(Config_)
            .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

ICellTrackerPtr CreateCellTracker(IBootstrap* bootstrap, TCellBalancerConfigPtr config)
{
    return New<TCellTracker>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
