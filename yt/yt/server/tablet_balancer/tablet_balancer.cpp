#include "action_manager.h"
#include "bootstrap.h"
#include "bundle_state.h"
#include "config.h"
#include "dynamic_config_manager.h"
#include "private.h"
#include "public.h"
#include "tablet_balancer.h"

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

static const TString TabletCellBundlesPath("//sys/tablet_cell_bundles");

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancer
    :  public ITabletBalancer
{
public:
    TTabletBalancer(
        IBootstrap* bootstrap,
        TStandaloneTabletBalancerConfigPtr config,
        IInvokerPtr controlInvoker);

    void Start() override;
    void Stop() override;

    IYPathServicePtr GetOrchidService() override;

    void OnDynamicConfigChanged(
        const TTabletBalancerDynamicConfigPtr& oldConfig,
        const TTabletBalancerDynamicConfigPtr& newConfig) override;

private:
    IBootstrap* const Bootstrap_;
    const TStandaloneTabletBalancerConfigPtr Config_;
    const IInvokerPtr ControlInvoker_;
    const TPeriodicExecutorPtr PollExecutor_;
    THashMap<TString, TBundleStatePtr> Bundles_;
    TThreadPoolPtr WorkerPool_;
    IActionManagerPtr ActionManager_;

    std::atomic<bool> Enable_{false};
    std::atomic<bool> EnableEverywhere_{false};

    i64 IterationIndex_;

    void BalancerIteration();

    bool IsBalancingAllowed(const TBundleStatePtr& bundle) const;

    void BalanceViaReshard(const TBundleStatePtr& bundle) const;
    void BalanceViaMove(const TBundleStatePtr& bundle) const;

    void UpdateBundleList();

    void BuildOrchid(IYsonConsumer* consumer) const;
};

////////////////////////////////////////////////////////////////////////////////

TTabletBalancer::TTabletBalancer(
    IBootstrap* bootstrap,
    TStandaloneTabletBalancerConfigPtr config,
    IInvokerPtr controlInvoker)
    : Bootstrap_(bootstrap)
    , Config_(std::move(config))
    , ControlInvoker_(std::move(controlInvoker))
    , PollExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TTabletBalancer::BalancerIteration, MakeWeak(this)),
        Config_->Period))
    , WorkerPool_(New<TThreadPool>(
        Config_->WorkerThreadPoolSize,
        "TabletBalancer"))
    , ActionManager_(CreateActionManager(
        Config_->TabletActionExpirationTime,
        Config_->TabletActionPollingPeriod,
        Bootstrap_->GetClient(),
        Bootstrap_))
    , IterationIndex_(0)
{
    bootstrap->GetDynamicConfigManager()->SubscribeConfigChanged(BIND(&TTabletBalancer::OnDynamicConfigChanged, MakeWeak(this)));
}

void TTabletBalancer::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Starting tablet balancer instance");

    PollExecutor_->Start();

    ActionManager_->Start(Bootstrap_->GetElectionManager()->GetPrerequisiteTransactionId());
}

void TTabletBalancer::Stop()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_INFO("Stopping tablet balancer instance");

    PollExecutor_->Stop();
    ActionManager_->Stop();

    YT_LOG_INFO("Tablet balancer instance stopped");
}

void TTabletBalancer::BalancerIteration()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    if (!Enable_) {
        YT_LOG_DEBUG("Standalone tablet balancer is not enabled");
        return;
    }

    YT_LOG_INFO("Balancer iteration (IterationIndex: %v)", IterationIndex_);

    YT_LOG_DEBUG("Started fetching bundles");
    UpdateBundleList();
    YT_LOG_DEBUG("Finished fetching bundles");

    for (auto& [bundleName, bundle] : Bundles_) {
        if (ActionManager_->HasUnfinishedActions(bundleName)) {
            YT_LOG_DEBUG("Skip balancing iteration since bundle has unfinished actions (BundleName: %v)", bundleName);
            continue;
        }

        YT_LOG_DEBUG("Started fetching (BundleName: %v)", bundleName);

        if (auto result = WaitFor(bundle->UpdateState()); !result.IsOK()) {
            YT_LOG_ERROR(result, "Failed to update meta registry (BundleName: %v)", bundleName);
            continue;
        }

        if (!IsBalancingAllowed(bundle)) {
            YT_LOG_DEBUG("Balancing is not allowed (BundleName: %v)", bundleName);
            continue;
        }

        if (auto result = WaitFor(bundle->FetchStatistics()); !result.IsOK()) {
            YT_LOG_ERROR(result, "Fetch statistics failed (BundleName: %v)", bundleName);
            continue;
        }

        // TODO(alexelex): Use Tablets as tablets for each table.

        if (IterationIndex_ % 2 != 0) {
            BalanceViaMove(bundle);
        } else {
            BalanceViaReshard(bundle);
        }

        ActionManager_->CreateActions(bundleName);
    }

    ++IterationIndex_;
}

bool TTabletBalancer::IsBalancingAllowed(const TBundleStatePtr& bundle) const
{
    return Enable_ &&
        bundle->GetHealth() == ETabletCellHealth::Good &&
        (EnableEverywhere_ ||
         bundle->GetBundle()->Config->EnableStandaloneTabletBalancer);
}

IYPathServicePtr TTabletBalancer::GetOrchidService()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    return IYPathService::FromProducer(BIND(&TTabletBalancer::BuildOrchid, MakeWeak(this)))
        ->Via(ControlInvoker_);
}

void TTabletBalancer::BuildOrchid(IYsonConsumer* consumer) const
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("config").Value(Config_)
        .EndMap();
}

void TTabletBalancer::OnDynamicConfigChanged(
    const TTabletBalancerDynamicConfigPtr& oldConfig,
    const TTabletBalancerDynamicConfigPtr& newConfig)
{
    // Order matters. Otherwise, the old Enable can be seen with the new EnableEverywhere
    // and balance everything, while EnableEverywhere has no effect if Enable is set to false.
    Enable_.store(newConfig->Enable);
    EnableEverywhere_.store(newConfig->EnableEverywhere);

    YT_LOG_DEBUG(
        "Updated tablet balancer dynamic config (OldConfig: %v, NewConfig: %v)",
        ConvertToYsonString(oldConfig, EYsonFormat::Text),
        ConvertToYsonString(newConfig, EYsonFormat::Text));
}

void TTabletBalancer::UpdateBundleList()
{
    TListNodeOptions options;
    options.Attributes = {"health", "tablet_balancer_config", "tablet_cell_ids"};

    auto bundles = WaitFor(Bootstrap_
        ->GetClient()
        ->ListNode(TabletCellBundlesPath, options))
        .ValueOrThrow();
    auto bundlesList = ConvertTo<IListNodePtr>(bundles);

    // Gather current bundles.
    THashSet<TString> currentBundles;
    for (const auto& bundle : bundlesList->GetChildren()) {
        const auto& name = bundle->AsString()->GetValue();
        currentBundles.insert(bundle->AsString()->GetValue());

        auto it = Bundles_.emplace(
            name,
            New<TBundleState>(
                name,
                Bootstrap_->GetClient(),
                WorkerPool_->GetInvoker())).first;
        it->second->UpdateBundleAttributes(&bundle->Attributes());
    }

    // Find bundles that are not in the list of bundles (probably deleted) and erase them.
    DropMissingKeys(&Bundles_, currentBundles);
}

void TTabletBalancer::BalanceViaMove(const TBundleStatePtr& bundle) const
{
    if (!bundle->GetBundle()->Config->EnableInMemoryCellBalancer) {
        return;
    }

    auto descriptors = ReassignInMemoryTablets(
        bundle->GetBundle(),
        /*movableTables*/ std::nullopt,
        /*ignoreTableWiseConfig*/ false,
        Logger);

    int actionCount = 0;

    if (!descriptors.empty()) {
        for (auto descriptor : descriptors) {
            YT_LOG_DEBUG("Move action created (TabletId: %v, TabletCellId: %v)",
                descriptor.TabletId,
                descriptor.TabletCellId);
            ActionManager_->ScheduleActionCreation(bundle->GetBundle()->Name, descriptor);
        }

        actionCount += std::ssize(descriptors);
    }

    YT_LOG_DEBUG("Balance tablets via move finished (ActionCount: %v)", actionCount);
}

void TTabletBalancer::BalanceViaReshard(const TBundleStatePtr& bundle) const
{
    std::vector<TTabletPtr> tablets;
    for (const auto& [id, tablet] : bundle->Tablets()) {
        if (IsTabletReshardable(tablet, /*ignoreConfig*/ false)) {
            tablets.push_back(tablet);
        }
    }

    std::sort(
        tablets.begin(),
        tablets.end(),
        [&] (const TTabletPtr lhs, const TTabletPtr rhs) {
            return lhs->Table->Id < rhs->Table->Id;
        });

    int actionCount = 0;
    TTabletBalancerContext context;

    auto beginIt = tablets.begin();
    while (beginIt != tablets.end()) {
        auto endIt = beginIt;
        while (endIt != tablets.end() && (*beginIt)->Table == (*endIt)->Table) {
            ++endIt;
        }

        if (TypeFromId((*beginIt)->Table->Id) != EObjectType::Table) {
            beginIt = endIt;
            continue;
        }

        auto tabletRange = MakeRange(beginIt, endIt);
        beginIt = endIt;

        // TODO(alexelex): Check if the table has actions.

        auto descriptors = MergeSplitTabletsOfTable(
            tabletRange,
            &context,
            Logger);

        for (auto descriptor : descriptors) {
            YT_LOG_DEBUG("Reshard action created (TabletIds: %v, TabletCount: %v, DataSize: %v)",
                descriptor.Tablets,
                descriptor.TabletCount,
                descriptor.DataSize);
            ActionManager_->ScheduleActionCreation(bundle->GetBundle()->Name, descriptor);
        }
        actionCount += std::ssize(descriptors);
    }

    YT_LOG_DEBUG("Balance tablets via reshard finished (ActionCount: %v)", actionCount);
}

////////////////////////////////////////////////////////////////////////////////

ITabletBalancerPtr CreateTabletBalancer(
    IBootstrap* bootstrap,
    TStandaloneTabletBalancerConfigPtr config,
    IInvokerPtr controlInvoker)
{
    return New<TTabletBalancer>(bootstrap, config, std::move(controlInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
