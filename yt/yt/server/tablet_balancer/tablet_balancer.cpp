#include "bootstrap.h"
#include "bundle_state.h"
#include "config.h"
#include "private.h"
#include "public.h"
#include "tablet_balancer.h"

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

private:
    IBootstrap* const Bootstrap_;
    const TStandaloneTabletBalancerConfigPtr Config_;
    const IInvokerPtr ControlInvoker_;
    const TPeriodicExecutorPtr PollExecutor_;
    THashMap<TString, TBundleStatePtr> Bundles_;
    TThreadPoolPtr WorkerPool_;

    i64 IterationIndex_;

    void FetchTablets();
    void BalancerIteration();

    void BalanceViaReshard(const TBundleStatePtr& bundle);
    void BalanceViaMove(const TBundleStatePtr& bundle);

    void BalanceBundle(const TBundleState& bundle);
    void UpdateBundleList();

    void DoStop();

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
    , IterationIndex_(0)
{ }

void TTabletBalancer::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Starting tablet balancer instance");

    PollExecutor_->Start();
}

void TTabletBalancer::Stop()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Stopping tablet balancer instance");

    ControlInvoker_->Invoke(BIND(&TTabletBalancer::DoStop, MakeWeak(this)));
}

void TTabletBalancer::FetchTablets()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    // TODO(alexelex): remove debug list request

    auto result = WaitFor(Bootstrap_
        ->GetMasterClient()
        ->ListNode("//sys"))
        .ValueOrThrow();

    YT_LOG_INFO("yt list //sys %v", result);
}

void TTabletBalancer::BalancerIteration()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_INFO("Balancer iteration (IterationIndex: %v)", IterationIndex_);

    YT_LOG_DEBUG("Started fetching bundles");
    UpdateBundleList();
    YT_LOG_DEBUG("Finished fetching bundles");

    for (auto& [bundleName, bundle] : Bundles_) {
        YT_LOG_DEBUG("Started fetching (BundleName: %v)", bundleName);

        if (auto result = WaitFor(bundle->UpdateState()); !result.IsOK()) {
            YT_LOG_ERROR(result, "Failed to update meta registry (BundleName: %v)", bundleName);
            continue;
        }

        if (!bundle->IsBalancingAllowed()) {
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

        // Get descriptors.
        // Print descriptors.
        // Create tablet actions.
    }

    ++IterationIndex_;
    FetchTablets();
}

void TTabletBalancer::DoStop()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_INFO("Stopping pool");

    PollExecutor_->Stop();

    // TODO(alexelex): wait all tablet_actions

    // TODO(alexelex): could clear state, could not

    YT_LOG_INFO("Tablet balancer instance stopped");
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

void TTabletBalancer::UpdateBundleList()
{
    TListNodeOptions options;
    options.Attributes = {"health", "tablet_balancer_config", "tablet_cell_ids"};

    auto bundles = WaitFor(Bootstrap_
        ->GetMasterClient()
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
                Bootstrap_->GetMasterClient(),
                WorkerPool_->GetInvoker())).first;
        it->second->UpdateBundleAttributes(&bundle->Attributes());
    }

    // Find bundles that are not in the list of bundles (probably deleted) and erase them.
    DropMissingKeys(&Bundles_, currentBundles);
}

void TTabletBalancer::BalanceViaMove(const TBundleStatePtr& bundle)
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
        }

        actionCount += std::ssize(descriptors);
    }

    YT_LOG_DEBUG("Balance tablets via move finished (ActionCount: %v)", actionCount);
}

void TTabletBalancer::BalanceViaReshard(const TBundleStatePtr& bundle)
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
