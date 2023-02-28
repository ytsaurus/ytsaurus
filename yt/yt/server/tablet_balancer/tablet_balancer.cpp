#include "action_manager.h"
#include "bootstrap.h"
#include "bundle_state.h"
#include "config.h"
#include "dynamic_config_manager.h"
#include "helpers.h"
#include "private.h"
#include "public.h"
#include "tablet_action.h"
#include "tablet_balancer.h"

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTracing;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

static const TString TabletCellBundlesPath("//sys/tablet_cell_bundles");

constexpr static TDuration MinBalanceFrequency = TDuration::Minutes(1);

using TGlobalGroupTag = std::pair<TString, TGroupName>;

////////////////////////////////////////////////////////////////////////////////

class TParameterizedBalancingTimeoutScheduler
{
public:
    TParameterizedBalancingTimeoutScheduler(TDuration timeoutOnStart, TDuration timeout)
        : TimeoutOnStart_(timeoutOnStart)
        , Timeout_(timeout)
    { }

    bool IsBalancingAllowed(const TGlobalGroupTag& groupTag) const
    {
        auto now = Now();
        if (now - InstanceStartTime_ < TimeoutOnStart_) {
            return false;
        }

        auto it = GroupToLastBalancingTime_.find(groupTag);
        return it == GroupToLastBalancingTime_.end() || now - it->second >= Timeout_;
    }

    void Start()
    {
        InstanceStartTime_ = Now();
        GroupToLastBalancingTime_.clear();
    }

    void UpdateBalancingTime(const TGlobalGroupTag& groupTag)
    {
        GroupToLastBalancingTime_[groupTag] = Now();
    }

private:
    const TDuration TimeoutOnStart_;
    const TDuration Timeout_;
    TInstant InstanceStartTime_;
    THashMap<TGlobalGroupTag, TInstant> GroupToLastBalancingTime_;
};

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
    THashSet<TGlobalGroupTag> GroupsToMoveOnNextIteration_;
    IThreadPoolPtr WorkerPool_;
    IActionManagerPtr ActionManager_;

    std::atomic<bool> Enable_{false};
    std::atomic<bool> EnableEverywhere_{false};
    std::atomic<int> MaxParameterizedMoveActionCount_{0};
    std::atomic<double> ParameterizedDeviationThreshold_{0.0};
    TAtomicObject<TTimeFormula> ScheduleFormula_;
    TAtomicObject<std::optional<TDuration>> Period_;
    TAtomicObject<TString> DefaultParameterizedMetric_;

    TParameterizedBalancingTimeoutScheduler ParameterizedBalancingScheduler_;
    TInstant CurrentIterationStartTime_;
    TInstant PreviousIterationStartTime_;
    i64 IterationIndex_;

    void BalancerIteration();
    void TryBalancerIteration();
    void BalanceBundle(const TBundleStatePtr& bundleState);

    bool IsBalancingAllowed(const TBundleStatePtr& bundleState) const;

    void BalanceViaReshard(const TBundleStatePtr& bundleState, const TGroupName& groupName);
    void BalanceViaMove(const TBundleStatePtr& bundleState, const TGroupName& groupName) const;
    void BalanceViaMoveInMemory(const TBundleStatePtr& bundleState) const;
    void BalanceViaMoveOrdinary(const TBundleStatePtr& bundleState) const;
    void TryBalanceViaMoveParameterized(const TBundleStatePtr& bundleState, const TGroupName& groupName);
    void BalanceViaMoveParameterized(const TBundleStatePtr& bundleState, const TGroupName& groupName);

    THashSet<TGroupName> GetBalancingGroups(const TBundleStatePtr& bundleState) const;

    std::vector<TString> UpdateBundleList();
    bool HasUntrackedUnfinishedActions(
        const TBundleStatePtr& bundleState,
        const IAttributeDictionary* attributes) const;

    bool DidBundleBalancingTimeHappen(const TTabletCellBundlePtr& bundle, const TTimeFormula& groupSchedule) const;
    TTimeFormula GetBundleSchedule(const TTabletCellBundlePtr& bundle, const TTimeFormula& groupSchedule) const;

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
        BIND(&TTabletBalancer::TryBalancerIteration, MakeWeak(this)),
        Config_->Period))
    , WorkerPool_(CreateThreadPool(
        Config_->WorkerThreadPoolSize,
        "TabletBalancer"))
    , ActionManager_(CreateActionManager(
        Config_->TabletActionExpirationTimeout,
        Config_->TabletActionPollingPeriod,
        Bootstrap_->GetClient(),
        Bootstrap_))
    , ParameterizedBalancingScheduler_(
        Config_->ParameterizedTimeoutOnStart,
        Config_->ParameterizedTimeout)
    , PreviousIterationStartTime_(TruncatedNow())
    , IterationIndex_(0)
{
    bootstrap->GetDynamicConfigManager()->SubscribeConfigChanged(BIND(&TTabletBalancer::OnDynamicConfigChanged, MakeWeak(this)));
}

void TTabletBalancer::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Starting tablet balancer instance (Period: %v)",
        Period_.Load().value_or(Config_->Period));

    GroupsToMoveOnNextIteration_.clear();

    ParameterizedBalancingScheduler_.Start();

    PollExecutor_->Start();

    ActionManager_->Start(Bootstrap_->GetElectionManager()->GetPrerequisiteTransactionId());
}

void TTabletBalancer::Stop()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_INFO("Stopping tablet balancer instance");

    YT_UNUSED_FUTURE(PollExecutor_->Stop());
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
    auto newBundles = UpdateBundleList();
    YT_LOG_DEBUG("Finished fetching bundles (NewBundleCount: %v)", newBundles.size());

    CurrentIterationStartTime_ = TruncatedNow();

    for (auto& [bundleName, bundle] : Bundles_) {
        if (!bundle->GetBundle()->Config) {
            YT_LOG_DEBUG("Skip balancing iteration since bundle has unparseable tablet balancer config (BundleName: %v)",
                bundleName);
            continue;
        }

        if (bundle->GetHasUntrackedUnfinishedActions() || ActionManager_->HasUnfinishedActions(bundleName)) {
            YT_LOG_DEBUG("Skip balancing iteration since bundle has unfinished actions (BundleName: %v)",
                bundleName);
            continue;
        }

        YT_LOG_DEBUG("Started fetching (BundleName: %v)", bundleName);

        if (auto result = WaitFor(bundle->UpdateState()); !result.IsOK()) {
            YT_LOG_ERROR(result, "Failed to update meta registry (BundleName: %v)", bundleName);
            continue;
        }

        if (!IsBalancingAllowed(bundle)) {
            YT_LOG_DEBUG("Balancing is disabled (BundleName: %v)",
                bundleName);
            continue;
        }

        if (auto result = WaitFor(bundle->FetchStatistics()); !result.IsOK()) {
            YT_LOG_ERROR(result, "Fetch statistics failed (BundleName: %v)", bundleName);
            continue;
        }

        BalanceBundle(bundle);

        ActionManager_->CreateActions(bundleName);
    }

    ++IterationIndex_;
    PreviousIterationStartTime_ = CurrentIterationStartTime_;
}

void TTabletBalancer::TryBalancerIteration()
{
    TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("TabletBalancer"));
    YT_PROFILE_TIMING("/tablet_balancer/balancer_iteration_time") {
        try {
            BalancerIteration();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Balancer iteration failed");
        }
    }
}

void TTabletBalancer::BalanceBundle(const TBundleStatePtr& bundle)
{
    const auto& bundleName = bundle->GetBundle()->Name;
    auto groups = GetBalancingGroups(bundle);

    for (const auto& groupName : groups) {
        const auto& groupConfig = GetOrCrash(bundle->GetBundle()->Config->Groups, groupName);
        TGlobalGroupTag groupTag(bundleName, groupName);

        if (auto it = GroupsToMoveOnNextIteration_.find(groupTag); it != GroupsToMoveOnNextIteration_.end()) {
            switch (groupConfig->Type) {
                case EBalancingType::Legacy:
                    GroupsToMoveOnNextIteration_.erase(it);
                    BalanceViaMove(bundle, groupName);
                    break;

                case EBalancingType::Parameterized:
                    if (ParameterizedBalancingScheduler_.IsBalancingAllowed(groupTag)) {
                        GroupsToMoveOnNextIteration_.erase(it);
                        TryBalanceViaMoveParameterized(bundle, groupName);
                    } else {
                        YT_LOG_DEBUG("Skip parameterized balancing iteration due to "
                            "recalculation of performance counters (BundleName: %v, Group: %v)",
                            bundleName,
                            groupName);
                    }
                    break;
            }
        } else if (DidBundleBalancingTimeHappen(bundle->GetBundle(), groupConfig->Schedule)) {
            GroupsToMoveOnNextIteration_.insert(std::move(groupTag));
            BalanceViaReshard(bundle, groupName);
        } else {
            YT_LOG_DEBUG("Skip balancing iteration because the time has not yet come (BundleName: %v, Group: %v)",
                bundleName,
                groupName);
        }
    }
}

bool TTabletBalancer::IsBalancingAllowed(const TBundleStatePtr& bundleState) const
{
    return Enable_ &&
        bundleState->GetHealth() == ETabletCellHealth::Good &&
        (EnableEverywhere_ ||
         bundleState->GetBundle()->Config->EnableStandaloneTabletBalancer);
}

THashSet<TGroupName> TTabletBalancer::GetBalancingGroups(const TBundleStatePtr& bundleState) const
{
    THashSet<TGroupName> groups;
    const auto& bundle = bundleState->GetBundle();
    for (const auto& [id, table] : bundle->Tables) {
        auto groupName = table->GetBalancingGroup();
        if (!groupName) {
            continue;
        }

        groups.insert(*groupName);
    }

    return groups;
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
    MaxParameterizedMoveActionCount_.store(newConfig->MaxParameterizedMoveActionCount);
    ParameterizedDeviationThreshold_.store(newConfig->ParameterizedDeviationThreshold);
    ScheduleFormula_.Store(newConfig->Schedule);
    Period_.Store(newConfig->Period);
    DefaultParameterizedMetric_.Store(newConfig->DefaultParameterizedMetric);

    auto oldPeriod = oldConfig->Period.value_or(Config_->Period);
    auto newPeriod = newConfig->Period.value_or(Config_->Period);
    if (newPeriod != oldPeriod) {
        PollExecutor_->SetPeriod(newPeriod);
    }

    YT_LOG_DEBUG(
        "Updated tablet balancer dynamic config (OldConfig: %v, NewConfig: %v)",
        ConvertToYsonString(oldConfig, EYsonFormat::Text),
        ConvertToYsonString(newConfig, EYsonFormat::Text));
}

std::vector<TString> TTabletBalancer::UpdateBundleList()
{
    TListNodeOptions options;
    options.Attributes = {"health", "tablet_balancer_config", "tablet_cell_ids", "tablet_actions"};

    auto bundles = WaitFor(Bootstrap_
        ->GetClient()
        ->ListNode(TabletCellBundlesPath, options))
        .ValueOrThrow();
    auto bundlesList = ConvertTo<IListNodePtr>(bundles);

    // Gather current bundles.
    THashSet<TString> currentBundles;
    std::vector<TString> newBundles;
    auto defaultParameterizedMetric = DefaultParameterizedMetric_.Load();
    for (const auto& bundle : bundlesList->GetChildren()) {
        const auto& name = bundle->AsString()->GetValue();
        currentBundles.insert(bundle->AsString()->GetValue());

        auto [it, isNew] = Bundles_.emplace(
            name,
            New<TBundleState>(
                name,
                Bootstrap_->GetClient(),
                WorkerPool_->GetInvoker()));
        it->second->UpdateBundleAttributes(&bundle->Attributes(), defaultParameterizedMetric);
        it->second->SetHasUntrackedUnfinishedActions(HasUntrackedUnfinishedActions(it->second, &bundle->Attributes()));

        if (isNew) {
            newBundles.push_back(name);
        }
    }

    // Find bundles that are not in the list of bundles (probably deleted) and erase them.
    DropMissingKeys(Bundles_, currentBundles);
    return newBundles;
}

bool TTabletBalancer::HasUntrackedUnfinishedActions(
    const TBundleStatePtr& bundleState,
    const IAttributeDictionary* attributes) const
{
    auto actions = attributes->Get<std::vector<IMapNodePtr>>("tablet_actions");
    for (auto actionMapNode : actions) {
        auto state = ConvertTo<ETabletActionState>(actionMapNode->FindChild("state"));
        if (IsTabletActionFinished(state)) {
            continue;
        }

        auto actionId = ConvertTo<TTabletActionId>(actionMapNode->FindChild("tablet_action_id"));
        if (!ActionManager_->IsKnownAction(bundleState->GetBundle()->Name, actionId)) {
            return true;
        }
    }
    return false;
}

bool TTabletBalancer::DidBundleBalancingTimeHappen(const TTabletCellBundlePtr& bundle, const TTimeFormula& groupSchedule) const
{
    auto formula = GetBundleSchedule(bundle, groupSchedule);

    try {
        if (Config_->Period >= MinBalanceFrequency) {
            TInstant timePoint = PreviousIterationStartTime_ + MinBalanceFrequency;
            if (timePoint > CurrentIterationStartTime_) {
                return false;
            }
            while (timePoint <= CurrentIterationStartTime_) {
                if (formula.IsSatisfiedBy(timePoint)) {
                    return true;
                }
                timePoint += MinBalanceFrequency;
            }
            return false;
        } else {
            return formula.IsSatisfiedBy(CurrentIterationStartTime_);
        }
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to evaluate tablet balancer schedule formula");
        return false;
    }
}

TTimeFormula TTabletBalancer::GetBundleSchedule(const TTabletCellBundlePtr& bundle, const TTimeFormula& groupSchedule) const
{
    if (!groupSchedule.IsEmpty()) {
        YT_LOG_DEBUG("Using group balancer schedule for bundle (BundleName: %v, ScheduleFormula: %v)",
            bundle->Name,
            groupSchedule.GetFormula());
        return groupSchedule;
    }
    const auto& local = bundle->Config->TabletBalancerSchedule;
    if (!local.IsEmpty()) {
        YT_LOG_DEBUG("Using local balancer schedule for bundle (BundleName: %v, ScheduleFormula: %v)",
            bundle->Name,
            local.GetFormula());
        return local;
    }
    auto formula = ScheduleFormula_.Load();
    YT_LOG_DEBUG("Using global balancer schedule for bundle (BundleName: %v, ScheduleFormula: %v)",
        bundle->Name,
        formula.GetFormula());
    return formula;
}

void TTabletBalancer::BalanceViaMoveInMemory(const TBundleStatePtr& bundleState) const
{
    YT_LOG_DEBUG("Balancing in memory tablets via move started (BundleName: %v)",
        bundleState->GetBundle()->Name);

    auto groupConfig = GetOrCrash(bundleState->GetBundle()->Config->Groups, LegacyInMemoryGroupName);
    if (!bundleState->GetBundle()->Config->EnableInMemoryCellBalancer || !groupConfig->Enable) {
        YT_LOG_DEBUG("Balancing in memory tablets via move is disabled (BundleName: %v)",
            bundleState->GetBundle()->Name);
        return;
    }

    auto descriptors = WaitFor(
        BIND(
            ReassignInMemoryTablets,
            bundleState->GetBundle(),
            /*movableTables*/ std::nullopt,
            /*ignoreTableWiseConfig*/ false,
            Logger)
        .AsyncVia(WorkerPool_->GetInvoker())
        .Run())
        .ValueOrThrow();

    int actionCount = 0;

    if (!descriptors.empty()) {
        for (auto descriptor : descriptors) {
            YT_LOG_DEBUG("Move action created (TabletId: %v, CellId: %v)",
                descriptor.TabletId,
                descriptor.TabletCellId);
            ActionManager_->ScheduleActionCreation(bundleState->GetBundle()->Name, descriptor);

            auto tablet = GetOrCrash(bundleState->Tablets(), descriptor.TabletId);
            auto& profilingCounters = GetOrCrash(bundleState->ProfilingCounters(), tablet->Table->Id);
            profilingCounters.InMemoryMoves.Increment(1);
        }

        actionCount += std::ssize(descriptors);
    }

    YT_LOG_DEBUG("Balancing in memory tablets via move finished (BundleName: %v, ActionCount: %v)",
        bundleState->GetBundle()->Name,
        actionCount);
}

void TTabletBalancer::BalanceViaMoveOrdinary(const TBundleStatePtr& bundleState) const
{
    YT_LOG_DEBUG("Balancing ordinary tablets via move started (BundleName: %v)",
        bundleState->GetBundle()->Name);

    auto groupConfig = GetOrCrash(bundleState->GetBundle()->Config->Groups, LegacyGroupName);
    if (!bundleState->GetBundle()->Config->EnableCellBalancer || !groupConfig->Enable) {
        YT_LOG_DEBUG("Balancing ordinary tablets via move is disabled (BundleName: %v)",
            bundleState->GetBundle()->Name);
        return;
    }

    auto descriptors = WaitFor(
        BIND(
            ReassignOrdinaryTablets,
            bundleState->GetBundle(),
            /*movableTables*/ std::nullopt,
            Logger)
        .AsyncVia(WorkerPool_->GetInvoker())
        .Run())
        .ValueOrThrow();

    int actionCount = 0;
    if (!descriptors.empty()) {
        for (auto descriptor : descriptors) {
            YT_LOG_DEBUG("Move action created (TabletId: %v, CellId: %v)",
                descriptor.TabletId,
                descriptor.TabletCellId);
            ActionManager_->ScheduleActionCreation(bundleState->GetBundle()->Name, descriptor);

            auto tablet = GetOrCrash(bundleState->Tablets(), descriptor.TabletId);
            auto& profilingCounters = GetOrCrash(bundleState->ProfilingCounters(), tablet->Table->Id);
            profilingCounters.OrdinaryMoves.Increment(1);
        }

        actionCount += std::ssize(descriptors);
    }

    YT_LOG_DEBUG("Balancing ordinary tablets via move finished (BundleName: %v, ActionCount: %v)",
        bundleState->GetBundle()->Name,
        actionCount);
}

void TTabletBalancer::BalanceViaMoveParameterized(const TBundleStatePtr& bundleState, const TGroupName& groupName)
{
    YT_LOG_DEBUG("Balancing tablets via parameterized move started (BundleName: %v, Group: %v)",
        bundleState->GetBundle()->Name,
        groupName);

    auto groupConfig = GetOrCrash(bundleState->GetBundle()->Config->Groups, groupName);
    if (!groupConfig->Enable) {
        YT_LOG_DEBUG("Balancing tablets via parameterized move is disabled (BundleName: %v, GroupName: %v)",
            bundleState->GetBundle()->Name,
            groupName);
        return;
    }

    auto descriptors = WaitFor(
        BIND(
            ReassignTabletsParameterized,
            bundleState->GetBundle(),
            bundleState->DefaultPerformanceCountersKeys_,
            MaxParameterizedMoveActionCount_.load(),
            ParameterizedDeviationThreshold_.load(),
            groupConfig->Parameterized,
            groupName,
            Logger)
        .AsyncVia(WorkerPool_->GetInvoker())
        .Run())
        .ValueOrThrow();

    int actionCount = 0;

    if (!descriptors.empty()) {
        for (auto descriptor : descriptors) {
            YT_LOG_DEBUG("Move action created (TabletId: %v, TabletCellId: %v)",
                descriptor.TabletId,
                descriptor.TabletCellId);
            ActionManager_->ScheduleActionCreation(bundleState->GetBundle()->Name, descriptor);

            auto tablet = GetOrCrash(bundleState->Tablets(), descriptor.TabletId);
            auto& profilingCounters = GetOrCrash(bundleState->ProfilingCounters(), tablet->Table->Id);
            profilingCounters.ParameterizedMoves.Increment(1);

            ApplyMoveTabletAction(tablet, descriptor.TabletCellId);
        }

        actionCount += std::ssize(descriptors);
    }

    if (actionCount > 0) {
        ParameterizedBalancingScheduler_.UpdateBalancingTime({bundleState->GetBundle()->Name, groupName});
    }

    YT_LOG_DEBUG("Balance tablets via parameterized move finished (ActionCount: %v)", actionCount);
}

void TTabletBalancer::TryBalanceViaMoveParameterized(const TBundleStatePtr& bundleState, const TGroupName& groupName)
{
    const auto bundle = bundleState->GetBundle();
    try {
        BalanceViaMoveParameterized(bundleState, groupName);
    } catch (const std::exception& ex) {
        const auto& groupConfig = GetOrCrash(bundle->Config->Groups, groupName);
        YT_LOG_ERROR(ex,
            "Parameterized balancing failed with an exception (BundleName: %v, Group: %v, GroupType: %lv, GroupMetric: %v)",
            bundle->Name,
            groupName,
            groupConfig->Type,
            groupConfig->Parameterized->Metric);
    }
}

void TTabletBalancer::BalanceViaMove(const TBundleStatePtr& bundleState, const TGroupName& groupName) const
{
    if (groupName == LegacyGroupName) {
        BalanceViaMoveOrdinary(bundleState);
    } else if (groupName == LegacyInMemoryGroupName) {
        BalanceViaMoveInMemory(bundleState);
    } else {
        YT_LOG_ERROR("Trying to balance a non-legacy group with legacy algorithm (BundleName: %v, GroupName: %v)",
            bundleState->GetBundle()->Name,
            groupName);
    }
}

void TTabletBalancer::BalanceViaReshard(const TBundleStatePtr& bundleState, const TGroupName& groupName)
{
    YT_LOG_DEBUG("Balancing tablets via reshard started (BundleName: %v)",
        bundleState->GetBundle()->Name);

    std::vector<TTabletPtr> tablets;
    for (const auto& [id, tablet] : bundleState->Tablets()) {
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

        if ((*beginIt)->Table->GetBalancingGroup() != groupName) {
            beginIt = endIt;
            continue;
        }

        auto& profilingCounters = GetOrCrash(bundleState->ProfilingCounters(), (*beginIt)->Table->Id);
        auto tableTablets = std::vector<TTabletPtr>(beginIt, endIt);
        beginIt = endIt;

        // TODO(alexelex): Check if the table has actions.

        auto descriptors = WaitFor(
            BIND(
                MergeSplitTabletsOfTable,
                std::move(tableTablets),
                Logger)
            .AsyncVia(WorkerPool_->GetInvoker())
            .Run())
            .ValueOrThrow();

        for (auto descriptor : descriptors) {
            YT_LOG_DEBUG("Reshard action created (TabletIds: %v, TabletCount: %v, DataSize: %v)",
                descriptor.Tablets,
                descriptor.TabletCount,
                descriptor.DataSize);
            ActionManager_->ScheduleActionCreation(bundleState->GetBundle()->Name, descriptor);

            if (descriptor.TabletCount == 1) {
                profilingCounters.TabletMerges.Increment(1);
            } else if (std::ssize(descriptor.Tablets) == 1) {
                profilingCounters.TabletSplits.Increment(1);
            } else {
                profilingCounters.NonTrivialReshards.Increment(1);
            }
        }
        actionCount += std::ssize(descriptors);
    }

    if (actionCount > 0) {
        ParameterizedBalancingScheduler_.UpdateBalancingTime({bundleState->GetBundle()->Name, groupName});
    }

    YT_LOG_DEBUG("Balancing tablets via reshard finished (BundleName: %v, ActionCount: %v)",
        bundleState->GetBundle()->Name,
        actionCount);
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
