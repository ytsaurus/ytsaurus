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
#include "table_registry.h"

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/parameterized_balancing_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/tablet_client/pivot_keys_picker.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <util/random/shuffle.h>

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

static constexpr int MaxSavedErrorCount = 10;

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
    struct TScheduledActionCountLimiter
    {
        THashMap<TGlobalGroupTag, i64> GroupToActionCount;
        i64 GroupLimit;

        bool TryIncrease(const TGlobalGroupTag& groupTag);
    };

    IBootstrap* const Bootstrap_;
    const TStandaloneTabletBalancerConfigPtr Config_;
    const IInvokerPtr ControlInvoker_;
    const TPeriodicExecutorPtr PollExecutor_;
    THashMap<TString, TBundleStatePtr> Bundles_;
    mutable THashMap<TString, std::deque<TError>> BundleErrors_;

    THashSet<TGlobalGroupTag> GroupsToMoveOnNextIteration_;
    IThreadPoolPtr WorkerPool_;
    IActionManagerPtr ActionManager_;
    TTableRegistryPtr TableRegistry_;

    TAtomicIntrusivePtr<TTabletBalancerDynamicConfig> DynamicConfig_;

    TScheduledActionCountLimiter ActionCountLimiter_;
    TParameterizedBalancingTimeoutScheduler ParameterizedBalancingScheduler_;
    TInstant CurrentIterationStartTime_;
    THashMap<TGlobalGroupTag, TInstant> GroupPreviousIterationStartTime_;
    i64 IterationIndex_;

    NProfiling::TCounter PickPivotFailures;

    void BalancerIteration();
    void TryBalancerIteration();
    void BalanceBundle(const TBundleStatePtr& bundleState);

    bool IsBalancingAllowed(const TBundleStatePtr& bundleState) const;
    bool TryScheduleActionCreation(const TGlobalGroupTag& groupTag, const TActionDescriptor& descriptor);

    void BalanceViaReshard(const TBundleStatePtr& bundleState, const TGroupName& groupName);
    void BalanceViaMove(const TBundleStatePtr& bundleState, const TGroupName& groupName);
    void BalanceViaMoveInMemory(const TBundleStatePtr& bundleState);
    void BalanceViaMoveOrdinary(const TBundleStatePtr& bundleState);
    void TryBalanceViaMoveParameterized(const TBundleStatePtr& bundleState, const TGroupName& groupName);
    void BalanceViaMoveParameterized(const TBundleStatePtr& bundleState, const TGroupName& groupName);

    THashSet<TGroupName> GetBalancingGroups(const TBundleStatePtr& bundleState) const;

    std::vector<TString> UpdateBundleList();
    bool HasUntrackedUnfinishedActions(
        const TBundleStatePtr& bundleState,
        const IAttributeDictionary* attributes) const;

    bool DidBundleBalancingTimeHappen(
        const TTabletCellBundlePtr& bundle,
        const TGlobalGroupTag& groupTag,
        const TTimeFormula& groupSchedule) const;
    TTimeFormula GetBundleSchedule(const TTabletCellBundlePtr& bundle, const TTimeFormula& groupSchedule) const;

    void BuildOrchid(IYsonConsumer* consumer) const;
    void SaveBundleError(const TString& bundleName, TError error) const;
    void RemoveBundleErrorsByTtl(TDuration ttl);

    void PickReshardPivotKeysIfNeeded(
        TReshardDescriptor* descriptor,
        const TTablePtr& table,
        const TBundleStatePtr& bundleState,
        std::optional<double> slicingAccuracy,
        bool enableVerboseLogging) const;
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
    , TableRegistry_(New<TTableRegistry>())
    , DynamicConfig_(TAtomicIntrusivePtr(New<TTabletBalancerDynamicConfig>()))
    , ParameterizedBalancingScheduler_(
        Config_->ParameterizedTimeoutOnStart,
        Config_->ParameterizedTimeout)
    , IterationIndex_(0)
    , PickPivotFailures(TabletBalancerProfiler.WithSparse().Counter("/pick_pivot_failures"))
{
    bootstrap->GetDynamicConfigManager()->SubscribeConfigChanged(BIND(&TTabletBalancer::OnDynamicConfigChanged, MakeWeak(this)));
}

void TTabletBalancer::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Starting tablet balancer instance (Period: %v)",
        DynamicConfig_.Acquire()->Period.value_or(Config_->Period));

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

    if (!DynamicConfig_.Acquire()->Enable) {
        YT_LOG_DEBUG("Standalone tablet balancer is not enabled");
        return;
    }

    YT_LOG_INFO("Balancer iteration (IterationIndex: %v)", IterationIndex_);

    YT_LOG_DEBUG("Started fetching bundles");
    auto newBundles = UpdateBundleList();
    YT_LOG_DEBUG("Finished fetching bundles (NewBundleCount: %v)", newBundles.size());

    CurrentIterationStartTime_ = TruncatedNow();
    auto dynamicConfig = DynamicConfig_.Acquire();
    RemoveBundleErrorsByTtl(dynamicConfig->BundleErrorsTtl);
    ActionCountLimiter_ = TScheduledActionCountLimiter{
        .GroupLimit = dynamicConfig->MaxActionsPerGroup};

    for (auto& [bundleName, bundle] : Bundles_) {
        if (!bundle->GetBundle()->Config) {
            YT_LOG_ERROR("Skip balancing iteration since bundle has unparsable tablet balancer config (BundleName: %v)",
                bundleName);

            SaveBundleError(bundleName, TError(
                EErrorCode::IncorrectConfig,
                "Bundle has unparsable tablet balancer config"));
            continue;
        }

        if (bundle->GetHasUntrackedUnfinishedActions() || ActionManager_->HasUnfinishedActions(bundleName)) {
            YT_LOG_DEBUG("Skip balancing iteration since bundle has unfinished actions (BundleName: %v)",
                bundleName);
            continue;
        }

        YT_LOG_DEBUG("Started fetching (BundleName: %v)", bundleName);

        if (auto result = WaitFor(bundle->UpdateState(DynamicConfig_.Acquire()->FetchTabletCellsFromSecondaryMasters)); !result.IsOK()) {
            YT_LOG_ERROR(result, "Failed to update meta registry (BundleName: %v)", bundleName);

            SaveBundleError(bundleName, TError(
                EErrorCode::StatisticsFetchFailed,
                "Failed to update meta registry")
                << result);
            continue;
        }

        if (!IsBalancingAllowed(bundle)) {
            YT_LOG_DEBUG("Balancing is disabled (BundleName: %v)",
                bundleName);
            continue;
        }

        if (auto result = WaitFor(bundle->FetchStatistics()); !result.IsOK()) {
            YT_LOG_ERROR(result, "Fetch statistics failed (BundleName: %v)", bundleName);

            SaveBundleError(bundleName, TError(
                EErrorCode::StatisticsFetchFailed,
                "Fetch statistics failed")
                << result);
            continue;
        }

        BalanceBundle(bundle);

        ActionManager_->CreateActions(bundleName);
    }

    ++IterationIndex_;
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
        } else if (DidBundleBalancingTimeHappen(bundle->GetBundle(), groupTag, groupConfig->Schedule)) {
            GroupsToMoveOnNextIteration_.insert(std::move(groupTag));
            BalanceViaReshard(bundle, groupName);
        } else {
            YT_LOG_DEBUG("Skip balancing iteration because the time has not yet come (BundleName: %v, Group: %v)",
                bundleName,
                groupName);
        }

        GroupPreviousIterationStartTime_[groupTag] = CurrentIterationStartTime_;
    }
}

bool TTabletBalancer::IsBalancingAllowed(const TBundleStatePtr& bundleState) const
{
    auto dynamicConfig = DynamicConfig_.Acquire();
    return dynamicConfig->Enable &&
        bundleState->GetHealth() == ETabletCellHealth::Good &&
        (dynamicConfig->EnableEverywhere ||
         bundleState->GetBundle()->Config->EnableStandaloneTabletBalancer);
}

bool TTabletBalancer::TScheduledActionCountLimiter::TryIncrease(const TGlobalGroupTag& groupTag)
{
    if (GroupToActionCount[groupTag] >= GroupLimit) {
        YT_LOG_WARNING("Action per iteration limit exceeded (BundleName: %v, Group: %v, Limit: %v)",
            groupTag.first,
            groupTag.second,
            GroupLimit);
        return false;
    }

    ++GroupToActionCount[groupTag];
    return true;
}

bool TTabletBalancer::TryScheduleActionCreation(
    const TGlobalGroupTag& groupTag,
    const TActionDescriptor& descriptor)
{
    auto increased = ActionCountLimiter_.TryIncrease(groupTag);
    if (increased) {
        ActionManager_->ScheduleActionCreation(groupTag.first, descriptor);
    }
    return increased;
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
            .Item("bundle_errors")
                .DoMapFor(BundleErrors_, [] (auto fluent, const auto& pair) {
                    fluent.Item(pair.first).DoListFor(pair.second, [] (auto fluent, const auto& error) {
                        fluent.Item().Value(error);
                    });
                })
            .Item("last_iteration_start_time").Value(CurrentIterationStartTime_)
        .EndMap();
}

void TTabletBalancer::OnDynamicConfigChanged(
    const TTabletBalancerDynamicConfigPtr& oldConfig,
    const TTabletBalancerDynamicConfigPtr& newConfig)
{
    DynamicConfig_.Store(newConfig);

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
    for (const auto& bundle : bundlesList->GetChildren()) {
        const auto& name = bundle->AsString()->GetValue();
        currentBundles.insert(bundle->AsString()->GetValue());

        auto [it, isNew] = Bundles_.emplace(
            name,
            New<TBundleState>(
                name,
                TableRegistry_,
                Bootstrap_->GetClient(),
                WorkerPool_->GetInvoker()));
        it->second->UpdateBundleAttributes(&bundle->Attributes());
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

bool TTabletBalancer::DidBundleBalancingTimeHappen(
    const TTabletCellBundlePtr& bundle,
    const TGlobalGroupTag& groupTag,
    const TTimeFormula& groupSchedule) const
{
    auto formula = GetBundleSchedule(bundle, groupSchedule);

    try {
        if (Config_->Period >= MinBalanceFrequency) {
            TInstant timePoint;
            if (auto it = GroupPreviousIterationStartTime_.find(groupTag); it != GroupPreviousIterationStartTime_.end()) {
                timePoint = it->second + MinBalanceFrequency;
            } else {
                // First balance of this group in this instance
                // so it's ok to balance if this time is satisfied by the formula.
                timePoint = CurrentIterationStartTime_;
            }

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
        SaveBundleError(bundle->Name, TError(
            EErrorCode::StatisticsFetchFailed,
            "Fetch statistics failed")
            << ex);
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
    auto formula = DynamicConfig_.Acquire()->Schedule;
    YT_LOG_DEBUG("Using global balancer schedule for bundle (BundleName: %v, ScheduleFormula: %v)",
        bundle->Name,
        formula.GetFormula());
    return formula;
}

void TTabletBalancer::BalanceViaMoveInMemory(const TBundleStatePtr& bundleState)
{
    YT_LOG_DEBUG("Balancing in memory tablets via move started (BundleName: %v)",
        bundleState->GetBundle()->Name);

    auto groupConfig = GetOrCrash(bundleState->GetBundle()->Config->Groups, LegacyInMemoryGroupName);
    if (!bundleState->GetBundle()->Config->EnableInMemoryCellBalancer || !groupConfig->EnableMove) {
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
    auto groupTag = TGlobalGroupTag(bundleState->GetBundle()->Name, LegacyInMemoryGroupName);

    if (!descriptors.empty()) {
        for (auto descriptor : descriptors) {
            if (!TryScheduleActionCreation(groupTag, descriptor)) {
                SaveBundleError(groupTag.first, TError(
                    EErrorCode::GroupActionLimitExceeded,
                    "Group %Qv has exceeded the limit for creating actions. "
                    "Failed to schedule in-memory move action",
                    groupTag.second)
                    << TErrorAttribute("limit", ActionCountLimiter_.GroupLimit));
                break;
            }

            ++actionCount;
            YT_LOG_DEBUG("Move action created (TabletId: %v, CellId: %v)",
                descriptor.TabletId,
                descriptor.TabletCellId);

            auto tablet = GetOrCrash(bundleState->Tablets(), descriptor.TabletId);
            bundleState->GetProfilingCounters(
                tablet->Table,
                LegacyInMemoryGroupName).InMemoryMoves.Increment(1);
        }
    }

    YT_LOG_DEBUG("Balancing in memory tablets via move finished (BundleName: %v, ActionCount: %v)",
        bundleState->GetBundle()->Name,
        actionCount);
}

void TTabletBalancer::BalanceViaMoveOrdinary(const TBundleStatePtr& bundleState)
{
    YT_LOG_DEBUG("Balancing ordinary tablets via move started (BundleName: %v)",
        bundleState->GetBundle()->Name);

    auto groupConfig = GetOrCrash(bundleState->GetBundle()->Config->Groups, LegacyGroupName);
    if (!bundleState->GetBundle()->Config->EnableCellBalancer || !groupConfig->EnableMove) {
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
    auto groupTag = TGlobalGroupTag(bundleState->GetBundle()->Name, LegacyGroupName);

    if (!descriptors.empty()) {
        for (auto descriptor : descriptors) {
            if (!TryScheduleActionCreation(groupTag, descriptor)) {
                SaveBundleError(groupTag.first, TError(
                    EErrorCode::GroupActionLimitExceeded,
                    "Group %Qv has exceeded the limit for creating actions. "
                    "Failed to schedule ordinary move action",
                    groupTag.second)
                    << TErrorAttribute("limit", ActionCountLimiter_.GroupLimit));
                break;
            }

            ++actionCount;
            YT_LOG_DEBUG("Move action created (TabletId: %v, CellId: %v)",
                descriptor.TabletId,
                descriptor.TabletCellId);

            auto tablet = GetOrCrash(bundleState->Tablets(), descriptor.TabletId);
            bundleState->GetProfilingCounters(
                tablet->Table,
                LegacyGroupName).OrdinaryMoves.Increment(1);
        }
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
    if (!groupConfig->EnableMove) {
        YT_LOG_DEBUG("Balancing tablets via parameterized move is disabled (BundleName: %v, Group: %v)",
            bundleState->GetBundle()->Name,
            groupName);
        return;
    }

    auto dynamicConfig = DynamicConfig_.Acquire();

    auto descriptors = WaitFor(
        BIND(
            ReassignTabletsParameterized,
            bundleState->GetBundle(),
            bundleState->DefaultPerformanceCountersKeys,
            TParameterizedReassignSolverConfig{
                .EnableSwaps = dynamicConfig->EnableSwaps,
                .MaxMoveActionCount = dynamicConfig->MaxParameterizedMoveActionCount,
                .NodeDeviationThreshold = dynamicConfig->ParameterizedNodeDeviationThreshold,
                .CellDeviationThreshold = dynamicConfig->ParameterizedCellDeviationThreshold,
                .MinRelativeMetricImprovement = dynamicConfig->ParameterizedMinRelativeMetricImprovement,
                .Metric = dynamicConfig->DefaultParameterizedMetric,
            }.MergeWith(groupConfig->Parameterized),
            groupName,
            Logger)
        .AsyncVia(WorkerPool_->GetInvoker())
        .Run())
        .ValueOrThrow();

    int actionCount = 0;
    auto groupTag = TGlobalGroupTag(bundleState->GetBundle()->Name, groupName);

    if (!descriptors.empty()) {
        for (auto descriptor : descriptors) {
            if (!TryScheduleActionCreation(groupTag, descriptor)) {
                SaveBundleError(groupTag.first, TError(
                    EErrorCode::GroupActionLimitExceeded,
                    "Group %Qv has exceeded the limit for creating actions. "
                    "Failed to schedule parameterized move action",
                    groupTag.second)
                    << TErrorAttribute("limit", ActionCountLimiter_.GroupLimit));
                break;
            }

            ++actionCount;
            YT_LOG_DEBUG("Move action created (TabletId: %v, TabletCellId: %v)",
                descriptor.TabletId,
                descriptor.TabletCellId);

            auto tablet = GetOrCrash(bundleState->Tablets(), descriptor.TabletId);
            bundleState->GetProfilingCounters(
                tablet->Table,
                groupName).ParameterizedMoves.Increment(1);

            ApplyMoveTabletAction(tablet, descriptor.TabletCellId);
        }
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

        SaveBundleError(bundle->Name, TError(
            EErrorCode::ParameterizedBalancingFailed,
            "Parameterized balancing failed")
            << ex);
    }
}

void TTabletBalancer::BalanceViaMove(const TBundleStatePtr& bundleState, const TGroupName& groupName)
{
    if (groupName == LegacyGroupName) {
        BalanceViaMoveOrdinary(bundleState);
    } else if (groupName == LegacyInMemoryGroupName) {
        BalanceViaMoveInMemory(bundleState);
    } else {
        YT_LOG_ERROR("Trying to balance a non-legacy group with legacy algorithm (BundleName: %v, Group: %v)",
            bundleState->GetBundle()->Name,
            groupName);
    }
}

void TTabletBalancer::BalanceViaReshard(const TBundleStatePtr& bundleState, const TGroupName& groupName)
{
    YT_LOG_DEBUG("Balancing tablets via reshard started (BundleName: %v, Group: %v)",
        bundleState->GetBundle()->Name,
        groupName);

    auto groupConfig = GetOrCrash(bundleState->GetBundle()->Config->Groups, groupName);
    if (!groupConfig->EnableReshard) {
        YT_LOG_DEBUG("Balancing tablets via reshard is disabled (BundleName: %v, Group: %v)",
            bundleState->GetBundle()->Name,
            groupName);
        return;
    }

    std::vector<TTablePtr> tables;
    for (const auto& [id, table] : bundleState->GetBundle()->Tables) {
        if (TypeFromId(id) != EObjectType::Table) {
            continue;
        }

        if (table->GetBalancingGroup() != groupName) {
            continue;
        }

        tables.push_back(table);
    }

    Shuffle(tables.begin(), tables.end());

    int actionCount = 0;
    bool actionLimitExceeded = false;
    auto groupTag = TGlobalGroupTag(bundleState->GetBundle()->Name, groupName);

    auto dynamicConfig = DynamicConfig_.Acquire();
    bool pickReshardPivotKeys = dynamicConfig->PickReshardPivotKeys;
    auto slicingAccuracy = dynamicConfig->ReshardSlicingAccuracy;
    bool enableVerboseLogging = dynamicConfig->EnableReshardVerboseLogging ||
        bundleState->GetBundle()->Config->EnableVerboseLogging;
    auto minDesiredTabletSize = dynamicConfig->MinDesiredTabletSize;

    for (const auto& table : tables) {
        if (actionLimitExceeded) {
            break;
        }

        std::vector<TTabletPtr> tablets;
        for (const auto& tablet : table->Tablets) {
            if (IsTabletReshardable(tablet, /*ignoreConfig*/ false)) {
                tablets.push_back(tablet);
            }
        }

        if (tablets.empty()) {
            continue;
        }

        auto descriptors = WaitFor(
            BIND(
                MergeSplitTabletsOfTable,
                std::move(tablets),
                minDesiredTabletSize,
                pickReshardPivotKeys,
                Logger)
            .AsyncVia(WorkerPool_->GetInvoker())
            .Run())
            .ValueOrThrow();

        std::vector<TReshardDescriptor> scheduledDescriptors;
        for (auto& descriptor : descriptors) {
            if (pickReshardPivotKeys) {
                try {
                    PickReshardPivotKeysIfNeeded(&descriptor, table, bundleState, slicingAccuracy, enableVerboseLogging);
                } catch (const std::exception& ex) {
                    YT_LOG_ERROR(ex,
                        "Failed to pick pivot keys for reshard action "
                        "(TabletIds: %v, TabletCount: %v, DataSize: %v, TableId: %v)",
                        descriptor.Tablets,
                        descriptor.TabletCount,
                        descriptor.DataSize,
                        table->Id);
                    PickPivotFailures.Increment(1);
                }
            }

            if (!TryScheduleActionCreation(groupTag, descriptor)) {
                SaveBundleError(groupTag.first, TError(
                    EErrorCode::GroupActionLimitExceeded,
                    "Group %Qv has exceeded the limit for creating actions. "
                    "Failed to schedule reshard action",
                    groupTag.second)
                    << TErrorAttribute("limit", ActionCountLimiter_.GroupLimit));
                actionLimitExceeded = true;
                break;
            }

            ++actionCount;
            YT_LOG_DEBUG("Reshard action created (TabletIds: %v, TabletCount: %v, DataSize: %v, TableId: %v)",
                descriptor.Tablets,
                descriptor.TabletCount,
                descriptor.DataSize,
                table->Id);

            scheduledDescriptors.emplace_back(std::move(descriptor));
        }

        const auto& profilingCounters = bundleState->GetProfilingCounters(table.Get(), groupName);
        for (const auto& descriptor : scheduledDescriptors) {
            if (descriptor.TabletCount == 1) {
                profilingCounters.TabletMerges.Increment(1);
            } else if (std::ssize(descriptor.Tablets) == 1) {
                profilingCounters.TabletSplits.Increment(1);
            } else {
                profilingCounters.NonTrivialReshards.Increment(1);
            }
        }
    }

    if (actionCount > 0) {
        ParameterizedBalancingScheduler_.UpdateBalancingTime({bundleState->GetBundle()->Name, groupName});
    }

    YT_LOG_DEBUG("Balancing tablets via reshard finished (BundleName: %v, Group: %v, ActionCount: %v)",
        bundleState->GetBundle()->Name,
        groupName,
        actionCount);
}

void TTabletBalancer::SaveBundleError(const TString& bundleName, TError error) const
{
    auto it = BundleErrors_.emplace(bundleName, std::deque<TError>{}).first;
    it->second.emplace_back(std::move(error));

    if (it->second.size() > MaxSavedErrorCount) {
        it->second.pop_front();
    }
}

void TTabletBalancer::RemoveBundleErrorsByTtl(TDuration ttl)
{
    auto currentTime = Now();
    THashSet<TString> relevantBundles;
    for (auto& [bundleName, errors] : BundleErrors_) {
        while (!errors.empty()) {
            const auto& error = errors.front();
            if (error.HasDatetime() && error.GetDatetime() + ttl < currentTime) {
                errors.pop_front();
                continue;
            }
            break;
        }

        if (!errors.empty()) {
            relevantBundles.insert(bundleName);
        }
    }

    DropMissingKeys(BundleErrors_, relevantBundles);
}

void TTabletBalancer::PickReshardPivotKeysIfNeeded(
    TReshardDescriptor* descriptor,
    const TTablePtr& table,
    const TBundleStatePtr& bundleState,
    std::optional<double> slicingAccuracy,
    bool enableVerboseLogging) const
{
    if (descriptor->TabletCount == 1) {
        // Do not pick pivot keys for merge actions.
        return;
    }

    auto options = TReshardTableOptions{
        .EnableSlicing = true,
        .SlicingAccuracy = slicingAccuracy,
    };

    auto tablet = GetOrCrash(bundleState->Tablets(), descriptor->Tablets[0]);
    options.FirstTabletIndex = tablet->Index;
    options.LastTabletIndex = tablet->Index + std::ssize(descriptor->Tablets) - 1;

    int partitionCount = 0;
    for (int index = *options.FirstTabletIndex; index <= options.LastTabletIndex; ++index) {
        partitionCount += table->Tablets[index]->Statistics.PartitionCount;
    }

    if (partitionCount >= 2 * descriptor->TabletCount) {
        // Do not pick pivot keys if there are enough partitions
        // for the master to perform reshard itself.
        return;
    }

    descriptor->PivotKeys = PickPivotKeysWithSlicing(
        Bootstrap_->GetClient(),
        table->Path,
        descriptor->TabletCount,
        options,
        Logger,
        enableVerboseLogging || table->TableConfig->EnableVerboseLogging);
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
