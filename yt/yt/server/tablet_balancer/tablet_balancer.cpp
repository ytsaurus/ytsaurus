#include "action_manager.h"
#include "bootstrap.h"
#include "bundle_state.h"
#include "config.h"
#include "dynamic_config_manager.h"
#include "helpers.h"
#include "move_iteration.h"
#include "private.h"
#include "public.h"
#include "reshard_iteration.h"
#include "tablet_action.h"
#include "tablet_balancer.h"
#include "table_registry.h"

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>
#include <yt/yt/server/lib/tablet_balancer/parameterized_balancing_helpers.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/tablet_client/pivot_keys_picker.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <util/random/shuffle.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTracing;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

static const TString TabletCellBundlesPath("//sys/tablet_cell_bundles");

static constexpr TDuration MinBalanceFrequency = TDuration::Minutes(1);

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
        if (now - InstanceStartTime_ < TimeoutOnStart_.load()) {
            return false;
        }

        auto it = GroupToLastBalancingTime_.find(groupTag);
        return it == GroupToLastBalancingTime_.end() || now - it->second >= Timeout_.load();
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

    void Reconfigure(TDuration timeoutOnStart, TDuration timeout)
    {
        TimeoutOnStart_.store(timeoutOnStart);
        Timeout_.store(timeout);
    }

private:
    std::atomic<TDuration> TimeoutOnStart_;
    std::atomic<TDuration> Timeout_;
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
    using TReshardDescriptorIt = std::vector<TReshardDescriptor>::iterator;

    struct TScheduledActionCountLimiter
    {
        THashMap<TGlobalGroupTag, i64> GroupToActionCount;
        i64 GroupLimit;

        bool TryIncrease(const TGlobalGroupTag& groupTag);
        bool CanIncrease(const TGlobalGroupTag& groupTag);
    };

    struct TBundleErrors
    {
        std::deque<TError> FatalErrors;
        std::deque<TError> RetryableErrors;
    };

    IBootstrap* const Bootstrap_;
    const TStandaloneTabletBalancerConfigPtr Config_;
    const IInvokerPtr ControlInvoker_;
    const TPeriodicExecutorPtr PollExecutor_;
    IThreadPoolPtr WorkerPool_;
    IThreadPoolPtr PivotPickerPool_;

    std::atomic_flag IsActive_ = ATOMIC_FLAG_INIT;

    THashMap<TString, TBundleStatePtr> Bundles_;
    mutable THashMap<TString, TBundleErrors> BundleErrors_;

    THashSet<TGlobalGroupTag> GroupsToMoveOnNextIteration_;
    IActionManagerPtr ActionManager_;
    TTableRegistryPtr TableRegistry_;

    TAtomicIntrusivePtr<TTabletBalancerDynamicConfig> DynamicConfig_;

    TScheduledActionCountLimiter ActionCountLimiter_;
    TParameterizedBalancingTimeoutScheduler ParameterizedBalancingScheduler_;

    // Precise iteration start time used for liveness reporting.
    TInstant PreciseCurrentIterationStartTime_;
    // Logical iteration start time used for iteration scheduling.
    TInstant CurrentIterationStartTime_;
    // To prevent all next iterations from being skipped due to inaccurate timing and formula matching.
    TInstant FirstIterationStartTime_;

    i64 IterationIndex_;
    THashMap<TGlobalGroupTag, TInstant> GroupPreviousIterationStartTime_;
    mutable THashMap<TGlobalGroupTag, THashMap<EBalancingMode, TEventTimer>> IterationProfilingTimers_;

    NProfiling::TCounter PickPivotFailures_;
    THashMap<TGlobalGroupTag, TTableParameterizedMetricTrackerPtr> GroupToParameterizedMetricTracker_;

    void BalancerIteration();
    void TryBalancerIteration();
    void BalanceBundle(const TBundleStatePtr& bundleState);
    IListNodePtr FetchNodeStatistics() const;

    bool IsBalancingAllowed(const TBundleStatePtr& bundleState) const;
    bool TryScheduleActionCreation(const TGlobalGroupTag& groupTag, const TActionDescriptor& descriptor);
    bool CanScheduleActionCreation(const TGlobalGroupTag& groupTag);

    TEventTimer& GetProfilingTimer(const TGlobalGroupTag& groupTag, EBalancingMode type) const;

    void BalanceViaReshard(const TBundleStatePtr& bundleState, const TGroupName& groupName);
    void BalanceViaMove(const TBundleStatePtr& bundleState, const TGroupName& groupName);
    void BalanceViaMoveInMemory(const TBundleStatePtr& bundleState);
    void BalanceViaMoveOrdinary(const TBundleStatePtr& bundleState);
    void TryBalanceViaMoveParameterized(const TBundleStatePtr& bundleState, const TGroupName& groupName);
    void BalanceViaMoveParameterized(
        const TBundleStatePtr& bundleState,
        const TGroupName& groupName,
        const TTabletBalancingGroupConfigPtr& groupConfig);
    void TryBalanceViaReshardParameterized(const TBundleStatePtr& bundleState, const TGroupName& groupName);
    void BalanceViaReshardParameterized(const TBundleStatePtr& bundleState, const TGroupName& groupName);

    void ExecuteReshardIteration(
        const IReshardIterationPtr& reshardIteration,
        const TBundleStatePtr& bundleState);
    void ExecuteMoveIteration(const IMoveIterationPtr& moveIteration);

    THashSet<TGroupName> GetBalancingGroups(const TTabletCellBundlePtr& bundleState) const;

    std::vector<TString> UpdateBundleList();
    bool HasUntrackedUnfinishedActions(
        const TBundleStatePtr& bundleState,
        const IAttributeDictionary* attributes) const;

    bool DidBundleBalancingTimeHappen(
        const TTabletCellBundlePtr& bundle,
        const TGlobalGroupTag& groupTag,
        const TTimeFormula& groupSchedule) const;
    bool CouldAnyBalancingHappen(const TTabletCellBundlePtr& bundle) const;
    TTimeFormula GetBundleSchedule(const TTabletCellBundlePtr& bundle, const TTimeFormula& groupSchedule) const;

    void BuildOrchid(IYsonConsumer* consumer) const;

    void SaveBundleError(std::deque<TError>* errors, TError error) const;
    void SaveRetryableBundleError(const TString& bundleName, TError error) const;
    void SaveFatalBundleError(const TString& bundleName, TError error) const;
    void RemoveBundleErrorsByTtl(TDuration ttl) const;
    void RemoveRetryableErrorsOnSuccessfulIteration(const TString& bundleName) const;

    TFuture<std::vector<TLegacyOwningKey>> PickReshardPivotKeysIfNeeded(
        TReshardDescriptor* descriptor,
        const TTable* table,
        int firstTabletIndex,
        int lastTabletIndex,
        std::optional<double> slicingAccuracy,
        bool enableVerboseLogging) const;

    std::vector<TReshardDescriptor> PickPivotsForDescriptors(
        TReshardDescriptorIt begin,
        TReshardDescriptorIt end,
        const IReshardIterationPtr& reshardIteration,
        const TBundleStatePtr& bundleState,
        TGlobalGroupTag groupTag);

    TTableParameterizedMetricTrackerPtr GetParameterizedMetricTracker(const TGlobalGroupTag& groupTag);
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
    , PivotPickerPool_(CreateThreadPool(
        Config_->PivotPickerThreadPoolSize,
        "PivotKeysPicker"))
    , TableRegistry_(New<TTableRegistry>())
    , DynamicConfig_(TAtomicIntrusivePtr(New<TTabletBalancerDynamicConfig>()))
    , ParameterizedBalancingScheduler_(
        Config_->ParameterizedTimeoutOnStart,
        Config_->ParameterizedTimeout)
    , IterationIndex_(0)
    , PickPivotFailures_(TabletBalancerProfiler().WithSparse().Counter("/pick_pivot_failures"))
{
    ActionManager_ = CreateActionManager(
        DynamicConfig_.Acquire()->ActionManager,
        Bootstrap_->GetClient(),
        Bootstrap_);

    bootstrap->GetDynamicConfigManager()->SubscribeConfigChanged(
        BIND(&TTabletBalancer::OnDynamicConfigChanged, MakeWeak(this)));
}

void TTabletBalancer::Start()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Starting tablet balancer instance (Period: %v)",
        DynamicConfig_.Acquire()->Period.value_or(Config_->Period));

    if (IsActive_.test_and_set()) {
        YT_LOG_WARNING("Trying to start tablet balancer instance which is already active");
    }

    GroupsToMoveOnNextIteration_.clear();
    FirstIterationStartTime_ = TruncatedNow();

    ParameterizedBalancingScheduler_.Start();

    PollExecutor_->Start();

    ActionManager_->Start(Bootstrap_->GetElectionManager()->GetPrerequisiteTransactionId());
}

void TTabletBalancer::Stop()
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_INFO("Stopping tablet balancer instance");

    IsActive_.clear();

    YT_UNUSED_FUTURE(PollExecutor_->Stop());
    ActionManager_->Stop();

    YT_LOG_INFO("Tablet balancer instance stopped");
}

IListNodePtr TTabletBalancer::FetchNodeStatistics() const
{
    TListNodeOptions options;
    static const TString TabletStaticPath = "/statistics/memory/tablet_static";
    static const TString TabletSlotsPath = "/tablet_slots";
    options.Attributes = TAttributeFilter({}, {TabletStaticPath, TabletSlotsPath});

    static const TString TabletNodesPath = "//sys/tablet_nodes";
    YT_LOG_DEBUG("Started fetching node statistics");

    auto nodesOrError = WaitFor(Bootstrap_
        ->GetClient()
        ->ListNode(TabletNodesPath, options));

    if (!nodesOrError.IsOK()) {
        YT_LOG_ERROR(nodesOrError, "Failed to fetch node statistics");
        return {};
    }

    return ConvertTo<IListNodePtr>(nodesOrError.ValueOrThrow());
}

void TTabletBalancer::BalancerIteration()
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    if (!DynamicConfig_.Acquire()->Enable) {
        YT_LOG_INFO("Standalone tablet balancer is not enabled");
        return;
    }

    YT_LOG_INFO("Balancer iteration (IterationIndex: %v)", IterationIndex_);

    YT_LOG_INFO("Started fetching bundles");
    auto newBundles = UpdateBundleList();
    YT_LOG_INFO("Finished fetching bundles (NewBundleCount: %v)", newBundles.size());

    PreciseCurrentIterationStartTime_ = Now();
    CurrentIterationStartTime_ = TruncatedNow();
    auto dynamicConfig = DynamicConfig_.Acquire();
    RemoveBundleErrorsByTtl(dynamicConfig->BundleErrorsTtl);
    ActionCountLimiter_ = TScheduledActionCountLimiter{
        .GroupLimit = dynamicConfig->MaxActionsPerGroup};

    auto nodeList = FetchNodeStatistics();
    for (auto& [bundleName, bundle] : Bundles_) {
        if (!bundle->GetBundle()->Config) {
            YT_LOG_ERROR(
                "Skip balancing iteration since bundle has unparsable tablet balancer config (BundleName: %v)",
                bundleName);

            SaveRetryableBundleError(bundleName, TError(
                NTabletBalancer::EErrorCode::IncorrectConfig,
                "Bundle has unparsable tablet balancer config"));
            continue;
        }

        if (bundle->GetHasUntrackedUnfinishedActions() || ActionManager_->HasUnfinishedActions(bundleName)) {
            YT_LOG_INFO("Skip balancing iteration since bundle has unfinished actions (BundleName: %v)",
                bundleName);
            continue;
        }

        if (!IsBalancingAllowed(bundle)) {
            YT_LOG_INFO("Balancing is disabled (BundleName: %v)",
                bundleName);
            continue;
        }

        if (!CouldAnyBalancingHappen(bundle->GetBundle())) {
            YT_LOG_INFO("Skip fetching for bundle since balancing is not planned "
                "at this iteration according to the schedule (BundleName: %v)",
                bundleName);
            continue;

        }

        YT_LOG_INFO("Started fetching (BundleName: %v)", bundleName);

        if (auto result = WaitFor(bundle->UpdateState(
                dynamicConfig->FetchTabletCellsFromSecondaryMasters,
                IterationIndex_));
            !result.IsOK())
        {
            YT_LOG_ERROR(result, "Failed to update meta registry (BundleName: %v)", bundleName);

            SaveRetryableBundleError(bundleName, TError(
                NTabletBalancer::EErrorCode::StatisticsFetchFailed,
                "Failed to update meta registry")
                << result);
            continue;
        }

        if (auto result = WaitFor(bundle->FetchStatistics(
                nodeList,
                dynamicConfig->UseStatisticsReporter,
                dynamicConfig->StatisticsTablePath));
            !result.IsOK())
        {
            YT_LOG_ERROR(result, "Fetch statistics failed (BundleName: %v)", bundleName);

            SaveRetryableBundleError(bundleName, TError(
                NTabletBalancer::EErrorCode::StatisticsFetchFailed,
                "Fetch statistics failed")
                << result);
            continue;
        }

        RemoveRetryableErrorsOnSuccessfulIteration(bundleName);

        YT_LOG_INFO("Bundle balancing iteration started (BundleName: %v)", bundleName);
        BalanceBundle(bundle);
        YT_LOG_INFO("Bundle balancing iteration finished (BundleName: %v)", bundleName);

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
    auto groups = GetBalancingGroups(bundle->GetBundle());

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
                        YT_LOG_INFO("Skip parameterized balancing iteration due to "
                            "recalculation of performance counters (BundleName: %v, Group: %v)",
                            bundleName,
                            groupName);
                    }
                    break;
            }
        } else if (DidBundleBalancingTimeHappen(bundle->GetBundle(), groupTag, groupConfig->Schedule)) {
            GroupsToMoveOnNextIteration_.insert(std::move(groupTag));
            if (groupConfig->Type == EBalancingType::Parameterized) {
                TryBalanceViaReshardParameterized(bundle, groupName);
            }
            BalanceViaReshard(bundle, groupName);
        } else {
            YT_LOG_INFO("Skip balancing iteration because the time has not yet come (BundleName: %v, Group: %v)",
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
    if (!CanIncrease(groupTag)) {
        return false;
    }

    ++GroupToActionCount[groupTag];
    return true;
}

bool TTabletBalancer::TScheduledActionCountLimiter::CanIncrease(const TGlobalGroupTag& groupTag)
{
    if (GroupToActionCount[groupTag] >= GroupLimit) {
        YT_LOG_WARNING("Action per iteration limit exceeded (BundleName: %v, Group: %v, Limit: %v)",
            groupTag.first,
            groupTag.second,
            GroupLimit);
        return false;
    }
    return true;
}

bool TTabletBalancer::CanScheduleActionCreation(
    const TGlobalGroupTag& groupTag)
{
    return ActionCountLimiter_.CanIncrease(groupTag);
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

THashSet<TGroupName> TTabletBalancer::GetBalancingGroups(const TTabletCellBundlePtr& bundle) const
{
    THashSet<TGroupName> groups;
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
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    if (!IsActive_.test()) {
        YT_LOG_DEBUG("Removing errors by ttl for inactive instance");

        RemoveBundleErrorsByTtl(DynamicConfig_.Acquire()->BundleErrorsTtl);
    }

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
                    fluent.DoIf(!pair.second.FatalErrors.empty(), [&] (TFluentMap fluent) {
                        fluent.Item(pair.first).DoListFor(pair.second.FatalErrors, [] (auto fluent, const auto& error) {
                            fluent.Item().Value(error);
                        });
                    });
                })
            .Item("retryable_bundle_errors")
                .DoMapFor(BundleErrors_, [] (auto fluent, const auto& pair) {
                    fluent.DoIf(!pair.second.RetryableErrors.empty(), [&] (TFluentMap fluent) {
                        fluent.Item(pair.first).DoListFor(pair.second.RetryableErrors, [] (auto fluent, const auto& error) {
                            fluent.Item().Value(error);
                        });
                    });
                })
            .DoIf(PreciseCurrentIterationStartTime_ != TInstant::Zero(), [&] (TFluentMap fluent) {
                fluent.Item("last_iteration_start_time").Value(PreciseCurrentIterationStartTime_);
            })
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

    ActionManager_->Reconfigure(newConfig->ActionManager);
    ParameterizedBalancingScheduler_.Reconfigure(
        newConfig->ParameterizedTimeoutOnStart.value_or(Config_->ParameterizedTimeoutOnStart),
        newConfig->ParameterizedTimeout.value_or(Config_->ParameterizedTimeout));

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
        it->second->SetHasUntrackedUnfinishedActions(
            HasUntrackedUnfinishedActions(it->second, &bundle->Attributes()));

        if (isNew) {
            newBundles.push_back(name);
        }
    }

    // Find bundles that are not in the list of bundles (probably deleted)
    // and erase them and all their tables.
    for (auto it = Bundles_.begin(); it != Bundles_.end();) {
        if (currentBundles.contains(it->first)) {
            ++it;
            continue;
        }

        TableRegistry_->RemoveBundle(it->second->GetBundle());
        Bundles_.erase(it++);
    }
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
            if (auto it = GroupPreviousIterationStartTime_.find(groupTag);
                it != GroupPreviousIterationStartTime_.end())
            {
                timePoint = it->second + MinBalanceFrequency;
            } else {
                // First balance of this group in this instance
                // so it's ok to balance if this time is satisfied by the formula.
                // But it is not ok to skip all iterations until the first one fits the formula perfectly.
                timePoint = FirstIterationStartTime_;
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
        YT_LOG_ERROR(ex,
            "Failed to evaluate tablet balancer schedule formula (BundleName: %v, Group: %v)",
            groupTag.first,
            groupTag.second);
        SaveFatalBundleError(bundle->Name, TError(
            NTabletBalancer::EErrorCode::ScheduleFormulaEvaluationFailed,
            "Failed to evaluate tablet balancer schedule formula for group %Qv",
            groupTag.second)
            << ex);
        return false;
    }
}

TTimeFormula TTabletBalancer::GetBundleSchedule(
    const TTabletCellBundlePtr& bundle,
    const TTimeFormula& groupSchedule) const
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

bool TTabletBalancer::CouldAnyBalancingHappen(const TTabletCellBundlePtr& bundle) const
{
    for (const auto& [bundleName, _] : GroupsToMoveOnNextIteration_) {
        if (bundle->Name == bundleName) {
            return true;
        }
    }

    for (const auto& [groupName, config] : bundle->Config->Groups) {
        if (DidBundleBalancingTimeHappen(bundle, {bundle->Name, groupName}, config->Schedule)) {
            return true;
        }
    }
    return false;
}

void TTabletBalancer::BalanceViaMoveInMemory(const TBundleStatePtr& bundleState)
{
    ExecuteMoveIteration(CreateInMemoryMoveIteration(
        bundleState,
        GetOrCrash(bundleState->GetBundle()->Config->Groups, LegacyInMemoryGroupName),
        DynamicConfig_.Acquire()));
}

void TTabletBalancer::BalanceViaMoveOrdinary(const TBundleStatePtr& bundleState)
{
    ExecuteMoveIteration(CreateOrdinaryMoveIteration(
        bundleState,
        GetOrCrash(bundleState->GetBundle()->Config->Groups, LegacyOrdinaryGroupName),
        DynamicConfig_.Acquire()));
}

void TTabletBalancer::BalanceViaMoveParameterized(
    const TBundleStatePtr& bundleState,
    const TGroupName& groupName,
    const TTabletBalancingGroupConfigPtr& groupConfig)
{
    ExecuteMoveIteration(CreateParameterizedMoveIteration(
        groupName,
        bundleState,
        GetParameterizedMetricTracker({bundleState->GetBundle()->Name, groupName}),
        groupConfig,
        DynamicConfig_.Acquire()));
}

void TTabletBalancer::BalanceViaReshardParameterized(const TBundleStatePtr& bundleState, const TGroupName& groupName)
{
    ExecuteReshardIteration(CreateParameterizedReshardIteration(
        bundleState->GetBundle()->Name,
        groupName,
        DynamicConfig_.Acquire()), bundleState);
}

void TTabletBalancer::TryBalanceViaMoveParameterized(const TBundleStatePtr& bundleState, const TGroupName& groupName)
{
    const auto bundle = bundleState->GetBundle();
    const auto& groupConfig = GetOrCrash(bundle->Config->Groups, groupName);

    try {
        BalanceViaMoveParameterized(bundleState, groupName, groupConfig);
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex,
            "Parameterized balancing via move failed with an exception "
            "(BundleName: %v, Group: %v, GroupType: %v, GroupMetric: %v)",
            bundle->Name,
            groupName,
            groupConfig->Type,
            groupConfig->Parameterized->Metric);

        SaveFatalBundleError(bundle->Name, TError(
            NTabletBalancer::EErrorCode::ParameterizedBalancingFailed,
            "Parameterized move balancing for group %Qv failed",
            groupName)
            << ex);
    }
}

void TTabletBalancer::TryBalanceViaReshardParameterized(
    const TBundleStatePtr& bundleState,
    const TGroupName& groupName)
{
    const auto bundle = bundleState->GetBundle();
    TEventTimerGuard timer(GetProfilingTimer(
        {bundle->Name, groupName},
        EBalancingMode::ParameterizedReshard));

    try {
        BalanceViaReshardParameterized(bundleState, groupName);
    } catch (const std::exception& ex) {
        const auto& groupConfig = GetOrCrash(bundle->Config->Groups, groupName);
        YT_LOG_ERROR(ex,
            "Parameterized balancing via reshard failed with an exception "
            "(BundleName: %v, Group: %v, GroupType: %v, GroupMetric: %v)",
            bundle->Name,
            groupName,
            groupConfig->Type,
            groupConfig->Parameterized->Metric);

        SaveFatalBundleError(bundle->Name, TError(
            NTabletBalancer::EErrorCode::ParameterizedBalancingFailed,
            "Parameterized reshard balancing for group %Qv failed",
            groupName)
            << ex);
    }
}

void TTabletBalancer::BalanceViaMove(const TBundleStatePtr& bundleState, const TGroupName& groupName)
{
    if (groupName == LegacyOrdinaryGroupName) {
        BalanceViaMoveOrdinary(bundleState);
    } else if (groupName == LegacyInMemoryGroupName) {
        BalanceViaMoveInMemory(bundleState);
    } else {
        YT_LOG_ERROR("Trying to balance a non-legacy group with "
            "legacy algorithm (BundleName: %v, Group: %v)",
            bundleState->GetBundle()->Name,
            groupName);
    }
}

void TTabletBalancer::ExecuteReshardIteration(
    const IReshardIterationPtr& reshardIteration,
    const TBundleStatePtr& bundleState)
{
    reshardIteration->StartIteration();

    auto groupConfig = GetOrCrash(bundleState->GetBundle()->Config->Groups, reshardIteration->GetGroupName());
    if (!groupConfig->EnableReshard) {
        YT_LOG_DEBUG("Balancing tablets via reshard is disabled (BundleName: %v, Group: %v)",
            reshardIteration->GetBundleName(),
            reshardIteration->GetGroupName());
        return;
    }

    if (!reshardIteration->IsGroupBalancingEnabled(groupConfig)) {
        return;
    }

    reshardIteration->Prepare(bundleState, groupConfig);

    auto tables = reshardIteration->GetTablesToReshard(bundleState->GetBundle());
    Shuffle(tables.begin(), tables.end());

    int actionCount = 0;
    auto groupTag = TGlobalGroupTag(reshardIteration->GetBundleName(), reshardIteration->GetGroupName());

    std::vector<TReshardDescriptor> descriptors;
    for (const auto& table : tables) {
        if (!reshardIteration->IsTableBalancingEnabled(table)) {
            continue;
        }

        auto tableDescriptors = WaitFor(reshardIteration->MergeSplitTable(table, WorkerPool_->GetInvoker()))
            .ValueOrThrow();

        for (auto& descriptor : tableDescriptors) {
            descriptors.emplace_back(std::move(descriptor));
        }
    }

    std::sort(descriptors.begin(), descriptors.end());

    auto descriptorRanges = [&]<class It>(It begin, It end) {
        std::vector<std::pair<It, It>> ranges;
        auto firstSplit = std::find_if(begin, end, [] (const auto& descriptor) {
            return std::get<0>(descriptor.Priority);
        });

        if (begin != firstSplit) {
            ranges.emplace_back(begin, firstSplit);
        }
        if (firstSplit != end) {
            ranges.emplace_back(firstSplit, end);
        }
        return ranges;
    } (descriptors.begin(), descriptors.end());

    int actionCountLimit = reshardIteration->GetDynamicConfig()->MaxActionsPerGroup;
    if (descriptorRanges.size() > 1) {
        actionCountLimit = std::min(
            DivCeil<int>(actionCountLimit, std::ssize(descriptorRanges)),
            reshardIteration->GetDynamicConfig()->MaxActionsPerReshardType);
    }

    auto saveLimitExceededError = [&] (const TReshardDescriptor& descriptor, auto limit) {
        SaveFatalBundleError(groupTag.first, TError(
            NTabletBalancer::EErrorCode::GroupActionLimitExceeded,
            "Group %Qv has exceeded the limit for creating actions. "
            "Failed to schedule reshard action",
            groupTag.second)
            << TErrorAttribute("limit", limit));

        YT_LOG_DEBUG("Group has exceeded the limit for creating actions. "
            "Will not schedule reshard actions anymore "
            "(Group: %v, Limit: %v, CorrelationId: %v, BundleName: %v)",
            groupTag.second,
            limit,
            descriptor.CorrelationId,
            groupTag.first);
    };

    bool actionLimitExceeded = false;
    for (auto [beginIt, endIt] : descriptorRanges) {
        if (actionLimitExceeded) {
            break;
        }

        if (std::distance(beginIt, endIt) > actionCountLimit) {
            endIt = beginIt + actionCountLimit;
            saveLimitExceededError(*endIt, actionCountLimit);
        }

        auto limitedDescriptors = PickPivotsForDescriptors(
            beginIt,
            endIt,
            reshardIteration,
            bundleState,
            groupTag);

        for (const auto& descriptor : limitedDescriptors) {
            auto firstTablet = GetOrCrash(bundleState->Tablets(), descriptor.Tablets[0]);
            auto* table = firstTablet->Table;
            auto firstTabletIndex = firstTablet->Index;
            auto lastTabletIndex = firstTabletIndex + std::ssize(descriptor.Tablets) - 1;

            if (!TryScheduleActionCreation(groupTag, {descriptor})) {
                saveLimitExceededError(descriptor, ActionCountLimiter_.GroupLimit);
                actionLimitExceeded = true;
                break;
            }

            ++actionCount;
            YT_LOG_DEBUG("Reshard action created (TabletIds: %v, TabletCount: %v, "
                "DataSize: %v, TableId: %v, TabletIndexes: %v-%v, TablePath: %v, CorrelationId: %v)",
                descriptor.Tablets,
                descriptor.TabletCount,
                descriptor.DataSize,
                table->Id,
                firstTabletIndex,
                lastTabletIndex,
                table->Path,
                descriptor.CorrelationId);

            reshardIteration->UpdateProfilingCounters(
                table,
                bundleState->GetProfilingCounters(table, reshardIteration->GetGroupName()),
                descriptor);
        }
    }

    if (actionCount > 0) {
        ParameterizedBalancingScheduler_.UpdateBalancingTime(
            {reshardIteration->GetBundleName(), reshardIteration->GetGroupName()});
    }

    reshardIteration->FinishIteration(actionCount);
}

void TTabletBalancer::ExecuteMoveIteration(const IMoveIterationPtr& moveIteration)
{
    moveIteration->StartIteration();

    if (!moveIteration->GetGroupConfig()->EnableMove || !moveIteration->IsGroupBalancingEnabled()) {
        moveIteration->LogDisabledBalancing();
        return;
    }

    auto groupTag = TGlobalGroupTag(moveIteration->GetBundleName(), moveIteration->GetGroupName());
    TEventTimerGuard timer(GetProfilingTimer(
        groupTag,
        moveIteration->GetBalancingMode()));

    moveIteration->Prepare(TableRegistry_);

    auto descriptors = WaitFor(moveIteration->ReassignTablets(WorkerPool_->GetInvoker()))
        .ValueOrThrow();

    int actionCount = 0;
    if (!descriptors.empty()) {
        for (auto descriptor : descriptors) {
            if (!TryScheduleActionCreation(groupTag, descriptor)) {
                SaveFatalBundleError(moveIteration->GetBundleName(), TError(
                    NTabletBalancer::EErrorCode::GroupActionLimitExceeded,
                    "Group %Qv has exceeded the limit for creating actions. "
                    "Failed to schedule %v move action",
                    moveIteration->GetGroupName(),
                    moveIteration->GetActionSubtypeName())
                    << TErrorAttribute("limit", ActionCountLimiter_.GroupLimit));
                break;
            }

            auto tablet = GetOrCrash(moveIteration->GetBundle()->Tablets(), descriptor.TabletId);

            moveIteration->UpdateProfilingCounters(tablet->Table);

            ++actionCount;
            YT_LOG_DEBUG("Move action created (TabletId: %v, CellId: %v, TablePath: %v, CorrelationId: %v)",
                descriptor.TabletId,
                descriptor.TabletCellId,
                tablet->Table->Path,
                descriptor.CorrelationId);

            ApplyMoveTabletAction(tablet, descriptor.TabletCellId);
        }
    }

    if (actionCount > 0) {
        ParameterizedBalancingScheduler_.UpdateBalancingTime(groupTag);
    }

    moveIteration->FinishIteration(actionCount);
}

std::vector<TReshardDescriptor> TTabletBalancer::PickPivotsForDescriptors(
    TReshardDescriptorIt begin,
    TReshardDescriptorIt end,
    const IReshardIterationPtr& reshardIteration,
    const TBundleStatePtr& bundleState,
    TGlobalGroupTag groupTag)
{
    bool pickPivotKeys = reshardIteration->IsPickPivotKeysEnabled(bundleState->GetBundle()->Config);
    if (!pickPivotKeys) {
        return {begin, end};
    }

    bool enableVerboseLogging = reshardIteration->GetDynamicConfig()->EnableReshardVerboseLogging ||
        bundleState->GetBundle()->Config->EnableVerboseLogging;

    std::vector<TFuture<std::vector<TLegacyOwningKey>>> futures;
    std::vector<TReshardDescriptor> descriptors;
    std::vector<TReshardDescriptorIt> descriptorsToPick;
    std::vector<const TTablet*> firstTablets;
    for (auto descriptorIt = begin; descriptorIt < end; ++descriptorIt) {
        if (pickPivotKeys) {
            auto firstTablet = GetOrCrash(bundleState->Tablets(), descriptorIt->Tablets[0]);
            auto future = PickReshardPivotKeysIfNeeded(
                descriptorIt,
                firstTablet->Table,
                firstTablet->Index,
                firstTablet->Index + std::ssize(descriptorIt->Tablets) - 1,
                reshardIteration->GetDynamicConfig()->ReshardSlicingAccuracy,
                enableVerboseLogging);

            if (future) {
                futures.emplace_back(std::move(future));
                firstTablets.emplace_back(firstTablet.Get());
                descriptorsToPick.push_back(descriptorIt);
                continue;
            }
        }

        descriptors.emplace_back(std::move(*descriptorIt));
    }

    YT_LOG_DEBUG("Pick pivot keys started (BundleName: %v, Group: %v, DescriptorsToPickCount: %v)",
        groupTag.first,
        groupTag.second,
        std::ssize(descriptorsToPick));

    auto responses = WaitFor(AllSet(std::move(futures))).ValueOrThrow();

    YT_LOG_DEBUG("Pick pivot keys finished (BundleName: %v, Group: %v)",
        groupTag.first,
        groupTag.second);

    for (int index = 0; index < std::ssize(descriptorsToPick); ++index) {
        auto descriptorIt = descriptorsToPick[index];
        auto* firstTablet = firstTablets[index];
        auto* table = firstTablet->Table;
        auto firstTabletIndex = firstTablet->Index;
        auto lastTabletIndex = firstTabletIndex + std::ssize(descriptorIt->Tablets) - 1;
        auto rspOrError = responses[index];

        if (!rspOrError.IsOK()) {
            YT_LOG_ERROR(rspOrError,
                "Failed to pick pivot keys for reshard action "
                "(TabletIds: %v, TabletCount: %v, DataSize: %v, "
                "TableId: %v, TabletIndexes: %v-%v, CorrelationId: %v)",
                descriptorIt->Tablets,
                descriptorIt->TabletCount,
                descriptorIt->DataSize,
                table->Id,
                firstTabletIndex,
                lastTabletIndex,
                descriptorIt->CorrelationId);

            PickPivotFailures_.Increment(1);

            if (reshardIteration->GetDynamicConfig()->CancelActionIfPickPivotKeysFails) {
                YT_LOG_DEBUG(rspOrError,
                    "Cancelled tablet action creation because pick pivot keys failed "
                    "(TabletIds: %v, TabletCount: %v, DataSize: %v, TableId: %v, "
                    "TabletIndexes: %v-%v, CorrelationId: %v)",
                    descriptorIt->Tablets,
                    descriptorIt->TabletCount,
                    descriptorIt->DataSize,
                    table->Id,
                    firstTabletIndex,
                    lastTabletIndex,
                    descriptorIt->CorrelationId);
                continue;
            }
        } else {
            descriptorIt->PivotKeys = std::move(rspOrError.ValueOrThrow());
        }

        descriptors.emplace_back(std::move(*descriptorIt));
    }
    return descriptors;
}

void TTabletBalancer::BalanceViaReshard(const TBundleStatePtr& bundleState, const TGroupName& groupName)
{
    TEventTimerGuard timer(GetProfilingTimer(
        {bundleState->GetBundle()->Name, groupName},
        EBalancingMode::Reshard));

    ExecuteReshardIteration(CreateSizeReshardIteration(
        bundleState->GetBundle()->Name,
        groupName,
        DynamicConfig_.Acquire()), bundleState);
}

void TTabletBalancer::SaveFatalBundleError(const TString& bundleName, TError error) const
{
    auto it = BundleErrors_.emplace(bundleName, TBundleErrors{}).first;
    SaveBundleError(&it->second.FatalErrors, std::move(error));
}

void TTabletBalancer::SaveRetryableBundleError(const TString& bundleName, TError error) const
{
    auto it = BundleErrors_.emplace(bundleName, TBundleErrors{}).first;
    SaveBundleError(&it->second.RetryableErrors, std::move(error));
}

void TTabletBalancer::SaveBundleError(std::deque<TError>* errors, TError error) const
{
    errors->emplace_back(std::move(error));

    if (errors->size() > MaxSavedErrorCount) {
        errors->pop_front();
    }
}

void TTabletBalancer::RemoveBundleErrorsByTtl(TDuration ttl) const
{
    auto currentTime = Now();
    THashSet<TString> relevantBundles;
    for (auto& [bundleName, errors] : BundleErrors_) {
        while (!errors.FatalErrors.empty()) {
            const auto& error = errors.FatalErrors.front();
            if (error.HasDatetime() && error.GetDatetime() + ttl < currentTime) {
                errors.FatalErrors.pop_front();
                continue;
            }
            break;
        }

        if (!errors.FatalErrors.empty() || !errors.RetryableErrors.empty()) {
            relevantBundles.insert(bundleName);
        }
    }

    DropMissingKeys(BundleErrors_, relevantBundles);
}

void TTabletBalancer::RemoveRetryableErrorsOnSuccessfulIteration(const TString& bundleName) const
{
    auto it = BundleErrors_.find(bundleName);
    if (it != BundleErrors_.end()) {
        it->second.RetryableErrors.clear();

        if (it->second.FatalErrors.empty()) {
            BundleErrors_.erase(it);
        }
    }
}

TFuture<std::vector<TLegacyOwningKey>> TTabletBalancer::PickReshardPivotKeysIfNeeded(
    TReshardDescriptor* descriptor,
    const TTable* table,
    int firstTabletIndex,
    int lastTabletIndex,
    std::optional<double> slicingAccuracy,
    bool enableVerboseLogging) const
{
    enableVerboseLogging |= table->TableConfig->EnableVerboseLogging;
    if (descriptor->TabletCount == 1) {
        // Do not pick pivot keys for merge actions.
        YT_LOG_DEBUG_IF(enableVerboseLogging,
            "Skip picking pivot keys because this is a merge action (CorrelationId: %v)",
            descriptor->CorrelationId);
        return {};
    }

    auto options = TReshardTableOptions{
        .EnableSlicing = true,
        .SlicingAccuracy = slicingAccuracy,
    };

    options.FirstTabletIndex = firstTabletIndex;
    options.LastTabletIndex = lastTabletIndex;

    int partitionCount = 0;
    for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
        partitionCount += table->Tablets[index]->Statistics.PartitionCount;
    }

    if (partitionCount >= 2 * descriptor->TabletCount) {
        // Do not pick pivot keys if there are enough partitions
        // for the master to perform reshard itself.
        YT_LOG_DEBUG_IF(enableVerboseLogging,
            "Skip picking pivot keys because there are enough partitions "
            "(PartitionCount: %v, TabletCount: %v, CorrelationId: %v)",
            partitionCount,
            descriptor->TabletCount,
            descriptor->CorrelationId);
        return {};
    }

    return BIND(
        PickPivotKeysWithSlicing,
        Bootstrap_->GetClient(),
        table->Path,
        descriptor->TabletCount,
        options,
        Logger(),
        enableVerboseLogging || table->TableConfig->EnableVerboseLogging)
        .AsyncVia(PivotPickerPool_->GetInvoker())
        .Run();
}

TTableParameterizedMetricTrackerPtr TTabletBalancer::GetParameterizedMetricTracker(
    const TGlobalGroupTag& groupTag)
{
    auto it = GroupToParameterizedMetricTracker_.find(groupTag);
    if (it == GroupToParameterizedMetricTracker_.end()) {
        auto profiler = TabletBalancerProfiler()
            .WithSparse()
            .WithTag("tablet_cell_bundle", groupTag.first)
            .WithTag("group", groupTag.second);
        auto metricTracker = New<TTableParameterizedMetricTracker>();
        metricTracker->BeforeMetric = profiler.Gauge("/parameterized_metric/before");
        metricTracker->AfterMetric = profiler.Gauge("/parameterized_metric/after");

        return EmplaceOrCrash(
            GroupToParameterizedMetricTracker_,
            groupTag,
            std::move(metricTracker))->second;
    }

    return it->second;
}

TEventTimer& TTabletBalancer::GetProfilingTimer(const TGlobalGroupTag& groupTag, EBalancingMode type) const
{
    THashMap<EBalancingMode, TEventTimer>* groupTimers;
    auto timersIt = IterationProfilingTimers_.find(groupTag);
    if (timersIt != IterationProfilingTimers_.end()) {
        groupTimers = &timersIt->second;
    } else {
        groupTimers = &EmplaceOrCrash(
            IterationProfilingTimers_,
            groupTag,
            THashMap<EBalancingMode, TEventTimer>())->second;
    }

    auto eventIt = groupTimers->find(type);
    if (eventIt != groupTimers->end()) {
        return eventIt->second;
    }

    return EmplaceOrCrash(*groupTimers, type, TabletBalancerProfiler()
        .WithTag("tablet_cell_bundle", groupTag.first)
        .WithTag("group", groupTag.second)
        .WithTag("type", ToString(type))
        .Timer("/group_iteration_time"))->second;
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
