#include "action_manager.h"
#include "bootstrap.h"
#include "config.h"
#include "helpers.h"
#include "private.h"
#include "tablet_action.h"

#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

static constexpr int MaxQueueSize = 1000;

////////////////////////////////////////////////////////////////////////////////

class TActionManager
    : public IActionManager
{
public:
    TActionManager(
        TActionManagerConfigPtr config,
        NApi::NNative::IClientPtr client,
        IBootstrap* bootstrap,
        IMulticellThrottlerPtr throttler);

    void ScheduleActionCreation(const std::string& bundleName, const TActionDescriptor& descriptor) override;
    void CreateActions(const std::string& bundleName) override;

    bool HasUnfinishedActions(
        const std::string& bundleName,
        const std::vector<TTabletActionId>& knownBundleActionIds) const override;

    bool HasPendingActions(const std::string& bundleName) const override;
    void CancelPendingActions(const std::string& bundleName) override;

    int GetPendingActionCount(const std::string& bundleName) const override;
    TFuture<void> WaitForAllActions() override;

    void Start(TTransactionId prerequisiteTransactionId, TDryRunConfigPtr dryRunConfig) override;
    void Stop() override;

    void Reconfigure(const TActionManagerConfigPtr& config) override;

private:
    struct TBundleProfilingCounters
    {
        NProfiling::TGauge RunningActions;
        NProfiling::TCounter FailedActions;
        NProfiling::TCounter StartedSmoothMovementActions;
        NProfiling::TCounter FailedAtStartSmoothMovementActions;
        NProfiling::TCounter FailedAtRuntimeSmoothMovementActions;
    };

    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const IMulticellThrottlerPtr MasterRequestThrottler_;

    TActionManagerConfigPtr Config_;
    NConcurrency::TPeriodicExecutorPtr PollExecutor_;
    NConcurrency::TPeriodicExecutorPtr CreateActionExecutor_;
    TDryRunConfigPtr DryRunConfig_;
    TPromise<void> AllActionsFinished_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, PendingActionsLock_);
    THashMap<std::string, std::deque<TActionDescriptor>> PendingActionDescriptors_;

    // Only to use from Invoker.
    THashMap<std::string, THashSet<TTabletActionPtr>> RunningActions_;
    THashMap<std::string, std::deque<TTabletActionPtr>> FinishedActions_;
    THashMap<std::string, TBundleProfilingCounters> ProfilingCounters_;

    // For bundles with confirmed pending actions we cannot add more pending actions.
    std::deque<std::string> BundlesWithPendingActions_;
    THashMap<std::string, TInstant> PendingActionsDeadline_;

    // Pending inplace reshards waiting for tablet move to complete.
    // Running actions may be absent from this set if the corresponding reshard action
    // has been canceled, e.g. due to timeout or failure.
    // Protected by PendingActionsLock_.
    THashMap<std::string, THashSet<TReshardDescriptorPtr>> PendingCrossCellReshards_;
    THashMap<TTabletId, TReshardDescriptorPtr> TabletToPendingCrossCellReshard_;

    bool Started_ = false;
    TTransactionId PrerequisiteTransactionId_ = NullTransactionId;

    void Poll();
    void TryPoll();

    int CreatePendingBundleActions(const std::string& bundleName, int actionCountLimit);
    void CreatePendingActions();
    void TryCreatePendingActions();

    int GetRunningActionCount() const;

    bool AreAllActionsKnown(
        const std::string& bundleName,
        std::vector<TTabletActionId> actionIds) const;

    IAttributeDictionaryPtr MakeActionAttributes(const TActionDescriptor& descriptor);

    bool IsSmoothMovementAction(const TActionDescriptor& descriptor) const;

    void MoveFinishedActionsFromRunningToFinished();

    void OnPreliminaryMoveFinished(const std::string& bundleName, const TTabletActionPtr& action);

    void RemovePendingCrossCellReshard(const std::string& bundleName, const TReshardDescriptorPtr& descriptor);

    const TBundleProfilingCounters& GetOrCreateProfilingCounters(const std::string& bundleName);

    void DropFrontBundleWithPendingActions(const std::string& bundleName);
};

////////////////////////////////////////////////////////////////////////////////

TActionManager::TActionManager(
    TActionManagerConfigPtr config,
    NApi::NNative::IClientPtr client,
    IBootstrap* bootstrap,
    IMulticellThrottlerPtr throttler)
    : Client_(std::move(client))
    , Invoker_(bootstrap->GetControlInvoker())
    , MasterRequestThrottler_(throttler)
    , Config_(std::move(config))
    , PollExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TActionManager::TryPoll, MakeWeak(this)),
        Config_->TabletActionPollingPeriod))
    , CreateActionExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TActionManager::TryCreatePendingActions, MakeWeak(this)),
        Config_->TabletActionPollingPeriod))
    , DryRunConfig_(New<TDryRunConfig>())
{ }

void TActionManager::ScheduleActionCreation(const std::string& bundleName, const TActionDescriptor& descriptor)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(PendingActionsLock_);

    if (const auto* reshardDescriptor = std::get_if<TReshardDescriptor>(&descriptor);
        reshardDescriptor && !reshardDescriptor->PendingTabletIds.empty())
    {
        const auto& inplaceReshardDescriptor = EmplaceOrCrash(
            PendingCrossCellReshards_[bundleName],
            New<TReshardDescriptor>(*reshardDescriptor));

        bool smoothMovement = reshardDescriptor->UseSmoothMovementToUniteTablets;
        for (auto tabletId : reshardDescriptor->PendingTabletIds) {
            PendingActionDescriptors_[bundleName].emplace_back(TMoveDescriptor{
                .TabletId = tabletId,
                .TabletCellId = reshardDescriptor->TargetCellId,
                .CorrelationId = reshardDescriptor->CorrelationId,
                .Smooth = smoothMovement,
            });
            EmplaceOrCrash(TabletToPendingCrossCellReshard_, tabletId, *inplaceReshardDescriptor);
        }

        YT_TLOG_DEBUG("Added pending cross-cell inplace reshard")
            .With("BundleName", bundleName)
            .With("CorrelationId", reshardDescriptor->CorrelationId)
            .With("TargetCellId", reshardDescriptor->TargetCellId)
            .With("PendingTabletCount", reshardDescriptor->PendingTabletIds.size());

        return;
    }

    PendingActionDescriptors_[bundleName].emplace_back(descriptor);
}

void TActionManager::CreateActions(const std::string& bundleName)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    THROW_ERROR_EXCEPTION_UNLESS(
        Started_,
        "Action manager instance has already stopped");

    if (RunningActions_.contains(bundleName)) {
        THROW_ERROR_EXCEPTION(
            "Cannot create new actions since bundle %v has unfinished actions",
            bundleName);
    }

    {
        auto guard = ReaderGuard(PendingActionsLock_);
        YT_VERIFY(PendingActionDescriptors_.contains(bundleName));
    }

    BundlesWithPendingActions_.push_back(bundleName);
    PendingActionsDeadline_[bundleName] = TInstant::Now() + Config_->TabletActionCreationTimeout;
}

void TActionManager::TryCreatePendingActions()
{
    TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("CreatePendingActions"));
    try {
        if (DryRunConfig_->IsDryRun && !DryRunConfig_->CreateTabletActions) {
            YT_TLOG_INFO("Skip creation of tablet actions in pure dry run mode")
                .With("DryRunConfig", ConvertToYsonString(DryRunConfig_, EYsonFormat::Text));
            return;
        }

        CreatePendingActions();
    } catch (const std::exception& ex) {
        YT_TLOG_ERROR("Failed to create pending actions")
            .With(ex);
    }
}

void TActionManager::CreatePendingActions()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    auto runningActionCount = GetRunningActionCount();
    if (BundlesWithPendingActions_.empty()) {
        YT_TLOG_DEBUG("No action to create in any bundle");
        return;
    }

    if (runningActionCount > Config_->CreateActionBatchSizeLimit / 2) {
        YT_TLOG_DEBUG("Too many running actions, will not create more at the moment")
            .With("ActionCount", runningActionCount)
            .With("SoftLimit", Config_->CreateActionBatchSizeLimit / 2)
            .With("HardLimit", Config_->CreateActionBatchSizeLimit);
        return;
    }

    auto iterationStartTime = TInstant::Now();

    YT_TLOG_DEBUG("Started creating pending actions")
        .With("IterationStartTime", iterationStartTime)
        .With("PendingBundleCount", std::ssize(BundlesWithPendingActions_))
        .With("ActionCreationTimeout", Config_->TabletActionCreationTimeout);

    int actionCount = 0;
    while (actionCount < Config_->CreateActionBatchSizeLimit && !BundlesWithPendingActions_.empty()) {
        auto bundleName = BundlesWithPendingActions_.front();
        if (PendingActionsDeadline_[bundleName] < iterationStartTime) {
            auto guard = WriterGuard(PendingActionsLock_);
            YT_TLOG_WARNING("Actions were dropped due to timeout")
                .With("Bundle", bundleName)
                .With("ActionCount", std::ssize(PendingActionDescriptors_[bundleName]))
                .With("Timeout", Config_->TabletActionCreationTimeout);

            DropFrontBundleWithPendingActions(bundleName);
            continue;
        }

        actionCount += CreatePendingBundleActions(bundleName, Config_->CreateActionBatchSizeLimit - actionCount);

        {
            auto guard = WriterGuard(PendingActionsLock_);
            if (PendingActionDescriptors_[bundleName].empty()) {
                DropFrontBundleWithPendingActions(bundleName);
            }
        }
    }

    YT_TLOG_DEBUG("Creating pending actions finished")
        .With("ActionCount", actionCount)
        .With("PendingBundleCount", std::ssize(BundlesWithPendingActions_));
}

int TActionManager::GetRunningActionCount() const
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    return std::accumulate(
        RunningActions_.begin(),
        RunningActions_.end(),
        0,
        [] (int x, const auto& pair) {
            return x + std::ssize(pair.second);
        });
}

int TActionManager::CreatePendingBundleActions(const std::string& bundleName, int actionCountLimit)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_TLOG_DEBUG("Creating pending actions")
        .With("Bundle", bundleName)
        .With("ActionCountLimit", actionCountLimit);

    std::deque<TActionDescriptor> descriptors;

    {
        auto guard = ReaderGuard(PendingActionsLock_);
        descriptors = PendingActionDescriptors_[bundleName];
    }

    actionCountLimit = std::min<int>(actionCountLimit, std::ssize(descriptors));

    std::vector<TFuture<NObjectClient::TObjectId>> futures;

    int createdSmoothMovementActionCount = 0;

    for (int index = 0; index < actionCountLimit; ++index) {
        auto attributes = MakeActionAttributes(descriptors[index]);
        YT_TLOG_DEBUG("Creating tablet action")
            .With("Attributes", ConvertToYsonString(attributes, EYsonFormat::Text))
            .With("BundleName", bundleName);
        TCreateObjectOptions options;
        options.Attributes = std::move(attributes);
        if (!DryRunConfig_->IsDryRun) {
            options.PrerequisiteTransactionIds.push_back(PrerequisiteTransactionId_);
        }
        futures.emplace_back(Client_->CreateObject(EObjectType::TabletAction, std::move(options)));

        createdSmoothMovementActionCount += IsSmoothMovementAction(descriptors[index]);
    }

    if (createdSmoothMovementActionCount) {
        GetOrCreateProfilingCounters(bundleName).StartedSmoothMovementActions.Increment(
            createdSmoothMovementActionCount);
    }

    auto responses = WaitFor(AllSet(std::move(futures)))
        .ValueOrThrow();

    THashSet<TTabletActionPtr> runningActions;
    std::vector<TActionDescriptor> fallbackDescriptors;
    for (int index = 0; index < actionCountLimit; ++index) {
        auto rspOrError = responses[index];
        if (!rspOrError.IsOK()) {
            YT_TLOG_WARNING("Failed to create tablet action")
                .With("BundleName", bundleName)
                .With("ActionDescriptor", descriptors[index])
                .With(rspOrError);

            // Retry smooth movement actions with regular move.
            if (IsSmoothMovementAction(descriptors[index])) {
                auto moveDescriptor = std::get<TMoveDescriptor>(descriptors[index]);

                YT_TLOG_DEBUG("Smooth movement action failed, scheduling regular action creation instead")
                    .With("BundleName", bundleName)
                    .With("TabletId", moveDescriptor.TabletId)
                    .With("CorrelationId", moveDescriptor.CorrelationId);

                GetOrCreateProfilingCounters(bundleName).FailedAtStartSmoothMovementActions.Increment();

                moveDescriptor.Smooth = false;
                fallbackDescriptors.push_back(moveDescriptor);
            }

            continue;
        }

        auto actionId = ConvertTo<TTabletActionId>(rspOrError.Value());

        YT_TLOG_DEBUG("Created tablet action")
            .With("TabletActionId", actionId)
            .With("BundleName", bundleName)
            .With("ActionDescriptor", descriptors[index]);
        EmplaceOrCrash(runningActions, New<TTabletAction>(actionId, descriptors[index]));
    }

    {
        auto guard = WriterGuard(PendingActionsLock_);
        auto& pendingActions = GetOrCrash(PendingActionDescriptors_, bundleName);
        YT_VERIFY(actionCountLimit <= std::ssize(pendingActions));
        pendingActions.erase(pendingActions.begin(), pendingActions.begin() + actionCountLimit);
        pendingActions.insert(pendingActions.end(), fallbackDescriptors.begin(), fallbackDescriptors.end());
    }

    int createdActionCount = std::ssize(runningActions);
    if (!runningActions.empty()) {
        auto it = RunningActions_.emplace(bundleName, THashSet<TTabletActionPtr>{}).first;
        it->second.insert(runningActions.begin(), runningActions.end());
        GetOrCreateProfilingCounters(bundleName).RunningActions.Update(std::ssize(it->second));
    }

    YT_TLOG_INFO("Created tablet actions for bundle")
        .With("ActionCount", createdActionCount)
        .With("BundleName", bundleName);

    return createdActionCount;
}

bool TActionManager::HasUnfinishedActions(
    const std::string& bundleName,
    const std::vector<TTabletActionId>& knownBundleActionIds) const
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    auto guard = ReaderGuard(PendingActionsLock_);

    return PendingActionDescriptors_.contains(bundleName) ||
        PendingCrossCellReshards_.contains(bundleName) ||
        RunningActions_.contains(bundleName) ||
        !AreAllActionsKnown(bundleName, knownBundleActionIds);
}

bool TActionManager::AreAllActionsKnown(
    const std::string& bundleName,
    std::vector<TTabletActionId> actionIds) const
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    if (DryRunConfig_->IsDryRun) {
        YT_TLOG_DEBUG("Skip checking whether all actions are known in dry run mode");
        return true;
    }

    auto collectActionIds = [] (const auto& actions) {
        THashSet<TTabletActionId> actionIds;
        for (const TTabletActionPtr& action : actions) {
            actionIds.insert(action->GetId());
        }
        return actionIds;
    };

    auto filterKnownActionIds = [&] (const THashSet<TTabletActionId>& knownActionIds) {
        std::erase_if(actionIds, [&] (const TTabletActionId& actionId) {
            return knownActionIds.contains(actionId);
        });
    };

    if (auto it = RunningActions_.find(bundleName); it != RunningActions_.end()) {
        filterKnownActionIds(collectActionIds(it->second));
    }

    if (auto it = FinishedActions_.find(bundleName); it != FinishedActions_.end()) {
        filterKnownActionIds(collectActionIds(it->second));
    }

    return actionIds.empty();
}

bool TActionManager::HasPendingActions(const std::string& bundleName) const
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    auto guard = ReaderGuard(PendingActionsLock_);
    return PendingActionDescriptors_.contains(bundleName);
}

void TActionManager::CancelPendingActions(const std::string& bundleName)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    auto guard = WriterGuard(PendingActionsLock_);
    PendingActionDescriptors_.erase(bundleName);

    if (PendingCrossCellReshards_.contains(bundleName)) {
        for (const auto& reshardDescriptor : PendingCrossCellReshards_[bundleName]) {
            for (auto tabletId : reshardDescriptor->PendingTabletIds) {
                TabletToPendingCrossCellReshard_.erase(tabletId);
            }
        }

        PendingCrossCellReshards_.erase(bundleName);
    }
}

TFuture<void> TActionManager::WaitForAllActions()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_VERIFY(DryRunConfig_->IsDryRun);
    AllActionsFinished_ = NewPromise<void>();

    return AllActionsFinished_;
}

int TActionManager::GetPendingActionCount(const std::string& bundleName) const
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_VERIFY(DryRunConfig_->IsDryRun);

    if (BundlesWithPendingActions_.empty()) {
        return 0;
    }

    auto guard = ReaderGuard(PendingActionsLock_);
    return std::ssize(GetOrCrash(PendingActionDescriptors_, bundleName)) +
        std::ssize(GetOrDefault(PendingCrossCellReshards_, bundleName));
}

void TActionManager::Start(TTransactionId prerequisiteTransactionId, TDryRunConfigPtr dryRunConfig)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    if (dryRunConfig) {
        YT_VERIFY(dryRunConfig->IsDryRun);
        DryRunConfig_ = std::move(dryRunConfig);
    }

    YT_TLOG_INFO("Starting tablet action manager")
        .With("PrerequisiteTransactionId", prerequisiteTransactionId);

    Started_ = true;

    YT_VERIFY(prerequisiteTransactionId || DryRunConfig_->IsDryRun);
    PrerequisiteTransactionId_ = prerequisiteTransactionId;

    auto guard = WriterGuard(PendingActionsLock_);

    RunningActions_.clear();
    PendingActionDescriptors_.clear();
    PendingCrossCellReshards_.clear();
    TabletToPendingCrossCellReshard_.clear();

    PollExecutor_->Start();
    CreateActionExecutor_->Start();
}

void TActionManager::Stop()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_TLOG_INFO("Stopping tablet action manager");

    Started_ = false;
    PrerequisiteTransactionId_ = NullTransactionId;

    YT_UNUSED_FUTURE(PollExecutor_->Stop());
    YT_UNUSED_FUTURE(CreateActionExecutor_->Stop());

    YT_TLOG_INFO("Tablet action manager stopped");
}

void TActionManager::Reconfigure(const TActionManagerConfigPtr& config)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    Config_ = config;
    PollExecutor_->SetPeriod(Config_->TabletActionPollingPeriod);
    CreateActionExecutor_->SetPeriod(Config_->TabletActionPollingPeriod);
}

void TActionManager::TryPoll()
{
    TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("ActionManager"));
    try {
        Poll();
    } catch (const std::exception& ex) {
        YT_TLOG_ERROR("Failed to poll actions")
            .With(ex);
    }
}

void TActionManager::Poll()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_TLOG_INFO("Start checking tablet action states");

    THashSet<TTabletActionId> actionIds;
    for (const auto& [bundleName, actions] : RunningActions_) {
        for (const auto& action : actions) {
            actionIds.insert(action->GetId());
        }
    }

    YT_TLOG_DEBUG("Started fetching tablet action states")
        .With("ActionCount", actionIds.size());

    static const std::vector<std::string> attributeKeys{"state", "error"};
    auto actionToAttributes = FetchAttributes(Client_, actionIds, attributeKeys, MasterRequestThrottler_);

    YT_TLOG_DEBUG("Finished fetching tablet action states")
        .With("ActionCount", actionToAttributes.size());

    for (const auto& [bundle, actions] : RunningActions_) {
        for (const auto& action : actions) {
            if (auto it = actionToAttributes.find(action->GetId()); it != actionToAttributes.end()) {
                const auto& attributes = it->second;
                auto state = attributes->Get<ETabletActionState>("state");
                action->SetState(state);

                YT_TLOG_DEBUG("Tablet action state fetched")
                    .With("TabletActionId", action->GetId())
                    .With("State", state)
                    .With("CorrelationId", action->GetCorrelationId());
                if (attributes->Contains("error")) {
                    auto error = attributes->Get<TError>("error");
                    action->Error() = error;
                    YT_TLOG_WARNING("Tablet action failed")
                        .With("TabletActionId", action->GetId())
                        .With("CorrelationId", action->GetCorrelationId())
                        .With("Kind", action->GetKind())
                        .With(error);
                }
            } else if (!actionIds.contains(action->GetId())) {
                YT_TLOG_DEBUG("Tablet action status is unknown")
                    .With("TabletActionId", action->GetId())
                    .With("Kind", action->GetKind())
                    .With("State", action->GetState())
                    .With("CorrelationId", action->GetCorrelationId());
            } else {
                action->SetLost(true);
                YT_TLOG_DEBUG("Tablet action is lost")
                    .With("TabletActionId", action->GetId())
                    .With("Kind", action->GetKind())
                    .With("CorrelationId", action->GetCorrelationId());
            }
        }
    }

    MoveFinishedActionsFromRunningToFinished();

    if (DryRunConfig_->IsDryRun &&
        BundlesWithPendingActions_.empty() &&
        RunningActions_.empty() &&
        AllActionsFinished_) [[unlikely]]
    {
        auto guard = ReaderGuard(PendingActionsLock_);
        YT_VERIFY(PendingActionDescriptors_.empty());
        YT_VERIFY(PendingCrossCellReshards_.empty());

        AllActionsFinished_.Set();
    }
}

void TActionManager::MoveFinishedActionsFromRunningToFinished()
{
    THashSet<std::string> relevantBundles;

    for (auto& [bundleName, runningActions] : RunningActions_) {
        auto& finishedActions = FinishedActions_[bundleName];
        int failedActionCount = 0;
        int failedSmoothMovementActionCount = 0;

        for (auto it = runningActions.begin(); it != runningActions.end(); ) {
            const auto& action = *it;
            if (action->IsFinished()) {
                OnPreliminaryMoveFinished(bundleName, action);

                if (action->GetState() == ETabletActionState::Failed) {
                    ++failedActionCount;

                    if (action->GetKind() == ETabletActionKind::SmoothMove) {
                        ++failedSmoothMovementActionCount;
                    }
                }

                finishedActions.push_back(action);
                if (std::ssize(finishedActions) > MaxQueueSize) {
                    finishedActions.pop_front();
                }
                runningActions.erase(it++);
            } else {
                ++it;
            }
        }

        if (!runningActions.empty()) {
            relevantBundles.emplace(bundleName);
        }

        const auto& profilingCounters = GetOrCreateProfilingCounters(bundleName);
        profilingCounters.RunningActions.Update(runningActions.size());
        profilingCounters.FailedActions.Increment(failedActionCount);
        profilingCounters.FailedAtRuntimeSmoothMovementActions.Increment(
            failedSmoothMovementActionCount);
    }

    DropMissingKeys(RunningActions_, relevantBundles);
}

const TActionManager::TBundleProfilingCounters& TActionManager::GetOrCreateProfilingCounters(const std::string& bundleName)
{
    if (auto it = ProfilingCounters_.find(bundleName); it != ProfilingCounters_.end()) {
        return it->second;
    }

    auto profiler = TabletBalancerProfiler()
        .WithTag("tablet_cell_bundle", bundleName)
        .WithPrefix("/action_manager");

    return EmplaceOrCrash(ProfilingCounters_, bundleName, TBundleProfilingCounters{
        .RunningActions = profiler.Gauge("/running_actions"),
        .FailedActions = profiler.Counter("/failed_actions"),
        .StartedSmoothMovementActions = profiler
            .WithSparse()
            .Counter("/started_smooth_movement_actions"),
        .FailedAtStartSmoothMovementActions = profiler
            .WithSparse()
            .WithTag("at_start", "true")
            .Counter("/failed_smooth_movement_actions"),
        .FailedAtRuntimeSmoothMovementActions = profiler
            .WithSparse()
            .WithTag("at_start", "false")
            .Counter("/failed_smooth_movement_actions"),
    })->second;
}

IAttributeDictionaryPtr TActionManager::MakeActionAttributes(const TActionDescriptor& descriptor)
{
    auto attributes = CreateEphemeralAttributes();
    Visit(descriptor,
        [&] (const TMoveDescriptor& descriptor) {
            attributes->Set("kind", descriptor.Smooth ? "smooth_move" : "move");
            attributes->Set("tablet_ids", std::vector<TTabletId>{descriptor.TabletId});
            attributes->Set("cell_ids", std::vector<TTabletCellId>{descriptor.TabletCellId});
            attributes->Set("correlation_id", descriptor.CorrelationId);
        },
        [&] (const TReshardDescriptor& descriptor) {
            attributes->Set("kind", "reshard");
            attributes->Set("tablet_ids", descriptor.Tablets);
            attributes->Set("correlation_id", descriptor.CorrelationId);
            attributes->Set("inplace_reshard", descriptor.Inplace);

            if (!descriptor.PivotKeys.empty()) {
                attributes->Set("pivot_keys", descriptor.PivotKeys);
            } else {
                attributes->Set("tablet_count", descriptor.TabletCount);
            }
        });
    attributes->Set("expiration_timeout", Config_->TabletActionExpirationTimeout);
    return attributes;
}

bool TActionManager::IsSmoothMovementAction(const TActionDescriptor& descriptor) const
{
    auto moveDescriptor = std::get_if<TMoveDescriptor>(&descriptor);
    return moveDescriptor && moveDescriptor->Smooth;
}

void TActionManager::RemovePendingCrossCellReshard(const std::string& bundleName, const TReshardDescriptorPtr& descriptor)
{
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(PendingActionsLock_);
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    PendingCrossCellReshards_[bundleName].erase(descriptor);
    if (PendingCrossCellReshards_[bundleName].empty()) {
        PendingCrossCellReshards_.erase(bundleName);
    }
}

void TActionManager::DropFrontBundleWithPendingActions(const std::string& bundleName)
{
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(PendingActionsLock_);
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_VERIFY(BundlesWithPendingActions_.front() == bundleName);
    BundlesWithPendingActions_.pop_front();
    EraseOrCrash(PendingActionDescriptors_, bundleName);
}

void TActionManager::OnPreliminaryMoveFinished(const std::string& bundleName, const TTabletActionPtr& action)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    if (!action->IsMove()) {
        return;
    }

    auto tabletId = action->TabletIds().front();

    TReshardDescriptorPtr reshardDescriptor;
    {
        auto guard = WriterGuard(PendingActionsLock_);

        auto it = TabletToPendingCrossCellReshard_.find(tabletId);
        if (it == TabletToPendingCrossCellReshard_.end()) {
            return;
        }

        reshardDescriptor = it->second;

        if (action->GetState() == ETabletActionState::Failed) {
            YT_TLOG_DEBUG("Move failed, canceling pending cross-cell inplace reshard")
                .With("BundleName", bundleName)
                .With("TabletId", tabletId)
                .With("TabletsToReshard", reshardDescriptor->Tablets);

            for (auto pendingTabletId : reshardDescriptor->PendingTabletIds) {
                TabletToPendingCrossCellReshard_.erase(pendingTabletId);
            }

            RemovePendingCrossCellReshard(bundleName, reshardDescriptor);
            return;
        }

        EraseOrCrash(reshardDescriptor->PendingTabletIds, tabletId);
        TabletToPendingCrossCellReshard_.erase(it);

        if (!reshardDescriptor->PendingTabletIds.empty()) {
            return;
        }

        RemovePendingCrossCellReshard(bundleName, reshardDescriptor);
    }

    if (PendingActionsDeadline_[bundleName] < Now()) {
        YT_TLOG_DEBUG("Pending cross-cell inplace reshard expired")
            .With("BundleName", bundleName);
        return;
    }

    YT_TLOG_DEBUG("All tablets arrived on target cell, scheduling intra-cell inplace reshard")
        .With("BundleName", bundleName)
        .With("TabletsToReshard", reshardDescriptor->Tablets);

    {
        auto guard = WriterGuard(PendingActionsLock_);
        if (!PendingActionDescriptors_.contains(bundleName)) {
            BundlesWithPendingActions_.push_back(bundleName);
        }
        auto it = std::find(
            BundlesWithPendingActions_.begin(),
            BundlesWithPendingActions_.end(),
            bundleName);
        YT_VERIFY(it != BundlesWithPendingActions_.end());
    }

    ScheduleActionCreation(bundleName, *reshardDescriptor);
}

////////////////////////////////////////////////////////////////////////////////

IActionManagerPtr CreateActionManager(
    TActionManagerConfigPtr config,
    NApi::NNative::IClientPtr client,
    IBootstrap* bootstrap,
    IMulticellThrottlerPtr throttler)
{
    return New<TActionManager>(
        std::move(config),
        std::move(client),
        bootstrap,
        throttler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
