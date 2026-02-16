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

    void Start(TTransactionId prerequisiteTransactionId) override;
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

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, PendingActionsLock_);
    THashMap<std::string, std::deque<TActionDescriptor>> PendingActionDescriptors_;

    // Only to use from Invoker.
    THashMap<std::string, THashSet<TTabletActionPtr>> RunningActions_;
    THashMap<std::string, std::deque<TTabletActionPtr>> FinishedActions_;
    THashMap<std::string, TBundleProfilingCounters> ProfilingCounters_;

    // For bundles with confirmed pending actions we cannot add more pending actions.
    std::queue<std::pair<std::string, TInstant>> BundlesWithPendingActions_;

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
{ }

void TActionManager::ScheduleActionCreation(const std::string& bundleName, const TActionDescriptor& descriptor)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(PendingActionsLock_);

    PendingActionDescriptors_[bundleName].emplace_back(descriptor);
}

void TActionManager::CreateActions(const std::string& bundleName)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_VERIFY(Started_);

    if (RunningActions_.contains(bundleName)) {
        THROW_ERROR_EXCEPTION(
            "Cannot create new actions since bundle %v has unfinished actions",
            bundleName);
    }

    {
        auto guard = ReaderGuard(PendingActionsLock_);
        YT_VERIFY(PendingActionDescriptors_.contains(bundleName));
    }

    BundlesWithPendingActions_.emplace(bundleName, TInstant::Now() + Config_->TabletActionCreationTimeout);
}

void TActionManager::TryCreatePendingActions()
{
    TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("CreatePendingActions"));
    try {
        CreatePendingActions();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to create pending actions");
    }
}

void TActionManager::CreatePendingActions()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    auto runningActionCount = GetRunningActionCount();
    if (BundlesWithPendingActions_.empty()) {
        YT_LOG_DEBUG("No action to create in any bundle");
        return;
    }

    if (runningActionCount > Config_->CreateActionBatchSizeLimit / 2) {
        YT_LOG_DEBUG("Too many running actions, will not create more at the moment"
            " (ActionCount: %v, SoftLimit: %v, HardLimit: %v)",
            runningActionCount,
            Config_->CreateActionBatchSizeLimit / 2,
            Config_->CreateActionBatchSizeLimit);
        return;
    }

    auto iterationStartTime = TInstant::Now();

    YT_LOG_DEBUG("Started creating pending actions (IterationStartTime: %v,"
        " PendingBundleCount: %v, ActionCreationTimeout: %v)",
        iterationStartTime,
        std::ssize(BundlesWithPendingActions_),
        Config_->TabletActionCreationTimeout);

    int actionCount = 0;
    while (actionCount < Config_->CreateActionBatchSizeLimit && !BundlesWithPendingActions_.empty()) {
        auto [bundleName, timeout] = BundlesWithPendingActions_.front();
        if (timeout < iterationStartTime) {
            auto guard = WriterGuard(PendingActionsLock_);
            YT_LOG_WARNING(
                "Actions were dropped due to timeout (Bundle: %v, ActionCount: %v, Timeout: %v)",
                bundleName,
                std::ssize(PendingActionDescriptors_[bundleName]),
                Config_->TabletActionCreationTimeout);

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

    YT_LOG_DEBUG("Creating pending actions finished (ActionCount: %v, PendingBundleCount: %v)",
        actionCount,
        std::ssize(BundlesWithPendingActions_));
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

    YT_LOG_DEBUG("Creating pending actions (Bundle: %v, ActionCountLimit: %v)",
        bundleName,
        actionCountLimit);

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
        YT_LOG_DEBUG("Creating tablet action (Attributes: %v, BundleName: %v)",
            ConvertToYsonString(attributes, EYsonFormat::Text),
            bundleName);
        TCreateObjectOptions options;
        options.Attributes = std::move(attributes);
        options.PrerequisiteTransactionIds.push_back(PrerequisiteTransactionId_);
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
    for (int index = 0; index < actionCountLimit; ++index) {
        auto rspOrError = responses[index];
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(
                rspOrError,
                "Failed to create tablet action (BundleName: %v, ActionDescriptor: %v)",
                bundleName,
                descriptors[index]);

            // Retry smooth movement actions with regular move.
            if (IsSmoothMovementAction(descriptors[index])) {
                auto moveDescriptor = std::get<TMoveDescriptor>(descriptors[index]);

                YT_LOG_DEBUG("Smooth movement action failed, scheduling regular action creation instead "
                    "(BundleName: %v, TabletId: %v, CorrelationId: %v)",
                    bundleName,
                    moveDescriptor.TabletId,
                    moveDescriptor.CorrelationId);

                GetOrCreateProfilingCounters(bundleName).FailedAtRuntimeSmoothMovementActions.Increment();

                moveDescriptor.Smooth = false;
                descriptors.push_back(moveDescriptor);
            }

            continue;
        }

        auto actionId = ConvertTo<TTabletActionId>(rspOrError.Value());

        YT_LOG_DEBUG("Created tablet action (TabletActionId: %v, BundleName: %v, ActionDescriptor: %v)",
            actionId,
            bundleName,
            descriptors[index]);
        EmplaceOrCrash(runningActions, New<TTabletAction>(actionId, descriptors[index]));
    }

    for (int index = 0; index < actionCountLimit; ++index) {
        descriptors.pop_front();
    }

    {
        auto guard = WriterGuard(PendingActionsLock_);
        PendingActionDescriptors_[bundleName] = descriptors;
    }

    int createdActionCount = std::ssize(runningActions);
    if (!runningActions.empty()) {
        auto it = RunningActions_.emplace(bundleName, THashSet<TTabletActionPtr>{}).first;
        it->second.insert(runningActions.begin(), runningActions.end());
        GetOrCreateProfilingCounters(bundleName).RunningActions.Update(std::ssize(it->second));
    }

    YT_LOG_INFO("Created tablet actions for bundle (ActionCount: %v, BundleName: %v)",
        createdActionCount,
        bundleName);

    return createdActionCount;
}

bool TActionManager::HasUnfinishedActions(
    const std::string& bundleName,
    const std::vector<TTabletActionId>& knownBundleActionIds) const
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    auto guard = ReaderGuard(PendingActionsLock_);

    return PendingActionDescriptors_.contains(bundleName) ||
        RunningActions_.contains(bundleName) ||
        !AreAllActionsKnown(bundleName, knownBundleActionIds);
}

bool TActionManager::AreAllActionsKnown(
    const std::string& bundleName,
    std::vector<TTabletActionId> actionIds) const
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

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
}

void TActionManager::Start(TTransactionId prerequisiteTransactionId)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_LOG_INFO("Starting tablet action manager (PrerequisiteTransactionId: %v)", prerequisiteTransactionId);

    Started_ = true;

    YT_VERIFY(prerequisiteTransactionId);
    PrerequisiteTransactionId_ = prerequisiteTransactionId;

    auto guard = WriterGuard(PendingActionsLock_);

    RunningActions_.clear();
    PendingActionDescriptors_.clear();

    PollExecutor_->Start();
    CreateActionExecutor_->Start();
}

void TActionManager::Stop()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_LOG_INFO("Stopping tablet action manager");

    Started_ = false;
    PrerequisiteTransactionId_ = NullTransactionId;

    YT_UNUSED_FUTURE(PollExecutor_->Stop());
    YT_UNUSED_FUTURE(CreateActionExecutor_->Stop());

    YT_LOG_INFO("Tablet action manager stopped");
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
        YT_LOG_ERROR(ex, "Failed to poll actions");
    }
}

void TActionManager::Poll()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    YT_LOG_INFO("Start checking tablet action states");

    THashSet<TTabletActionId> actionIds;
    for (const auto& [bundleName, actions] : RunningActions_) {
        for (const auto& action : actions) {
            actionIds.insert(action->GetId());
        }
    }

    YT_LOG_DEBUG("Started fetching tablet action states (ActionCount: %v)", actionIds.size());

    static const std::vector<std::string> attributeKeys{"state", "error"};
    auto actionToAttributes = FetchAttributes(Client_, actionIds, attributeKeys, MasterRequestThrottler_);

    YT_LOG_DEBUG("Finished fetching tablet action states (ActionCount: %v)", actionToAttributes.size());

    for (const auto& [bundle, actions] : RunningActions_) {
        for (const auto& action : actions) {
            if (auto it = actionToAttributes.find(action->GetId()); it != actionToAttributes.end()) {
                const auto& attributes = it->second;
                auto state = attributes->Get<ETabletActionState>("state");
                action->SetState(state);

                YT_LOG_DEBUG("Tablet action state fetched (TabletActionId: %v, State: %v, CorrelationId: %v)",
                    action->GetId(),
                    state,
                    action->GetCorrelationId());
                if (attributes->Contains("error")) {
                    auto error = attributes->Get<TError>("error");
                    action->Error() = error;
                    YT_LOG_WARNING(error,
                        "Tablet action failed (TabletActionId: %v, CorrelationId: %v, Kind: %v)",
                        action->GetId(),
                        action->GetCorrelationId(),
                        action->GetKind());
                }
            } else if (!actionIds.contains(action->GetId())) {
                YT_LOG_DEBUG("Tablet action status is unknown "
                    "(TabletActionId: %v, Kind: %v, State: %v, CorrelationId: %v)",
                    action->GetId(),
                    action->GetKind(),
                    action->GetState(),
                    action->GetCorrelationId());
            } else {
                action->SetLost(true);
                YT_LOG_DEBUG("Tablet action is lost (TabletActionId: %v, Kind: %v, CorrelationId: %v)",
                    action->GetId(),
                    action->GetKind(),
                    action->GetCorrelationId());
            }
        }
    }

    MoveFinishedActionsFromRunningToFinished();
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

void TActionManager::DropFrontBundleWithPendingActions(const std::string& bundleName)
{
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(PendingActionsLock_);

    YT_VERIFY(BundlesWithPendingActions_.front().first == bundleName);
    BundlesWithPendingActions_.pop();
    EraseOrCrash(PendingActionDescriptors_, bundleName);
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
