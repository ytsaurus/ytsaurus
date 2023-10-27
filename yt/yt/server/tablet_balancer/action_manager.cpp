#include "action_manager.h"
#include "bootstrap.h"
#include "config.h"
#include "helpers.h"
#include "private.h"
#include "tablet_action.h"

#include <yt/yt/server/lib/tablet_balancer/balancing_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

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

static const auto& Logger = TabletBalancerLogger;

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
        IBootstrap* bootstrap);

    void ScheduleActionCreation(const TString& bundleName, const TActionDescriptor& descriptor) override;
    void CreateActions(const TString& bundleName) override;

    bool HasUnfinishedActions(const TString& bundleName) const override;
    bool IsKnownAction(const TString& bundleName, TTabletActionId actionId) const override;

    void Start(TTransactionId prerequisiteTransactionId) override;
    void Stop() override;

    void Reconfigure(const TActionManagerConfigPtr& config) override;

private:
    struct TBundleProfilingCounters
    {
        NProfiling::TGauge RunningActions;
        NProfiling::TCounter FailedActions;
    };

    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;

    TActionManagerConfigPtr Config_;
    NConcurrency::TPeriodicExecutorPtr PollExecutor_;
    NConcurrency::TPeriodicExecutorPtr CreateActionExecutor_;

    THashMap<TString, std::deque<TActionDescriptor>> PendingActionDescriptors_;
    THashMap<TString, THashSet<TTabletActionPtr>> RunningActions_;
    THashMap<TString, std::deque<TTabletActionPtr>> FinishedActions_;
    THashMap<TString, TBundleProfilingCounters> ProfilingCounters_;

    std::queue<std::pair<TString, TInstant>> BundlesWithPendingActions_;

    bool Started_ = false;
    TTransactionId PrerequisiteTransactionId_ = NullTransactionId;

    void Poll();
    void TryPoll();

    int CreatePendingBundleActions(const TString& bundleName, int actionCountLimit);
    void CreatePendingActions();
    void TryCreatePendingActions();

    int GetRunningActionCount() const;

    IAttributeDictionaryPtr MakeActionAttributes(const TActionDescriptor& descriptor);
    void MoveFinishedActionsFromRunningToFinished();
    const TBundleProfilingCounters& GetOrCreateProfilingCounters(const TString& bundleName);
};

////////////////////////////////////////////////////////////////////////////////

TActionManager::TActionManager(
    TActionManagerConfigPtr config,
    NApi::NNative::IClientPtr client,
    IBootstrap* bootstrap)
    : Client_(std::move(client))
    , Invoker_(bootstrap->GetControlInvoker())
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

void TActionManager::ScheduleActionCreation(const TString& bundleName, const TActionDescriptor& descriptor)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    PendingActionDescriptors_[bundleName].emplace_back(descriptor);
}

void TActionManager::CreateActions(const TString& bundleName)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_VERIFY(Started_);

    if (RunningActions_.contains(bundleName)) {
        THROW_ERROR_EXCEPTION(
            "Cannot create new actions since bundle %v has unfinished actions",
            bundleName);
    }

    if (!PendingActionDescriptors_.contains(bundleName)) {
        YT_LOG_INFO("Action manager has no actions to create (BundleName: %v)", bundleName);
        return;
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
            YT_LOG_WARNING(
                "Actions were dropped due to timeout (Bundle: %v, ActionCount: %v, Timeout: %v)",
                bundleName,
                std::ssize(PendingActionDescriptors_[bundleName]),
                Config_->TabletActionCreationTimeout);

            BundlesWithPendingActions_.pop();
            EraseOrCrash(PendingActionDescriptors_, bundleName);
            continue;
        }

        actionCount += CreatePendingBundleActions(bundleName, Config_->CreateActionBatchSizeLimit - actionCount);

        if (PendingActionDescriptors_[bundleName].empty()) {
            BundlesWithPendingActions_.pop();
            EraseOrCrash(PendingActionDescriptors_, bundleName);
        }
    }

    YT_LOG_DEBUG("Creating pending actions finished (ActionCount: %v, PendingBundleCount: %v)",
        actionCount,
        std::ssize(BundlesWithPendingActions_));
}

int TActionManager::GetRunningActionCount() const
{
    return std::accumulate(
        RunningActions_.begin(),
        RunningActions_.end(),
        0,
        [] (int x, const auto& pair) {
            return x + std::ssize(pair.second);
        });
}

int TActionManager::CreatePendingBundleActions(const TString& bundleName, int actionCountLimit)
{
    YT_LOG_DEBUG("Creating pending actions (Bundle: %v, ActionCountLimit: %v)",
        bundleName,
        actionCountLimit);

    auto proxy = CreateObjectServiceWriteProxy(Client_);
    auto batchReq = proxy.ExecuteBatch();

    auto& descriptors = PendingActionDescriptors_[bundleName];
    actionCountLimit = std::min<int>(actionCountLimit, std::ssize(descriptors));

    std::vector<TFuture<NObjectClient::TObjectId>> futures;

    for (int index = 0; index < actionCountLimit; ++index) {
        auto attributes = MakeActionAttributes(descriptors[index]);
        YT_LOG_DEBUG("Creating tablet action (Attributes: %v, BundleName: %v)",
            ConvertToYsonString(attributes, EYsonFormat::Text),
            bundleName);
        TCreateObjectOptions options;
        options.Attributes = std::move(attributes);
        options.PrerequisiteTransactionIds.push_back(PrerequisiteTransactionId_);
        futures.emplace_back(Client_->CreateObject(EObjectType::TabletAction, std::move(options)));
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

    int createdActionCount = std::ssize(runningActions);
    if (!runningActions.empty()) {
        GetOrCreateProfilingCounters(bundleName).RunningActions.Update(runningActions.size());
        EmplaceOrCrash(RunningActions_, bundleName, std::move(runningActions));
    }

    YT_LOG_INFO("Created tablet actions for bundle (ActionCount: %v, BundleName: %v)",
        createdActionCount,
        bundleName);
    return createdActionCount;
}

bool TActionManager::HasUnfinishedActions(const TString& bundleName) const
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    return PendingActionDescriptors_.contains(bundleName) || RunningActions_.contains(bundleName);
}

bool TActionManager::IsKnownAction(const TString& bundleName, TTabletActionId actionId) const
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    if (auto it = RunningActions_.find(bundleName); it != RunningActions_.end()) {
        auto action = std::find_if(
            it->second.begin(),
            it->second.end(),
            [actionId = actionId] (const TTabletActionPtr& action) {
                return action->GetId() == actionId;
            });

        if (action != it->second.end()) {
            return true;
        }
    }

    if (auto it = FinishedActions_.find(bundleName); it != FinishedActions_.end()) {
        auto action = std::find_if(
            it->second.begin(),
            it->second.end(),
            [actionId = actionId] (const TTabletActionPtr& action) {
                return action->GetId() == actionId;
            });

        return action != it->second.end();
    }

    return false;
}

void TActionManager::Start(TTransactionId prerequisiteTransactionId)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_INFO("Starting tablet action manager (PrerequisiteTransactionId: %v)", prerequisiteTransactionId);

    Started_ = true;

    YT_VERIFY(prerequisiteTransactionId);
    PrerequisiteTransactionId_ = prerequisiteTransactionId;

    RunningActions_.clear();
    PendingActionDescriptors_.clear();

    PollExecutor_->Start();
    CreateActionExecutor_->Start();
}

void TActionManager::Stop()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_INFO("Stopping tablet action manager");

    Started_ = false;
    PrerequisiteTransactionId_ = NullTransactionId;

    YT_UNUSED_FUTURE(PollExecutor_->Stop());
    YT_UNUSED_FUTURE(CreateActionExecutor_->Stop());

    YT_LOG_INFO("Tablet action manager stopped");
}

void TActionManager::Reconfigure(const TActionManagerConfigPtr& config)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

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
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_INFO("Start checking tablet action states");

    THashSet<TTabletActionId> actionIds;
    for (const auto& [bundleName, actions] : RunningActions_) {
        for (const auto& action : actions) {
            actionIds.insert(action->GetId());
        }
    }

    YT_LOG_DEBUG("Started fetching tablet action states (ActionCount: %v)", actionIds.size());

    static const std::vector<TString> attributeKeys{"state", "error"};
    auto actionToAttributes = FetchAttributes(Client_, actionIds, attributeKeys);

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
                        "Tablet action failed (TabletActionId: %v, CorrelationId: %v)",
                        action->GetId(),
                        action->GetCorrelationId());
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
    THashSet<TString> relevantBundles;

    for (auto& [bundleName, runningActions] : RunningActions_) {
        auto& finishedActions = FinishedActions_[bundleName];
        int failedActionCount = 0;

        for (auto it = runningActions.begin(); it != runningActions.end(); ) {
            const auto& action = *it;
            if (action->IsFinished()) {
                if (action->GetState() == ETabletActionState::Failed) {
                    ++failedActionCount;
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
    }

    DropMissingKeys(RunningActions_, relevantBundles);
}

const TActionManager::TBundleProfilingCounters& TActionManager::GetOrCreateProfilingCounters(const TString& bundleName)
{
    if (auto it = ProfilingCounters_.find(bundleName); it != ProfilingCounters_.end()) {
        return it->second;
    }
    return EmplaceOrCrash(ProfilingCounters_, bundleName, TBundleProfilingCounters{
        .RunningActions = TabletBalancerProfiler
            .WithTag("tablet_cell_bundle", bundleName)
            .Gauge("/action_manager/running_actions"),
        .FailedActions = TabletBalancerProfiler
            .WithTag("tablet_cell_bundle", bundleName)
            .Counter("/action_manager/failed_actions")
    })->second;
}

IAttributeDictionaryPtr TActionManager::MakeActionAttributes(const TActionDescriptor& descriptor)
{
    auto attributes = CreateEphemeralAttributes();
    Visit(descriptor,
        [&] (const TMoveDescriptor& descriptor) {
            attributes->Set("kind", "move");
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

////////////////////////////////////////////////////////////////////////////////

IActionManagerPtr CreateActionManager(
    TActionManagerConfigPtr config,
    NApi::NNative::IClientPtr client,
    IBootstrap* bootstrap)
{
    return New<TActionManager>(
        std::move(config),
        std::move(client),
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
