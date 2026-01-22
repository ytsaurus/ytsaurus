#include "master_heartbeat_reporter_base.h"

#include "bootstrap.h"
#include "master_connector.h"

#include <yt/yt/core/concurrency/retrying_periodic_executor.h>

namespace NYT::NClusterNode {

using namespace NCellMasterClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NLogging;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TMasterHeartbeatReporterBase::TMasterHeartbeatReporterBase(
    IBootstrapBase* bootstrap,
    bool reportHeartbeatsToAllSecondaryMasters,
    TLogger logger)
    : Bootstrap_(bootstrap)
    , ReportHeartbeatsToAllSecondaryMasters_(reportHeartbeatsToAllSecondaryMasters)
    , Logger(std::move(logger))
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);
}

void TMasterHeartbeatReporterBase::Initialize()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    if (ReportHeartbeatsToAllSecondaryMasters_) {
        MasterCellTags_ = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector()->GetMasterCellTags();
    } else {
        MasterCellTags_ = {CellTagFromId(Bootstrap_->GetClusterNodeBootstrap()->GetCellId())};
    }

    for (auto cellTag : MasterCellTags_) {
        ExecutorLockPerMaster_[cellTag];
    }

    const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
    clusterNodeMasterConnector->SubscribeStartHeartbeats(
        BIND_NO_PROPAGATE(&TMasterHeartbeatReporterBase::OnStartHeartbeats, MakeWeak(this))
            .Via(Bootstrap_->GetControlInvoker()));
    Bootstrap_->SubscribeReadyToUpdateHeartbeatStream(
        BIND_NO_PROPAGATE(&TMasterHeartbeatReporterBase::OnReadyToUpdateHeartbeatStream, MakeWeak(this))
            .Via(Bootstrap_->GetControlInvoker()));
}

void TMasterHeartbeatReporterBase::StartNodeHeartbeats()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    YT_LOG_INFO(
        "Starting node heartbeats (NodeId: %v, MasterCellTags: %v)",
        Bootstrap_->GetNodeId(),
        MasterCellTags_);

    StartNodeHeartbeatsToCells(MasterCellTags_);
}

void TMasterHeartbeatReporterBase::ScheduleOutOfBandMasterHeartbeats(const THashSet<NObjectClient::TCellTag>& masterCellTags)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    for (auto cellTag : masterCellTags) {
        if (!MasterCellTags_.contains(cellTag)) {
            YT_LOG_ALERT(
                "Attempted to initiate heartbeat report to unknown master (CellTag: %v)",
                cellTag);
            continue;
        }

        if (auto executor = FindExecutor(cellTag)) {
            executor->ScheduleOutOfBand();
        } else {
            executor = New<TRetryingPeriodicExecutor>(
                Bootstrap_->GetMasterConnectionInvoker(),
                BIND_NO_PROPAGATE([this, weakThis = MakeWeak(this), cellTag] {
                    auto this_ = weakThis.Lock();
                    return this_ ? ReportHeartbeat(cellTag) : TError("Master heartbeat reporter is destroyed");
                }),
                Options_);

            executor->Start();
            Executors_[cellTag] = std::move(executor);
        }
    }
}

void TMasterHeartbeatReporterBase::StartNodeHeartbeatsToCells(const THashSet<TCellTag>& masterCellTags)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    THashSet<TCellTag> allowedMasterCellTags;
    allowedMasterCellTags.reserve(masterCellTags.size());
    for (auto cellTag : masterCellTags) {
        if (!MasterCellTags_.contains(cellTag)) {
            YT_LOG_ALERT(
                "Attempted to initiate heartbeat report to unknown master (CellTag: %v)",
                cellTag);
            continue;
        }
        InsertOrCrash(allowedMasterCellTags, cellTag);
    }

    // These locks ensure that only one process per master heartbeat is in the section below,
    // it is important to exclude intersections between heartbeat flows to master because of reporter's state.
    std::vector<TFuture<TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>>> futureGuards;
    for (auto cellTag : allowedMasterCellTags) {
        futureGuards.push_back(TAsyncLockWriterGuard::Acquire(&GetOrCrash(ExecutorLockPerMaster_, cellTag)));
    }

    AllSet(std::move(futureGuards))
        .Subscribe(BIND([allowedMasterCellTags = std::move(allowedMasterCellTags), this, weakThis = MakeWeak(this)] (const NYT::TErrorOr<std::vector<TErrorOr<TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>>>>& guardsOrError)  {
            auto this_ = weakThis.Lock();
            if (!this_) {
                YT_LOG_INFO("Master heartbeat reporter is destroyed");
                return;
            }
            if (!guardsOrError.IsOK()) {
                YT_LOG_ALERT(
                    guardsOrError,
                    "Failed to acquire lock writer guards to start heartbeat reports to masters (MasterCellTags: %v)",
                    allowedMasterCellTags);
                const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
                clusterNodeMasterConnector->ResetAndRegisterAtMaster(ERegistrationReason::HeartbeatFailure);
                return;
            }
            std::vector<TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>> guards;
            for (auto guardOrError: guardsOrError.Value()) {
                if (!guardOrError.IsOK()) {
                    YT_LOG_ALERT(
                        guardOrError,
                        "Failed to acquire lock writer guards to start heartbeat reports to masters (MasterCellTags: %v)",
                        allowedMasterCellTags);
                    const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
                    clusterNodeMasterConnector->ResetAndRegisterAtMaster(ERegistrationReason::HeartbeatFailure);
                    return;
                }
                guards.push_back(std::move(guardOrError.Value()));
            }
            DoStartNodeHeartbeatsToCells(allowedMasterCellTags);

            // These guards are not used directly, but they protect the whole process of starting node heartbeat reports to master
            // from intersection with another same process.
            Y_UNUSED(guards);
        }).Via(Bootstrap_->GetControlInvoker()));
}

void TMasterHeartbeatReporterBase::Reconfigure(const TRetryingPeriodicExecutorOptions& options)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    Options_ = options;
    for (auto cellTag : MasterCellTags_) {
        // NB(arkady-e1ppa): HeartbeatExecutor is created once OnMasterRegistrationSignal is fired.
        // This happens after the first time DynamicConfigManager applies dynamic config.
        // Therefore we must be ready to encounter null executor.
        if (auto executor = FindExecutor(cellTag)) {
            executor->SetOptions(options);
        }
    }
}

void TMasterHeartbeatReporterBase::DoStopNodeHeartbeatsToCells(
    const THashSet<TCellTag>& masterCellTags)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    THashSet<TCellTag> masterCellTagsToStop;
    std::vector<TFuture<void>> executorsStopFutures;
    masterCellTagsToStop.reserve(masterCellTags.size());
    executorsStopFutures.reserve(masterCellTags.size());

    for (auto cellTag : masterCellTags) {
        if (auto executor = FindExecutor(cellTag); executor && executor->IsStarted()) {
            executorsStopFutures.push_back(executor->Stop());
            masterCellTagsToStop.insert(cellTag);
        }
    }

    YT_LOG_INFO(
        "Waiting for the previous heartbeat executors to stop (CellTags: %v)",
        masterCellTagsToStop);

    auto error = WaitFor(AllSucceeded(std::move(executorsStopFutures)));

    if (!error.IsOK()) {
        YT_LOG_ALERT(
            error,
            "Unexpected failure while waiting for previous heartbeat executors to shut down");

        const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
        clusterNodeMasterConnector->ResetAndRegisterAtMaster(ERegistrationReason::HeartbeatFailure);
        return;
    }

    // Reset reporters' states only after stopped heartbeats events were reported.
    ResetStates(masterCellTags);

    YT_LOG_INFO("Stopped node heartbeats to cells (CellTags: %v)", masterCellTags);
}

void TMasterHeartbeatReporterBase::DoStartNodeHeartbeatsToCells(
    const THashSet<TCellTag>& masterCellTags)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    DoStopNodeHeartbeatsToCells(masterCellTags);

    // MasterConnectionInvoker changes after every registration
    // and so we have to make a new HeartbeatExecutor.
    // Technically, we could support "UpdateInvoker" method,
    // but there is no reason to preserve HeartbeatExecutor's state.
    for (auto cellTag : masterCellTags) {
        auto executor = New<TRetryingPeriodicExecutor>(
            Bootstrap_->GetMasterConnectionInvoker(),
            BIND_NO_PROPAGATE([this, weakThis = MakeWeak(this), cellTag] {
                auto this_ = weakThis.Lock();
                return this_ ? ReportHeartbeat(cellTag) : TError("Master heartbeat reporter is destroyed");
            }),
            Options_);

        YT_LOG_INFO(
            "Starting node heartbeat reports to master (CellTag: %v)",
            cellTag);
        executor->Start();
        Executors_[cellTag] = std::move(executor);
    }
}

TError TMasterHeartbeatReporterBase::ReportHeartbeat(TCellTag cellTag)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
    if (!clusterNodeMasterConnector->IsConnected()) {
        return TError("Node disconnected");
    }

    YT_LOG_INFO("Sending node heartbeat to master (CellTag: %v)",
        cellTag);

    auto error = WaitFor(DoReportHeartbeat(cellTag));
    if (error.IsOK()) {
        YT_LOG_INFO("Successfully reported node heartbeat to master (CellTag: %v)",
            cellTag);
        OnHeartbeatSucceeded(cellTag);
        return TError();
    } else {
        YT_LOG_DEBUG(
            error,
            "Failed to report node heartbeat to master (CellTag: %v)",
            cellTag);
        OnHeartbeatFailed(cellTag);
        if (IsRetriableError(error) || error.FindMatching(HeartbeatRetriableErrors)) {
            // TODO(arkady-e1ppa): Maybe backoff in this case?
            return TError();
        } else {
            YT_LOG_WARNING(
                error,
                "Received non-retriable error during heartbeat report to master, node will reconnect to primary master (CellTag: %v)",
                cellTag);
            clusterNodeMasterConnector->ResetAndRegisterAtMaster(ERegistrationReason::HeartbeatFailure);
            return TError("Received non-retriable error while reporting node heartbeat") << error;
        }
    }
}

void TMasterHeartbeatReporterBase::OnStartHeartbeats()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    StartNodeHeartbeats();
}

void TMasterHeartbeatReporterBase::OnReadyToUpdateHeartbeatStream(
    const TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    if (!ReportHeartbeatsToAllSecondaryMasters_) {
        return;
    }

    for (const auto& [cellTag, _] : newSecondaryMasterConfigs) {
        EmplaceOrCrash(MasterCellTags_, cellTag);
        ExecutorLockPerMaster_[cellTag];
    }

    const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
    THashSet<TCellTag> newSecondaryMasterCellTags;
    newSecondaryMasterCellTags.reserve(newSecondaryMasterConfigs.size());
    for (const auto& [cellTag, _] : newSecondaryMasterConfigs) {
        newSecondaryMasterCellTags.insert(cellTag);
    }
    if (clusterNodeMasterConnector->IsRegisteredAtPrimaryMaster()) {
        StartNodeHeartbeatsToCells(newSecondaryMasterCellTags);
    }

    YT_LOG_INFO(
        "Received master cell directory change, initiated heartbeat reports to new masters "
        "(NewCellTags: %v)",
        newSecondaryMasterCellTags);
}

TRetryingPeriodicExecutorPtr TMasterHeartbeatReporterBase::FindExecutor(TCellTag cellTag) const
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto executorIt = Executors_.find(cellTag);
    return executorIt == Executors_.end() ? nullptr : executorIt->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
