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
    // it is important to exclude intersections between heartbeat flows to master.
    std::vector<TFuture<TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>>> futureGuards;
    for (auto cellTag : allowedMasterCellTags) {
        futureGuards.push_back(TAsyncLockWriterGuard::Acquire(&GetOrCrash(ExecutorLockPerMaster_, cellTag)));
    }
    auto guards = WaitFor(AllSucceeded(std::move(futureGuards)))
        .ValueOrThrow();
    DoStartNodeHeartbeatsToCells(allowedMasterCellTags);
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
void TMasterHeartbeatReporterBase::DoStopNodeHeartbeatsToCells(const THashSet<TCellTag>& masterCellTags)
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

    YT_LOG_DEBUG(
        "Waiting for the previous heartbeat executors to stop (CellTags: %v)",
        masterCellTagsToStop);

    auto error = WaitFor(AllSucceeded(std::move(executorsStopFutures)));

    YT_LOG_ALERT_UNLESS(
        error.IsOK(),
        error,
        "Unexpected failure while waiting for previous heartbeat executors to shut down");

    // Reset reporters' states only after stopped heartbeats events were reported.
    ResetStates(masterCellTags);

    YT_LOG_DEBUG("Stopped node heartbeats to cells (CellTags: %v)", masterCellTagsToStop);
}

void TMasterHeartbeatReporterBase::DoStartNodeHeartbeatsToCells(const THashSet<TCellTag>& masterCellTags)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    DoStopNodeHeartbeatsToCells(masterCellTags);

    // MasterConnectionInvoker changes after every registration
    // and so we have to make a new HeartbeatExecutor.
    // Technically, we could support "UpdateInvoker" method,
    // but there is no reason to preserve HeartbeatExecutor's state.
    for (auto cellTag : masterCellTags) {
        Executors_[cellTag] = New<TRetryingPeriodicExecutor>(
            Bootstrap_->GetMasterConnectionInvoker(),
            BIND([this, weakThis = MakeWeak(this), cellTag] {
                auto this_ = weakThis.Lock();
                return this_ ? ReportHeartbeat(cellTag) : TError("Master heartbeat reporter is destroyed");
            }),
            Options_);

        YT_LOG_INFO(
            "Starting node heartbeat reports to master (CellTag: %v)",
            cellTag);
        Executors_[cellTag]->Start();
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
