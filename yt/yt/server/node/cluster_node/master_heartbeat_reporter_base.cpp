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
        "Starting node heartbeats (NodeId: %v)",
        Bootstrap_->GetNodeId());

    for (auto cellTag : MasterCellTags_) {
        StartNodeHeartbeatsToCell(cellTag);
    }
}

void TMasterHeartbeatReporterBase::StartNodeHeartbeatsToCell(TCellTag cellTag)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    if (!MasterCellTags_.contains(cellTag)) {
        YT_LOG_ALERT(
            "Attempted to initiate heartbeat report to unknown master (CellTag: %v)",
            cellTag);
        return;
    }

    // This lock ensures that only one process is in the section below,
    // it is important to to exclude intersections between heartbeat flows to master.
    auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&GetOrCrash(ExecutorLockPerMaster_, cellTag)))
        .ValueOrThrow();
    DoStartNodeHeartbeatsToCell(cellTag);
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

void TMasterHeartbeatReporterBase::DoStartNodeHeartbeatsToCell(TCellTag cellTag)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    if (auto executor = FindExecutor(cellTag); executor && executor->IsStarted()) {
        YT_LOG_DEBUG(
            "Waiting for the previous heartbeat executor to stop (CellTag: %v)",
            cellTag);
        auto error = WaitForFast(executor->Stop());

        YT_LOG_ALERT_UNLESS(
            error.IsOK(),
            error,
            "Unexpected failure while waiting for previous heartbeat executor to shut down");
    }

    // Reset reporter's state only after stopped heartbeats events were reported.
    ResetState(cellTag);

    // MasterConnectionInvoker changes after every registration
    // and so we have to make a new HeartbeatExecutor.
    // Technically, we could support "UpdateInvoker" method,
    // but there is no reason to preserve HeartbeatExecutor's state.
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
            clusterNodeMasterConnector->ResetAndRegisterAtMaster(/*firstTime*/ false);
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
    std::vector<TCellTag> newSecondaryMasterCellTags;
    newSecondaryMasterCellTags.reserve(newSecondaryMasterConfigs.size());
    for (const auto& [cellTag, _] : newSecondaryMasterConfigs) {
        newSecondaryMasterCellTags.emplace_back(cellTag);
        if (clusterNodeMasterConnector->IsRegisteredAtPrimaryMaster()) {
            StartNodeHeartbeatsToCell(cellTag);
        }
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
