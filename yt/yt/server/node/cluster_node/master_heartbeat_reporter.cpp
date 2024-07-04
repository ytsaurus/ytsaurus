#include "master_heartbeat_reporter.h"
#include "bootstrap.h"
#include "master_connector.h"
#include "master_heartbeat_reporter_callbacks.h"

#include <yt/yt/core/concurrency/retrying_periodic_executor.h>

namespace NYT::NClusterNode {

using namespace NCellMasterClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NLogging;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TMasterHeartbeatReporter
    : public IMasterHeartbeatReporter
{
public:
    TMasterHeartbeatReporter(
        IBootstrapBase* bootstrap,
        bool reportHeartbeatsToAllSecondaryMasters,
        IMasterHeartbeatReporterCallbacksPtr callbacks,
        TRetryingPeriodicExecutorOptions options,
        const TLogger& logger)
        : Bootstrap_(bootstrap)
        , ReportHeartbeatsToAllSecondaryMasters_(reportHeartbeatsToAllSecondaryMasters)
        , Callbacks_(std::move(callbacks))
        , Options_(std::move(options))
        , Logger(logger)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
    }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (ReportHeartbeatsToAllSecondaryMasters_) {
            MasterCellTags_ = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector()->GetMasterCellTags();
        } else {
            MasterCellTags_ = {CellTagFromId(Bootstrap_->GetClusterNodeBootstrap()->GetCellId())};
        }

        Bootstrap_->SubscribeSecondaryMasterCellListChanged(
            BIND_NO_PROPAGATE(&TMasterHeartbeatReporter::OnSecondaryMasterCellListChanged, MakeWeak(this))
                .Via(Bootstrap_->GetControlInvoker()));
    }

    void StartHeartbeats() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Starting node heartbeats");

        for (auto cellTag : MasterCellTags_) {
            StartHeartbeatsToCell(cellTag);
        }
    }

    void StartHeartbeatsToCell(TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!MasterCellTags_.contains(cellTag)) {
            YT_LOG_ALERT(
                "Attempted to initiate heartbeat report to unknown master (CellTag: %v)",
                cellTag);
            return;
        }

        if (auto executor = FindExecutor(cellTag); executor && executor->IsStarted()) {
            PreviousExecutorStoppedEvents_[cellTag] = executor->Stop();
        }

        auto stoppedEventIt = PreviousExecutorStoppedEvents_.find(cellTag);
        if (stoppedEventIt != PreviousExecutorStoppedEvents_.end()) {
            auto stoppedEvent = stoppedEventIt->second;
            YT_LOG_DEBUG(
                "Waiting for the previous heartbeat executor to stop (CellTag: %v)",
                cellTag);
            auto error = WaitForFast(std::move(stoppedEvent));

            YT_LOG_ALERT_UNLESS(
                error.IsOK(),
                error,
                "Unexpected failure while waiting for previous heartbeat executor to shut down");
        }

        // Reset callbacks' state only after stopped heartbeats events were reported.
        Callbacks_->Reset(cellTag);

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

    void Reconfigure(const TRetryingPeriodicExecutorOptions& options) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

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

private:
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    IBootstrapBase* const Bootstrap_;

    const bool ReportHeartbeatsToAllSecondaryMasters_;

    THashSet<TCellTag> MasterCellTags_;

    IMasterHeartbeatReporterCallbacksPtr Callbacks_;

    TRetryingPeriodicExecutorOptions Options_;
    THashMap<TCellTag, TRetryingPeriodicExecutorPtr> Executors_;
    THashMap<TCellTag, TFuture<void>> PreviousExecutorStoppedEvents_;

    TLogger Logger;

    void OnSecondaryMasterCellListChanged(
        const TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!ReportHeartbeatsToAllSecondaryMasters_) {
            return;
        }
        for (const auto& [cellTag, _] : newSecondaryMasterConfigs) {
            EmplaceOrCrash(MasterCellTags_, cellTag);
        }
        const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
        std::vector<TCellTag> newSecondaryMasterCellTags;
        newSecondaryMasterCellTags.reserve(newSecondaryMasterConfigs.size());
        for (const auto& [cellTag, _] : newSecondaryMasterConfigs) {
            newSecondaryMasterCellTags.emplace_back(cellTag);
            if (clusterNodeMasterConnector->IsRegisteredAtPrimaryMaster()) {
                StartHeartbeatsToCell(cellTag);
            }
        }

        YT_LOG_INFO(
            "Received master cell directory change, initiated heartbeat reports to new masters "
            "(NewCellTags: %v)",
            newSecondaryMasterCellTags);
    }

    TError ReportHeartbeat(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
        YT_VERIFY(clusterNodeMasterConnector->IsConnected());

        auto rspOrError = WaitFor(Callbacks_->ReportHeartbeat(cellTag));
        if (rspOrError.IsOK()) {
            Callbacks_->OnHeartbeatSucceeded(cellTag);

            YT_LOG_INFO("Successfully reported node heartbeat to master (CellTag: %v)",
                cellTag);
            return TError();
        } else {
            Callbacks_->OnHeartbeatFailed(cellTag);

            if (IsRetriableError(rspOrError) || rspOrError.FindMatching(HeartbeatRetriableErrors)) {
                // TODO(arkady-e1ppa): Maybe backoff in this case?
                return TError();
            } else {
                PreviousExecutorStoppedEvents_[cellTag] = Executors_[cellTag]->Stop();
                clusterNodeMasterConnector->ResetAndRegisterAtMaster(/*firstTime*/ false);
                return TError("Received non-retriable error while reporting heartbeat") << rspOrError;
            }
        }
    }

    TRetryingPeriodicExecutorPtr FindExecutor(TCellTag cellTag) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto executorIt = Executors_.find(cellTag);
        return executorIt == Executors_.end() ? nullptr : executorIt->second;
    }
};

////////////////////////////////////////////////////////////////////////////////

IMasterHeartbeatReporterPtr CreateMasterHeartbeatReporter(
    IBootstrapBase* bootstrap,
    bool reportHeartbeatsToAllSecondaryMasters,
    IMasterHeartbeatReporterCallbacksPtr callbacks,
    TRetryingPeriodicExecutorOptions options,
    const TLogger& logger)
{
    return New<TMasterHeartbeatReporter>(
        bootstrap,
        reportHeartbeatsToAllSecondaryMasters,
        std::move(callbacks),
        std::move(options),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
