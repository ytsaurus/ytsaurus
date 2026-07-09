#include "worker_tracker.h"

#include "config.h"
#include "job_manager.h"
#include "private.h"
#include "worker.h"
#include "yt_connector.h"

#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/lease_manager.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow::NController {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = WorkerTrackerLogger;

////////////////////////////////////////////////////////////////////////////////

class TWorkerTracker
    : public IWorkerTracker
{
public:
    struct TFaultyWorker
    {
        double Failures = 0;
        TInstant FailedAt = TInstant::Now();
    };

    TWorkerTracker(
        TControlActionQueuePtr controlQueue,
        IYTConnectorPtr connector,
        TNodeInfoPtr nodeInfo)
        : ControlQueue_(std::move(controlQueue))
        , Connector_(std::move(connector))
        , NodeInfo_(std::move(nodeInfo))
        , DynamicPipelineSpec_(New<TDynamicPipelineSpec>())
        , Executor_(New<NConcurrency::TPeriodicExecutor>(
            GetControlInvoker(),
            BIND(&TWorkerTracker::CleanUpFaultyAddresses, MakeWeak(this)),
            NConcurrency::TPeriodicExecutorOptions::WithJitter(DynamicPipelineSpec_.Acquire()->JobManager->FaultyAddressWindow)))
    { }

    void Initialize() override
    {
        Executor_->Start();
        Connector_->SubscribeLeadingStarted(BIND_NO_PROPAGATE(
            &TWorkerTracker::OnLeadingStarted,
            Unretained(this)));
        Connector_->SubscribeLeadingEnded(BIND_NO_PROPAGATE(
            &TWorkerTracker::OnLeadingStopped,
            Unretained(this)));
    }

    std::vector<TWorkerInfo> GetWorkers() const override
    {
        auto guard = Guard(Lock_);

        std::vector<TWorkerInfo> result;
        result.reserve(AddressToWorker_.size());
        for (const auto& [address, worker] : AddressToWorker_) {
            result.push_back(worker->GetInfo());
        }
        return result;
    }

    TWorkerInfo RegisterWorker(
        const TNodeInfoBase& workerNodeInfo,
        std::vector<TWorkerGroupId> groups,
        THashMap<std::string, ssize_t> capabilities) override
    {
        Connector_->ValidateLeader();

        if (workerNodeInfo.BuildVersion != NodeInfo_->BuildVersion) {
            THROW_ERROR_EXCEPTION("Worker %Qv version %Qv differ from controller version %Qv",
                workerNodeInfo.RpcAddress,
                workerNodeInfo.BuildVersion,
                NodeInfo_->BuildVersion);
        }

        auto guard = Guard(Lock_);
        if (auto existingWorker = GetOrDefault(AddressToWorker_, workerNodeInfo.RpcAddress)) {
            YT_LOG_INFO("Kicking out worker due to address conflict (NewWorkerIdentifyingString: %v, ExistingWorkerIdentifyingString: %v, ExistingConnectionIncarnationId: %v)",
                workerNodeInfo.GetIdentifyingString(),
                existingWorker->GetInfo().GetIdentifyingString(),
                existingWorker->GetInfo().ConnectionIncarnationId);
            UnregisterWorker(existingWorker, guard);
        }

        auto connectionIncarnationId = NWorker::TIncarnationId(TGuid::Create());

        auto worker = New<TWorker>(workerNodeInfo, connectionIncarnationId, groups, capabilities);
        EmplaceOrCrash(AddressToWorker_, worker->GetInfo().RpcAddress, worker);

        worker->SetState(EWorkerState::WaitingForInitialHeartbeat);
        worker->SetLease(TLeaseManager::CreateLease(
            DynamicPipelineSpec_.Acquire()->JobManager->LostJobTimeout,
            BIND_NO_PROPAGATE(&TWorkerTracker::OnWorkerLeaseExpired, MakeWeak(this), worker->GetInfo())
                .Via(GetControlInvoker())));

        YT_LOG_INFO("Worker registered (WorkerIdentifyingString: %v, ConnectionIncarnationId: %v)",
            workerNodeInfo.GetIdentifyingString(),
            connectionIncarnationId);

        return worker->GetInfo();
    }

    TWorkerInfo HandleWorkerHeartbeat(
        const std::string& rpcAddress,
        NWorker::TIncarnationId connectionIncarnationId,
        ui64 heartbeatSeqNo) override
    {
        Connector_->ValidateLeader();

        auto guard = Guard(Lock_);
        auto worker = GetOrDefault(AddressToWorker_, rpcAddress);

        if (!worker) {
            THROW_ERROR_EXCEPTION("Worker %Qv is not found",
                rpcAddress);
        }

        if (connectionIncarnationId != worker->GetInfo().ConnectionIncarnationId) {
            THROW_ERROR_EXCEPTION("Wrong worker connection incarnation id: expected %v, got %v",
                worker->GetInfo().ConnectionIncarnationId,
                connectionIncarnationId);
        }

        if (heartbeatSeqNo <= worker->GetHeartbeatSeqNo()) {
            THROW_ERROR_EXCEPTION("Wrong heartbeat seq no: expected at least %v, got %v",
                worker->GetHeartbeatSeqNo() + 1,
                heartbeatSeqNo);
        }

        auto formatCommonLoggingTags = [&, cache = std::string()] () mutable {
            if (cache.empty()) {
                cache = Format("WorkerIdentifyingString: %v, ConnectionIncarnationId: %v, HeartbeatSeqNo: %v",
                    worker->GetInfo().GetIdentifyingString(),
                    worker->GetInfo().ConnectionIncarnationId,
                    heartbeatSeqNo);
            }
            return cache;
        };

        YT_LOG_DEBUG("Worker heartbeat received (%v)", formatCommonLoggingTags());

        if (worker->GetState() == EWorkerState::WaitingForInitialHeartbeat) {
            if (IsFaultyAddress(worker->GetInfo().RpcAddress, guard)) {
                YT_LOG_WARNING("Worker registration confirmed by heartbeat but connection considered faulty (%v)", formatCommonLoggingTags());
                worker->SetState(EWorkerState::RegisteredFaulty);
            } else {
                YT_LOG_INFO("Worker registration confirmed by heartbeat (%v)", formatCommonLoggingTags());
                worker->SetState(EWorkerState::Registered);
            }
        } else if (worker->GetState() == EWorkerState::RegisteredFaulty) {
            if (IsFaultyAddress(worker->GetInfo().RpcAddress, guard)) {
                YT_LOG_WARNING("Worker registration still faulty (%v)", formatCommonLoggingTags());
            } else {
                YT_LOG_INFO("Worker registration became good (%v)", formatCommonLoggingTags());
                worker->SetState(EWorkerState::Registered);
            }
        }

        TLeaseManager::RenewLease(worker->GetLease(), DynamicPipelineSpec_.Acquire()->JobManager->LostJobTimeout);
        worker->SetLastHeartbeatTime(TInstant::Now());
        worker->SetHeartbeatSeqNo(heartbeatSeqNo);

        return worker->GetInfo();
    }

    void Reconfigure(const TDynamicPipelineSpecPtr& dynamicSpec) override
    {
        Executor_->SetPeriod(dynamicSpec->JobManager->FaultyAddressWindow);
        DynamicPipelineSpec_.Store(dynamicSpec);
    }

private:
    const TControlActionQueuePtr ControlQueue_;
    const IYTConnectorPtr Connector_;
    const TNodeInfoPtr NodeInfo_;
    TAtomicIntrusivePtr<TDynamicPipelineSpec> DynamicPipelineSpec_;
    const NConcurrency::TPeriodicExecutorPtr Executor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<std::string, TWorkerPtr> AddressToWorker_;
    THashMap<std::string, TFaultyWorker> FaultyAddresses_;

    void TerminateWorker(const TWorkerPtr& worker, TGuard<NThreading::TSpinLock>& /*guard*/)
    {
        TLeaseManager::CloseLease(worker->GetLease());
        worker->SetLease({});
    }

    bool IsFaultyAddress(const std::string& address, TGuard<NThreading::TSpinLock>& /*guard*/)
    {
        const auto iter = FaultyAddresses_.find(address);
        if (iter == FaultyAddresses_.end()) {
            return false;
        }
        const auto now = TInstant::Now();
        const auto spec = DynamicPipelineSpec_.Acquire()->JobManager;
        const auto failures = iter->second.Failures * std::exp(-(now - iter->second.FailedAt).SecondsFloat() / spec->FaultyAddressWindow.SecondsFloat());
        return failures >= spec->FaultyAddressAttempts;
    }

    void CleanUpFaultyAddresses()
    {
        auto guard = Guard(Lock_);
        const auto now = TInstant::Now();
        const auto spec = DynamicPipelineSpec_.Acquire()->JobManager;
        for (auto iter = FaultyAddresses_.begin(); iter != FaultyAddresses_.end();) {
            if (iter->second.FailedAt + 2 * spec->FaultyAddressWindow < now) {
                FaultyAddresses_.erase(iter++);
            } else {
                ++iter;
            }
        }
    }

    void RegisterWorkerFailure(const std::string& address, TGuard<NThreading::TSpinLock>& /*guard*/)
    {
        const auto now = TInstant::Now();
        auto& faultyAddress = FaultyAddresses_[address];
        faultyAddress.Failures *= std::exp(-(now - faultyAddress.FailedAt).SecondsFloat() / DynamicPipelineSpec_.Acquire()->JobManager->FaultyAddressWindow.SecondsFloat());
        faultyAddress.Failures += 1;
        faultyAddress.FailedAt = now;
    }

    void UnregisterWorker(const TWorkerPtr& worker, TGuard<NThreading::TSpinLock>& guard)
    {
        TerminateWorker(worker, guard);
        EraseOrCrash(AddressToWorker_, worker->GetInfo().RpcAddress);
        RegisterWorkerFailure(worker->GetInfo().RpcAddress, guard);

        YT_LOG_INFO("Worker unregistered (WorkerIdentifyingString: %v, ConnectionIncarnationId: %v, LastHeartbeatTime: %v)",
            worker->GetInfo().GetIdentifyingString(),
            worker->GetInfo().ConnectionIncarnationId,
            worker->GetLastHeartbeatTime());
    }

    void OnWorkerLeaseExpired(TWorkerInfo workerInfo)
    {
        auto guard = Guard(Lock_);
        auto worker = GetOrDefault(AddressToWorker_, workerInfo.RpcAddress);
        if (worker && worker->GetInfo().IncarnationId == workerInfo.IncarnationId) {
            UnregisterWorker(worker, guard);
        }
        auto lastHeartbeatTime = worker ? worker->GetLastHeartbeatTime() : TInstant::Zero();
        guard.Release();

        YT_LOG_EVENT(
            PublicControllerLogger,
            NLogging::ELogLevel::Warning,
            "Worker lease timed out; unregistered (WorkerIdentifyingString: %v, LastHeartbeatTime: %v)",
            workerInfo.GetIdentifyingString(),
            lastHeartbeatTime);
    }

    void OnLeadingStarted()
    {
        DoCleanup();

        YT_LOG_INFO("Worker tracker activated");
    }

    void OnLeadingStopped()
    {
        DoCleanup();

        YT_LOG_INFO("Worker tracker disabled");
    }

    void DoCleanup()
    {
        auto guard = Guard(Lock_);
        for (const auto& [address, worker] : AddressToWorker_) {
            TerminateWorker(worker, guard);
        }
        AddressToWorker_.clear();
    }

    const IInvokerPtr& GetControlInvoker()
    {
        return ControlQueue_->GetInvoker(EControlQueue::WorkerTracker);
    }
};

////////////////////////////////////////////////////////////////////////////////

IWorkerTrackerPtr CreateWorkerTracker(
    TControlActionQueuePtr controlQueue,
    IYTConnectorPtr connector,
    TNodeInfoPtr nodeInfo)
{
    return New<TWorkerTracker>(
        std::move(controlQueue),
        std::move(connector),
        std::move(nodeInfo));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
