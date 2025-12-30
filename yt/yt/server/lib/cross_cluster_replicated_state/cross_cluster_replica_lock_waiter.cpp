#include "cross_cluster_replica_lock_waiter.h"

#include "config.h"

#include <yt/yt/client/api/cypress_client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NCrossClusterReplicatedState {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TLockEntry
    : public TRefCounted
{
public:
    TLockEntry(
        NCypressClient::TLockId lockId,
        NApi::ITransactionPtr transaction)
        : LockId_(lockId)
        , Transaction_(std::move(transaction))
    { }

    TFuture<ELockState> CheckLockState()
    {
        auto checkState = [this_ = MakeStrong(this)] (TYsonString&& rsp) {
            auto response = ConvertToNode(rsp);
            return MakeFuture(response->Attributes().Get<ELockState>("state"));
        };

        TGetNodeOptions options{
            .Attributes = {"state"},
        };
        return Transaction_->GetNode(FromObjectId(LockId_), options)
            .AsUnique()
            .Apply(BIND(std::move(checkState)));
    }

private:
    TLockId LockId_;
    ITransactionPtr Transaction_;
};

////////////////////////////////////////////////////////////////////////////////

class TWaitingLockEntry
    : public TRefCounted
{
public:
    TWaitingLockEntry(
        TIntrusivePtr<TLockEntry> lockEntry,
        ISingleClusterClientPtr client,
        TYPath nodePath,
        TReplicaVersion targetVersion,
        TDuration timeout)
        : LockEntry_(std::move(lockEntry))
        , Client_(std::move(client))
        , Acquired_(NewPromise<ELockAcquisionState>())
        , NodePath_(std::move(nodePath))
        , TargetVersion_(std::move(targetVersion))
        , Deadline_(TInstant::Now() + timeout)
    { }

    TFuture<ELockAcquisionState> GetAcquisionFuture()
    {
        return Acquired_.ToFuture();
    }

    bool IsReadyOrCanceled() const
    {
        return Acquired_.IsSet() || Acquired_.IsCanceled();
    }

    void Stop()
    {
        Acquired_.Set(TError("Lock waiting periodic has been stopped"));
    }

    TFuture<void> CheckLock()
    {
        if (TInstant::Now() > Deadline_) {
            auto exceededError = TError(NYT::EErrorCode::Timeout, "Periodic timeout exceeded")
                << TErrorAttribute("replica_index", Client_->GetIndex())
                << TErrorAttribute("node_path", NodePath_)
                << TErrorAttribute("target_version", TargetVersion_);
            Acquired_.Set(std::move(exceededError));
            return OKFuture;
        }

        auto checkVersion = [this, this_ = MakeStrong(this)] (TYsonString&& rsp) {
            auto node = ConvertToNode(rsp);
            auto version = ExtractVersion(node);
            if (version >= TargetVersion_) {
                Acquired_.Set(ELockAcquisionState::Outdated);
                return OKFuture;
            }

            return LockEntry_->CheckLockState()
                .Apply(BIND([this, this_] (ELockState lockState) {
                    if (lockState == ELockState::Acquired) {
                        Acquired_.Set(ELockAcquisionState::Acquired);
                    }
                }));
        };

        return Client_->GetClient()->GetNode(NodePath_)
            .AsUnique()
            .Apply(BIND(std::move(checkVersion)))
            .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    Acquired_.Set(error);
                }
            }));
    }

private:
    TIntrusivePtr<TLockEntry> LockEntry_;
    ISingleClusterClientPtr Client_;
    TPromise<ELockAcquisionState> Acquired_;
    TYPath NodePath_;
    TReplicaVersion TargetVersion_;
    TInstant Deadline_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TCrossClusterReplicaLockWaiter
    : public ICrossClusterReplicaLockWaiter
{
public:
    TCrossClusterReplicaLockWaiter(
        IInvokerPtr invoker,
        const TCrossClusterReplicatedStateConfigPtr& config,
        TLogger logger)
        : LockWaiterInvoker_(CreateSerializedInvoker(std::move(invoker)))
        , CheckLocksExecutor_(New<TPeriodicExecutor>(
            LockWaiterInvoker_,
            BIND(&TCrossClusterReplicaLockWaiter::OnCheckLocks, MakeWeak(this)),
            config->LockCheckPeriod))
        , Logger(std::move(logger))
    { }

    void Start() override
    {
        CheckLocksExecutor_->Start();
    }

    TFuture<void> Stop() override
    {
        auto stopAllLockEntries = [this, this_ = MakeStrong(this)] {
            for (auto& runningCheck : RunningChecks_ | std::views::values) {
                runningCheck.Cancel(TError("Lock waiting periodic has been stopped"));
            }
            for (const auto& lockEntry : LockEntries_ | std::views::values) {
                if (lockEntry->IsReadyOrCanceled()) {
                    continue;
                }
                lockEntry->Stop();
            }

            LockEntries_.clear();
        };
        return CheckLocksExecutor_->Stop()
            .Apply(BIND(std::move(stopAllLockEntries)));
    }

    TFuture<ELockAcquisionState> WaitForLockAcquision(
        NCypressClient::TLockId lockId,
        ISingleClusterClientPtr client,
        ITransactionPtr transaction,
        TYPath nodePath,
        TReplicaVersion targetVersion,
        TDuration timeout) override
    {
        auto lockEntry = New<TLockEntry>(lockId, std::move(transaction));

        auto enqueueIfNotAcquired =
            [
                client = std::move(client),
                nodePath = std::move(nodePath),
                targetVersion = std::move(targetVersion),
                lockEntry,
                lockId,
                timeout,
                this,
                this_ = MakeStrong(this)
            ] (ELockState lockState) mutable {
                if (lockState == ELockState::Acquired) {
                    return MakeFuture(ELockAcquisionState::Acquired);
                }
                YT_LOG_DEBUG(
                    "Starting waiting (ReplicaIndex: %v, ReplicaVersionTag: %v)",
                    client->GetIndex(),
                    targetVersion.second);

                auto waitingLockEntry = New<TWaitingLockEntry>(
                    std::move(lockEntry),
                    std::move(client),
                    std::move(nodePath),
                    std::move(targetVersion),
                    timeout);
                auto acquisionFuture = waitingLockEntry->GetAcquisionFuture();

                LockEntries_.emplace(lockId, std::move(waitingLockEntry));
                return acquisionFuture;
            };

        return lockEntry->CheckLockState()
            .Apply(BIND(std::move(enqueueIfNotAcquired)).AsyncVia(LockWaiterInvoker_));
    }

private:
    THashMap<TLockId, TIntrusivePtr<TWaitingLockEntry>> LockEntries_;
    THashMap<TLockId, TFuture<void>> RunningChecks_;

    IInvokerPtr LockWaiterInvoker_;
    TPeriodicExecutorPtr CheckLocksExecutor_;

    TLogger Logger;

    void OnCheckLocks()
    {
        YT_LOG_DEBUG("Starting check locks iteration (LockEntriesSize: %v)", LockEntries_.size());
        TCompactVector<TLockId, 4> locksToErase;
        for (const auto& [lockId, lockEntry] : LockEntries_) {
            if (lockEntry->IsReadyOrCanceled()) {
                locksToErase.push_back(lockId);
                continue;
            }
            if (auto runningCheck = RunningChecks_.find(lockId); runningCheck != RunningChecks_.end()) {
                if (!runningCheck->second.IsSet()) {
                    runningCheck->second.Cancel(TError("Exceeded lock check time"));
                    YT_LOG_DEBUG("Exceeded lock check time (LockId: %v)", lockId);
                } else {
                    const auto& checkResult = runningCheck->second.Get();
                    if (!checkResult.IsOK()) {
                        YT_LOG_DEBUG(checkResult, "Lock check failed (LockId: %v)", lockId);
                    }
                }
            }
            RunningChecks_.insert_or_assign(lockId, lockEntry->CheckLock());
        }
        for (auto lockId : locksToErase) {
            LockEntries_.erase(lockId);
            RunningChecks_.erase(lockId);
        }
    }
};

ICrossClusterReplicaLockWaiterPtr CreateCrossClusterReplicaLockWaiter(
    IInvokerPtr invoker, TCrossClusterReplicatedStateConfigPtr config, NLogging::TLogger logger)
{
    return New<TCrossClusterReplicaLockWaiter>(std::move(invoker), std::move(config), std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NCrossClusterReplicatedState
