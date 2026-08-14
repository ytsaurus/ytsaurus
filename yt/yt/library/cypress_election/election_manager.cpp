#include "election_manager.h"

#include "private.h"
#include "config.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NCypressElection {

using namespace NApi;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TCypressElectionManager
    : public ICypressElectionManager
{
public:
    TCypressElectionManager(
        IClientPtr client,
        IInvokerPtr invoker,
        TCypressElectionManagerConfigPtr config,
        TCypressElectionManagerOptionsPtr options)
        : Config_(std::move(config))
        , Options_(std::move(options))
        , Client_(std::move(client))
        , Invoker_(CreateSerializedInvoker(std::move(invoker), NProfiling::TTagSet({{"invoker", "cypress_election_manager"}, {"group", Options_->GroupName}, {"path", Config_->LockPath}})))
        , Logger(CypressElectionLogger()
            .WithTag("GroupName", Options_->GroupName)
            .WithTag("Path", Config_->LockPath))
        , LockAcquisitionExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TCypressElectionManager::TryAcquireLock, MakeWeak(this)),
            Config_->LockAcquisitionPeriod))
        , LeaderTransactionAttributeCacheExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TCypressElectionManager::UpdateCachedLeaderTransactionAttributes, MakeWeak(this)),
            Config_->LeaderCacheUpdatePeriod))
    { }

    void Start() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_TLOG_DEBUG("Starting Cypress election manager");

        IsActive_ = true;

        LockAcquisitionExecutor_->Start();
        if (Options_->TransactionAttributes) {
            LeaderTransactionAttributeCacheExecutor_->Start();
        }
    }

    TFuture<void> Stop() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_TLOG_DEBUG("Stopping Cypress election manager");

        return BIND(&TCypressElectionManager::DoStop, MakeWeak(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    bool IsActive() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return IsActive_;
    }

    TFuture<void> StopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_TLOG_DEBUG("Stopping leading");

        return BIND(&TCypressElectionManager::DoStopLeading, MakeWeak(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    NPrerequisiteClient::TPrerequisiteId GetPrerequisiteId() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return PrerequisiteTransactionId_.Load();
    }

    TTransactionId GetPrerequisiteTransactionId() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return PrerequisiteTransactionId_.Load();
    }

    bool IsLeader() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return GetPrerequisiteTransactionId() != NullTransactionId;
    }

    IAttributeDictionaryPtr GetCachedLeaderTransactionAttributes() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (IsLeader()) {
            return Options_->TransactionAttributes;
        }

        return CachedLeaderTransactionAttributes_.Acquire();
    }

    DEFINE_SIGNAL_OVERRIDE(void(), LeadingStarted);
    DEFINE_SIGNAL_OVERRIDE(void(), LeadingEnded);

private:
    const TCypressElectionManagerConfigPtr Config_;
    const TCypressElectionManagerOptionsPtr Options_;
    const IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;

    const TPeriodicExecutorPtr LockAcquisitionExecutor_;

    TObjectId LockNodeId_;

    NThreading::TAtomicObject<TTransactionId> PrerequisiteTransactionId_;

    ITransactionPtr Transaction_;
    TObjectId LockId_;

    const TPeriodicExecutorPtr LeaderTransactionAttributeCacheExecutor_;
    TAtomicIntrusivePtr<IAttributeDictionary> CachedLeaderTransactionAttributes_;

    std::atomic<bool> IsActive_ = false;

    void UpdateCachedLeaderTransactionAttributes()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        try {
            GuardedUpdateCachedLeaderTransactionAttributes();
        } catch (const std::exception& ex) {
            YT_TLOG_DEBUG("Leader transaction attribute cache update iteration failed")
                .With(TError(ex));
        }
    }

    void GuardedUpdateCachedLeaderTransactionAttributes()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto locksYson = WaitFor(Client_->GetNode(Config_->LockPath + "/@locks"))
            .ValueOrThrow();
        auto locks = ConvertTo<std::vector<IMapNodePtr>>(locksYson);
        for (const auto& lock : locks) {
            if (ConvertTo<ELockState>(lock->GetChildOrThrow("state")) == ELockState::Acquired) {
                auto transactionId = ConvertTo<TTransactionId>(lock->GetChildOrThrow("transaction_id"));
                YT_VERIFY(Options_->TransactionAttributes);
                TGetNodeOptions options{.Attributes = Options_->TransactionAttributes->ListKeys()};
                auto responseYson = WaitFor(Client_->GetNode(FromObjectId(transactionId) + "/@", options))
                    .ValueOrThrow();
                CachedLeaderTransactionAttributes_.Store(ConvertToAttributes(responseYson));
                break;
            }
        }
    }

    void TryAcquireLock()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (IsLeader()) {
            return;
        }

        try {
            if (!LockNodeId_) {
                YT_TLOG_DEBUG("Creating lock node");

                CreateLockNode();

                YT_TLOG_DEBUG("Lock node created")
                    .With("LockNodeId", LockNodeId_);
            }

            if (!Transaction_) {
                YT_TLOG_DEBUG("Starting transaction");

                StartTransaction();

                YT_TLOG_DEBUG("Transaction started")
                    .With("TransactionId", Transaction_->GetId());
            }

            if (!LockId_) {
                auto transactionId = Transaction_->GetId();
                YT_TLOG_DEBUG("Creating lock")
                    .With("TransactionId", transactionId);

                CreateLock();

                YT_TLOG_DEBUG("Lock created")
                    .With("TransactionId", transactionId)
                    .With("LockId", LockId_)
                    .With("TransactionStillAlive", Transaction_ != nullptr);
            }

            if (CheckLockAcquired()) {
                YT_TLOG_DEBUG("Lock is acquired, starting leading")
                    .With("LockId", LockId_);

                OnLeadingStarted();
            } else {
                YT_TLOG_DEBUG("Lock is not acquired yet, skipping")
                    .With("LockId", LockId_);
            }
        } catch (const std::exception& ex) {
            YT_TLOG_INFO("Lock acquisition iteration failed")
                .With(TError(ex));
        }
    }

    void StartTransaction()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(!Transaction_);
        YT_VERIFY(!IsLeader());

        auto attributes = Options_->TransactionAttributes
            ? Options_->TransactionAttributes->Clone()
            : CreateEphemeralAttributes();
        auto title = Format("Lock transaction for %v:%v",
            Options_->GroupName,
            Options_->MemberName);
        attributes->Set("title", std::move(title));
        TTransactionStartOptions options {
            .Timeout = Config_->TransactionTimeout,
            .PingPeriod = Config_->TransactionPingPeriod,
            .MasterExpirationMode = Config_->MasterTransactionExpirationMode,
            .Attributes = std::move(attributes),
        };
        Transaction_ = WaitFor(
            Client_->StartTransaction(ETransactionType::Master, std::move(options)))
            .ValueOrThrow();

        auto transactionId = Transaction_->GetId();
        Transaction_->SubscribeAborted(
            BIND(&TCypressElectionManager::OnTransactionAborted, MakeWeak(this), transactionId)
                .Via(Invoker_));
        Transaction_->SubscribeCommitted(
            BIND(&TCypressElectionManager::OnTransactionCommitted, MakeWeak(this), transactionId)
                .Via(Invoker_));
    }

    void CreateLock()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(!LockId_);
        YT_VERIFY(!IsLeader());

        auto transactionId = Transaction_->GetId();
        TLockNodeOptions options;
        options.TransactionId = transactionId;
        options.Waitable = true;
        auto rspOrError = WaitFor(
            Client_->LockNode(FromObjectId(LockNodeId_), ELockMode::Exclusive, std::move(options)));
        if (rspOrError.IsOK()) {
            LockId_ = rspOrError.Value().LockId;
        } else {
            // NB: If transaction has created lock, but response was lost, creating a new lock
            // will end up with a conflict, so it's safer to create a new transaction in case
            // of any errors.
            YT_TLOG_DEBUG("Failed to create lock")
                .With("TransactionId", transactionId)
                .With(rspOrError);
            LockNodeId_ = NullObjectId;
            Transaction_.Reset();
            rspOrError.ThrowOnError();
        }
    }

    bool CheckLockAcquired()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(!IsLeader());

        TGetNodeOptions options{
            .Attributes = {"state"},
        };
        auto rspOrError = WaitFor(Client_->GetNode(FromObjectId(LockId_), std::move(options)));
        if (rspOrError.IsOK()) {
            auto response = ConvertTo<INodePtr>(rspOrError.Value());
            auto lockState = response->Attributes().Get<ELockState>("state");
            return lockState == ELockState::Acquired;
        } else if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            YT_TLOG_DEBUG("Lock does not exist")
                .With("LockId", LockId_)
                .With(rspOrError);
            Transaction_.Reset();
            LockId_ = NullObjectId;
            return false;
        } else {
            rspOrError.ThrowOnError();
        }

        YT_ABORT();
    }

    void OnTransactionAborted(TTransactionId transactionId, const TError& error)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_TLOG_DEBUG("Transaction aborted")
            .With("TransactionId", transactionId)
            .With(error);

        OnTransactionFinished(transactionId);
    }

    void OnTransactionCommitted(TTransactionId transactionId)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_TLOG_DEBUG("Transacton committed")
            .With("TransactionId", transactionId);

        OnTransactionFinished(transactionId);
    }

    void OnTransactionFinished(TTransactionId transactionId)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        // NB: Stale callbacks are possible.
        if (!Transaction_ || Transaction_->GetId() != transactionId) {
            return;
        }

        Reset();
    }

    void OnLeadingStarted()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(!IsLeader());

        if (!Transaction_) {
            YT_TLOG_WARNING("Transaction expired just before leading started");
            return;
        }

        PrerequisiteTransactionId_.Store(Transaction_->GetId());

        YT_TLOG_DEBUG("Leading started");

        try {
            TForbidContextSwitchGuard guard;
            LeadingStarted_.Fire();
        } catch (const std::exception& ex) {
            YT_TLOG_ALERT("Unexpected error occurred during leading start")
                .With(TError(ex));
        }
    }

    void DoStop()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (Options_->TransactionAttributes) {
            WaitFor(LeaderTransactionAttributeCacheExecutor_->Stop())
                .ThrowOnError();
        }
        WaitFor(LockAcquisitionExecutor_->Stop())
            .ThrowOnError();

        Reset();

        IsActive_ = false;

        YT_TLOG_DEBUG("Election manager stopped");
    }

    void DoStopLeading()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (IsLeader()) {
            Reset();
        }
    }

    void Reset()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (IsLeader()) {
            YT_TLOG_DEBUG("Leading ended");

            PrerequisiteTransactionId_.Store(NullTransactionId);

            try {
                TForbidContextSwitchGuard guard;
                LeadingEnded_.Fire();
            } catch (const std::exception& ex) {
                YT_TLOG_ALERT("Unexpected error occurred during leading end")
                    .With(TError(ex));
            }
        }

        Transaction_.Reset();
        LockId_ = NullObjectId;
    }

    void CreateLockNode()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(!LockNodeId_);

        TCreateNodeOptions options;
        options.IgnoreExisting = true;
        options.IgnoreTypeMismatch = true;
        LockNodeId_ = WaitFor(
            Client_->CreateNode(Config_->LockPath, EObjectType::MapNode, std::move(options)))
            .ValueOrThrow();
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressElectionManagerPtr CreateCypressElectionManager(
    IClientPtr client,
    IInvokerPtr invoker,
    TCypressElectionManagerConfigPtr config,
    TCypressElectionManagerOptionsPtr options)
{
    return New<TCypressElectionManager>(
        std::move(client),
        std::move(invoker),
        std::move(config),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressElection
