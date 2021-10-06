#include "election_manager.h"

#include "private.h"
#include "config.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NCypressElection {

using namespace NApi;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NLogging;
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
        , Invoker_(CreateSerializedInvoker(std::move(invoker)))
        , Logger(CypressElectionLogger.WithTag("Name: %v, Path: %v", Options_->Name, Config_->LockPath))
        , LockAquisitionExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TCypressElectionManager::AcquireLock, MakeWeak(this)),
            Config_->LockAcquisitionPeriod))
    { }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Starting cypress election manager", Config_->LockPath);
        LockAquisitionExecutor_->Start();
    }

    void Stop() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Cypress election manager stopping");

        WaitFor(BIND(&TCypressElectionManager::DoStop, MakeWeak(this))
            .AsyncVia(Invoker_)
            .Run())
            .ThrowOnError();
    }

    void StopLeading() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Stopping leading");

        // NB: Aborts the transaction.
        Invoker_->Invoke(BIND(&TCypressElectionManager::OnLeadingEnded, MakeWeak(this)));
    }

    TTransactionId GetPrerequistiveTransactionId() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(TransactionLock_);

        if (IsLeading_) {
            YT_VERIFY(Transaction_);
            return Transaction_->GetId();
        } else {
            return NullTransactionId;
        }
    }

    DEFINE_SIGNAL_OVERRIDE(void(), LeadingStarted);
    DEFINE_SIGNAL_OVERRIDE(void(), LeadingEnded);

private:
    const TCypressElectionManagerConfigPtr Config_;
    const TCypressElectionManagerOptionsPtr Options_;
    const IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;

    const TPeriodicExecutorPtr LockAquisitionExecutor_;

    std::atomic<bool> IsLeading_ = false;
    ITransactionPtr Transaction_;
    YT_DECLARE_SPINLOCK(TAdaptiveLock, TransactionLock_);

    void AcquireLock()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(!IsLeading_);

        YT_LOG_DEBUG("Trying acquire lock");

        try {
            if (!Transaction_) {
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Format("Lock transaction for %v", Options_->Name));
                TTransactionStartOptions options {
                    .PingPeriod = Config_->TransactionPingPeriod,
                    .Timeout = Config_->TransactionTimeout,
                    .Attributes = std::move(attributes),
                };
                Transaction_ = WaitFor(
                    Client_->StartTransaction(ETransactionType::Master, std::move(options)))
                    .ValueOrThrow();
                YT_LOG_DEBUG("Lock transaction started (TransactionId: %v)", Transaction_->GetId());
            }
            auto result = WaitFor(Transaction_->LockNode(Config_->LockPath, ELockMode::Exclusive));
            if (result.IsOK()) {
                YT_LOG_DEBUG(
                    "Lock acquisition succeeded (TransactionId: %v, LockId: %v)",
                    Transaction_->GetId(),
                    result.Value().LockId);
                OnLeadingStarted();
            } else if (result.GetCode() == NTransactionClient::EErrorCode::NoSuchTransaction) {
                Transaction_.Reset();
            }
            result.ThrowOnError();
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Lock acquisition failed");
            return;
        }
    }

    void OnTransactionAborted(const TError& /*error*/)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        OnLeadingEnded();
    }

    void OnLeadingStarted()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        IsLeading_ = true;

        LockAquisitionExecutor_->Stop();

        YT_LOG_DEBUG("Leading is started");

        LeadingStarted_.Fire();

        Transaction_->SubscribeAborted(
            BIND(&TCypressElectionManager::OnTransactionAborted, MakeWeak(this)).Via(Invoker_));
        Transaction_->SubscribeCommitted(
            BIND(&TCypressElectionManager::OnLeadingEnded, MakeWeak(this)).Via(Invoker_));

        // In case transaction has aborted before subscription on abort.
        if (!WaitFor(Transaction_->Ping()).IsOK()) {
            OnLeadingEnded();
        }
    }

    void OnLeadingEnded()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!IsLeading_) {
            return;
        }

        {
            auto guard = Guard(TransactionLock_);
            Transaction_.Reset();
            IsLeading_ = false;
        }

        YT_LOG_DEBUG("Leading ended");

        LockAquisitionExecutor_->Start();

        LeadingEnded_.Fire();
    }

    void DoStop()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        OnLeadingEnded();
        LockAquisitionExecutor_->Stop();
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
