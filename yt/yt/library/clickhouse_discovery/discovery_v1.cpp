#include "discovery_v1.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TDiscovery::TDiscovery(
    TDiscoveryV1ConfigPtr config,
    NApi::IClientPtr client,
    IInvokerPtr invoker,
    std::vector<TString> extraAttributes,
    NLogging::TLogger logger)
    : TDiscoveryBase(config, std::move(invoker), std::move(logger))
    , Config_(std::move(config))
    , Client_(client)
    , Epoch_(0)
{
    if (std::find(extraAttributes.begin(), extraAttributes.end(), "locks") == extraAttributes.end()) {
        extraAttributes.push_back("locks");
    }
    ListOptions_.Attributes = std::move(extraAttributes);
    // TMasterReadOptions
    ListOptions_.ReadFrom = Config_->ReadFrom;
    ListOptions_.ExpireAfterSuccessfulUpdateTime = Config_->MasterCacheExpireTime;
    ListOptions_.ExpireAfterFailedUpdateTime = Config_->MasterCacheExpireTime;
}

TFuture<void> TDiscovery::Enter(TString name, IAttributeDictionaryPtr attributes)
{
    return BIND(&TDiscovery::DoEnter, MakeStrong(this), std::move(name), std::move(attributes))
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TDiscovery::Leave()
{
    return BIND(&TDiscovery::DoLeave, MakeStrong(this))
        .AsyncVia(Invoker_)
        .Run();
}

int TDiscovery::Version() const
{
    return 1;
}

void TDiscovery::DoEnter(TString name, IAttributeDictionaryPtr attributes)
{
    YT_VERIFY(!Transaction_);
    YT_LOG_INFO("Entering the group");

    {
        auto guard = WriterGuard(Lock_);
        NameAndAttributes_ = {name, attributes};
    }

    TransactionAbortedHandler_ = BIND(&TDiscovery::DoRestoreTransaction, MakeWeak(this), Epoch_)
        .Via(Invoker_);

    DoCreateNode(Epoch_);
    DoLockNode(Epoch_);

    YT_LOG_INFO("Entered the group");
}

void TDiscovery::DoLeave()
{
    ++Epoch_;

    // Transaction can be null during "restore" routine.
    // An epoch increment will stop restore attempts, so no more actions are required in this case.
    if (Transaction_) {
        Transaction_->UnsubscribeAborted(TransactionAbortedHandler_);

        auto transactionId = Transaction_->GetId();
        auto error = WaitFor(Transaction_->Abort());
        if (!error.IsOK()) {
            // Transaction may already expire and lock will be dead.
            YT_LOG_INFO("Error during aborting transaction (Error: %v)", error);
        }
        YT_LOG_INFO("Left the group (TransactionId: %v)", transactionId);
    } else {
        YT_LOG_INFO("Left the group, transaction is already dead");
    }

    {
        auto guard = WriterGuard(Lock_);
        NameAndAttributes_.reset();
    }

    Transaction_.Reset();
}

void TDiscovery::DoUpdateList()
{
    auto list = ConvertToNode(WaitFor(Client_->ListNode(Config_->Directory, ListOptions_))
        .ValueOrThrow());
    THashMap<TString, IAttributeDictionaryPtr> newList;

    i64 aliveCount = 0;
    i64 deadCount = 0;

    for (const auto& node : list->AsList()->GetChildren()) {
        bool isAlive = true;

        if (Config_->SkipUnlockedParticipants) {
            isAlive = false;
            auto locks = node->Attributes().Find<IListNodePtr>("locks");
            for (const auto& lockNode : locks->GetChildren()) {
                auto lock = lockNode->AsMap();
                auto childKey = lock->FindChild("child_key");
                if (childKey && childKey->AsString()->GetValue() == "lock") {
                    isAlive = true;
                    break;
                }
            }
        }

        if (isAlive) {
            ++aliveCount;
            newList[node->GetValue<TString>()] = node->Attributes().Clone();
        } else {
            ++deadCount;
        }
    }
    {
        auto guard = WriterGuard(Lock_);
        swap(List_, newList);
        LastUpdate_ = TInstant::Now();
    }
    YT_LOG_DEBUG("List of participants updated (Alive: %v, Dead: %v)", aliveCount, deadCount);
}

void TDiscovery::DoCreateNode(int epoch)
{
    if (Epoch_ != epoch) {
        return;
    }

    const auto& [name, userAttributes] = *NameAndAttributes_;

    auto attributes = ConvertToAttributes(userAttributes);
    // Set expiration time to remove old instances from the clique directory.
    // Regardless of the expiration time, the node will be alive until transaction is aborted.
    // But there is a gap between creation and locking the node, so set a few minutes from now to avoid deleting the node before locking it.
    attributes->SetYson("expiration_time", ConvertToYsonString(TInstant::Now() + Config_->LockNodeTimeout));

    TCreateNodeOptions createOptions;
    createOptions.IgnoreExisting = true;
    createOptions.Attributes = std::move(attributes);

    WaitFor(Client_->CreateNode(Config_->Directory + "/" + name, NObjectClient::EObjectType::MapNode, createOptions))
        .ThrowOnError();

    YT_LOG_DEBUG("Instance node created (Name: %v)", name);
}

void TDiscovery::DoLockNode(int epoch)
{
    if (Epoch_ != epoch) {
        return;
    }

    TTransactionStartOptions transactionOptions {
        .Timeout = Config_->TransactionTimeout,
        .PingPeriod = Config_->TransactionPingPeriod,
    };
    auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Master, transactionOptions))
        .ValueOrThrow();

    YT_LOG_DEBUG("Transaction for lock started (TransactionId: %v)",
        transaction->GetId());

    if (Epoch_ != epoch) {
        YT_UNUSED_FUTURE(transaction->Abort());
        return;
    }

    TLockNodeOptions lockOptions;
    lockOptions.ChildKey = "lock";

    auto nodePath = Config_->Directory + "/" + NameAndAttributes_->first;

    auto lock = WaitFor(transaction->LockNode(nodePath, NCypressClient::ELockMode::Shared, lockOptions))
        .ValueOrThrow();

    if (Epoch_ != epoch) {
        YT_UNUSED_FUTURE(transaction->Abort());
        return;
    }

    Transaction_ = std::move(transaction);
    // Set it here to avoid restoring transaction without lock.
    Transaction_->SubscribeAborted(TransactionAbortedHandler_);

    YT_LOG_DEBUG("Lock completed (TransactionId: %v, LockId: %v)",
        Transaction_->GetId(),
        lock.LockId);
}

void TDiscovery::DoRestoreTransaction(int epoch, const TError& error)
{
    YT_LOG_WARNING(error, "Lock transaction aborted (Epoch: %v)", epoch);
    while (Epoch_ == epoch) {
        try {
            Transaction_.Reset();
            DoCreateNode(epoch);
            DoLockNode(epoch);
            break;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error occurred while restoring lock of the node");
            TDelayedExecutor::WaitForDuration(Config_->TransactionPingPeriod);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

IDiscoveryPtr CreateDiscoveryV1(
    TDiscoveryV1ConfigPtr config,
    NApi::IClientPtr client,
    IInvokerPtr invoker,
    std::vector<TString> extraAttributes,
    NLogging::TLogger logger)
{
    return New<TDiscovery>(
        std::move(config),
        std::move(client),
        std::move(invoker),
        std::move(extraAttributes),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
