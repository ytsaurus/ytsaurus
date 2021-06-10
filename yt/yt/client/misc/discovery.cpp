#include "discovery.h"
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TDiscovery::TDiscovery(
    TDiscoveryConfigPtr config,
    NApi::IClientPtr client,
    IInvokerPtr invoker,
    std::vector<TString> extraAttributes,
    NLogging::TLogger logger)
    : Config_(config)
    , Client_(client)
    , Invoker_(invoker)
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        invoker,
        BIND(&TDiscovery::DoUpdateList, MakeWeak(this)),
        Config_->UpdatePeriod))
    , Logger(logger.WithTag("Group: %v, DiscoveryId: %v",
        Config_->Directory,
        TGuid::Create()))
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

TFuture<void> TDiscovery::UpdateList(TDuration ageThreshold)
{
    auto guard = WriterGuard(Lock_);
    if (LastUpdate_ + ageThreshold >= TInstant::Now()) {
        return VoidFuture;
    }
    if (!ScheduledForceUpdate_ || ScheduledForceUpdate_.IsSet()) {
        ScheduledForceUpdate_ = BIND(&TDiscovery::GuardedUpdateList, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
        YT_LOG_DEBUG("Force update scheduled");
    }
    return ScheduledForceUpdate_;
}

THashMap<TString, IAttributeDictionaryPtr> TDiscovery::List(bool includeBanned) const
{
    THashMap<TString, IAttributeDictionaryPtr> result;
    THashMap<TString, TInstant> bannedUntil;
    decltype(NameAndAttributes_) nameAndAttributes;
    {
        auto guard = ReaderGuard(Lock_);
        result = List_;
        bannedUntil = BannedUntil_;
        nameAndAttributes = NameAndAttributes_;
    }
    auto now = TInstant::Now();
    if (nameAndAttributes) {
        result.insert(*nameAndAttributes);
    }
    if (!includeBanned) {
        for (auto it = result.begin(); it != result.end();) {
            auto banIt = bannedUntil.find(it->first);
            if (banIt != bannedUntil.end() && now < banIt->second) {
                result.erase(it++);
            } else {
                ++it;
            }
        }
    }
    return result;
}

void TDiscovery::Ban(const TString& name)
{
    Ban(std::vector{name});
}

void TDiscovery::Ban(const std::vector<TString>& names)
{
    if (names.empty()) {
        return;
    }
    auto guard = WriterGuard(Lock_);
    auto banDeadline = TInstant::Now() + Config_->BanTimeout;
    for (const auto& name : names) {
        BannedUntil_[name] = banDeadline;
    }
    YT_LOG_INFO("Participants banned (Names: %v, Until: %v)", names, banDeadline);
}

void TDiscovery::Unban(const TString& name)
{
    Unban(std::vector{name});
}

void TDiscovery::Unban(const std::vector<TString>& names)
{
    if (names.empty()) {
        return;
    }
    auto guard = WriterGuard(Lock_);
    for (const auto& name : names) {
        if (auto it = BannedUntil_.find(name); it != BannedUntil_.end()) {
            BannedUntil_.erase(it);
            YT_LOG_INFO("Participant unbanned (Name: %v)", name);
        }
    }
}

TFuture<void> TDiscovery::StartPolling()
{
    PeriodicExecutor_->Start();
    return PeriodicExecutor_->GetExecutedEvent();
}

TFuture<void> TDiscovery::StopPolling()
{
    return PeriodicExecutor_->Stop();
}

i64 TDiscovery::GetWeight()
{
    auto guard = ReaderGuard(Lock_);
    return List_.size();
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

void TDiscovery::GuardedUpdateList()
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

void TDiscovery::DoUpdateList()
{
    try {
        GuardedUpdateList();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to update discovery");
    }
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
        transaction->Abort();
        return;
    }

    TLockNodeOptions lockOptions;
    lockOptions.ChildKey = "lock";

    auto nodePath = Config_->Directory + "/" + NameAndAttributes_->first;

    auto lock = WaitFor(transaction->LockNode(nodePath, NCypressClient::ELockMode::Shared, lockOptions))
        .ValueOrThrow();

    if (Epoch_ != epoch) {
        transaction->Abort();
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
            YT_LOG_ERROR(ex, "Error occured while restoring lock of the node");
            TDelayedExecutor::WaitForDuration(Config_->TransactionPingPeriod);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
