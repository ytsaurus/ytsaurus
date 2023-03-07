#include "discovery.h"
#include <yt/client/api/transaction.h>

#include <yt/core/concurrency/periodic_executor.h>

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
    , Logger(logger
        .AddTag("Group: %v", Config_->Directory)
        .AddTag("DiscoveryId: %v", TGuid::Create()))
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

TFuture<void> TDiscovery::Enter(TString name, TAttributeMap attributes)
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
    TWriterGuard guard(Lock_);
    if (LastUpdate_ + ageThreshold >= TInstant::Now()) {
        return VoidFuture;
    }
    if (!ScheduledForceUpdate_ || ScheduledForceUpdate_.IsSet()) {
        ScheduledForceUpdate_ = BIND(&TDiscovery::DoUpdateList, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
        YT_LOG_DEBUG("Force update scheduled");
    }
    return ScheduledForceUpdate_;
}

THashMap<TString, TAttributeMap> TDiscovery::List(bool includeBanned) const
{
    THashMap<TString, TAttributeMap> result;
    THashMap<TString, TInstant> bannedSince;
    decltype(NameAndAttributes_) nameAndAttributes;
    {
        TReaderGuard guard(Lock_);
        result = List_;
        bannedSince = BannedSince_;
        nameAndAttributes = NameAndAttributes_;
    }
    auto now = TInstant::Now();
    if (nameAndAttributes) {
        result.insert(*nameAndAttributes);
    }
    if (!includeBanned) {
        for (auto it = result.begin(); it != result.end();) {
            auto banIt = bannedSince.find(it->first);
            if (banIt != bannedSince.end() && (banIt->second + Config_->BanTimeout) > now) {
                result.erase(it++);
            } else {
                ++it;
            }
        }
    }
    return result;
}

void TDiscovery::Ban(TString name)
{
    TWriterGuard guard(Lock_);
    BannedSince_[name] = TInstant::Now();
    YT_LOG_INFO("Participant banned (Name: %v, Duration: %v)", name, Config_->BanTimeout);
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
    TReaderGuard guard(Lock_);
    return List_.size();
}

void TDiscovery::DoEnter(TString name, TAttributeMap attributes)
{
    YT_VERIFY(!Transaction_);
    YT_LOG_INFO("Entering the group");

    {
        TWriterGuard guard(Lock_);
        NameAndAttributes_ = {name, attributes};
    }

    TransactionRestorer_ = BIND(&TDiscovery::DoRestoreTransaction, MakeWeak(this), Epoch_)
        .Via(Invoker_);

    DoCreateNode(Epoch_);
    DoLockNode(Epoch_);

    YT_LOG_INFO("Entered the group");
}

void TDiscovery::DoLeave()
{
    YT_VERIFY(Transaction_);

    ++Epoch_;

    Transaction_->UnsubscribeAborted(TransactionRestorer_);

    auto transactionId = Transaction_->GetId();
    auto error = WaitFor(Transaction_->Abort());
    if (!error.IsOK()) {
        // Transaction may already expire and lock will be dead.
        YT_LOG_INFO("Error during aborting transaction (Error: %v)", error);
    }

    YT_LOG_INFO("Left the group (TransactionId: %v)", transactionId);

    {
        TWriterGuard guard(Lock_);
        NameAndAttributes_.reset();
    }

    Transaction_.Reset();
}

void TDiscovery::DoUpdateList()
{
    auto list = ConvertToNode(WaitFor(Client_->ListNode(Config_->Directory, ListOptions_))
        .ValueOrThrow());
    THashMap<TString, TAttributeMap> newList;

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
            auto stringNode = node->AsString();
            auto attributes = stringNode->Attributes().ToMap()->GetChildren();
            newList[stringNode->GetValue()] = TAttributeMap(attributes.begin(), attributes.end());
        } else {
            ++deadCount;
        }
    }
    {
        TWriterGuard guard(Lock_);
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

    const auto& [name, attributes] = *NameAndAttributes_;

    TCreateNodeOptions createOptions;
    createOptions.IgnoreExisting = true;
    createOptions.Attributes = ConvertToAttributes(attributes);

    WaitFor(Client_->CreateNode(Config_->Directory + "/" + name, NObjectClient::EObjectType::MapNode, createOptions))
        .ThrowOnError();

    YT_LOG_DEBUG("Instance node created (Name: %v)", name);
}

void TDiscovery::DoLockNode(int epoch)
{
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
    Transaction_->SubscribeAborted(TransactionRestorer_);

    // After transaction is aborted the node will be unlocked and removed.
    WaitFor(Client_->SetNode(nodePath + "/@expiration_time", ConvertToYsonString(TInstant::Now())))
        .ThrowOnError();

    YT_LOG_DEBUG("Lock completed (TransactionId: %v, LockId: %v)",
        Transaction_->GetId(),
        lock.LockId);
}

void TDiscovery::DoRestoreTransaction(int epoch)
{
    YT_LOG_WARNING("Lock transaction aborted (Epoch: %v)", epoch);
    while (Epoch_ == epoch) {
        try {
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
