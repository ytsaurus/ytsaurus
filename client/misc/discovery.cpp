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
    const NLogging::TLogger& logger)
    : Config_(config)
    , Client_(client)
    , Invoker_(invoker)
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        invoker,
        BIND(&TDiscovery::UpdateList, MakeWeak(this)),
        config->UpdatePeriod))
    , Logger(logger)
{
    if (std::find(extraAttributes.begin(), extraAttributes.end(), "locks") == extraAttributes.end()) {
        extraAttributes.push_back("locks");
    }
    ListOptions_.Attributes = std::move(extraAttributes);
    Logger.AddTag("Group: %v", Config_->Directory);
}

TFuture<void> TDiscovery::Enter(TString name, TAttributeDictionary attributes)
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

THashMap<TString, TDiscovery::TAttributeDictionary> TDiscovery::List() const
{
    THashMap<TString, TAttributeDictionary> result;
    THashMap<TString, TInstant> bannedSince;
    {
        TReaderGuard guard(Lock_);
        result = List_;
        bannedSince = BannedSince_;
    }
    auto now = TInstant::Now();
    for (auto it = result.begin(); it != result.end();) {
        auto banIt = bannedSince.find(it->first);
        if (banIt != bannedSince.end() && (banIt->second + Config_->BanTimeout) > now) {
            result.erase(it++);
        } else {
            ++it;
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

void TDiscovery::StartPolling()
{
    PeriodicExecutor_->Start();
}

void TDiscovery::StopPolling()
{
    WaitFor(PeriodicExecutor_->Stop())
        .ThrowOnError();
}

void TDiscovery::DoEnter(TString name, TAttributeDictionary attributes)
{
    YT_VERIFY(!Transaction_);

    TCreateNodeOptions createOptions;
    createOptions.IgnoreExisting = true;
    createOptions.Attributes = ConvertToAttributes(attributes);

    WaitFor(Client_->CreateNode(Config_->Directory + "/" + name, NObjectClient::EObjectType::MapNode, createOptions))
        .ThrowOnError();
        
    Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master))
        .ValueOrThrow();

    TLockNodeOptions lockOptions;
    lockOptions.ChildKey = "lock";

    auto lock = WaitFor(Transaction_->LockNode(Config_->Directory + "/" + name, NCypressClient::ELockMode::Shared, lockOptions))
        .ValueOrThrow();
    
    YT_LOG_INFO("Enter to the group (TransactionId: %v, LockId: %v)",
        Transaction_->GetId(),
        lock.LockId);
}

void TDiscovery::DoLeave()
{
    YT_VERIFY(Transaction_);

    WaitFor(Transaction_->Abort())
        .ThrowOnError();

    YT_LOG_INFO("Left the group (TransactionId: %v)", Transaction_->GetId());

    Transaction_.Reset();
}
    
void TDiscovery::UpdateList()
{
    auto list = ConvertToNode(WaitFor(Client_->ListNode(Config_->Directory, ListOptions_))
        .ValueOrThrow());
    THashMap<TString, TAttributeDictionary> newList;

    i64 aliveCount = 0;
    i64 deadCount = 0;

    for (const auto &node : list->AsList()->GetChildren()) {
        auto locks = node->Attributes().Find<IListNodePtr>("locks");
        bool isAlive = false;
        
        for (const auto& lockNode : locks->GetChildren()) {
            auto lock = lockNode->AsMap();
            auto childKey = lock->FindChild("child_key");
            if (childKey && childKey->AsString()->GetValue() == "lock") {
                isAlive = true;
                break;
            }
        }
        if (isAlive) {
            ++aliveCount;
            auto stringNode = node->AsString();
            auto attributes = stringNode->Attributes().ToMap()->GetChildren();
            newList[stringNode->GetValue()] = TAttributeDictionary(attributes.begin(), attributes.end());
        } else {
            ++deadCount;
        }
    }
    {
        TWriterGuard guard(Lock_);
        swap(List_, newList);
    }
    YT_LOG_INFO("List of participants updated (Alive: %v, Dead: %v)", aliveCount, deadCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
