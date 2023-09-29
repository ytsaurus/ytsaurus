#include "transaction_presence_cache.h"

#include "private.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NTransactionServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NObjectClient;

///////////////////////////////////////////////////////////////////////////////

namespace {

// TConcurrentHashMap doesn't support clearing.
template <class K, class V, size_t B, class L>
void ClearConcurrentHashMap(TConcurrentHashMap<K, V, B, L>* map)
{
    for (auto& bucket : map->Buckets) {
        auto guard = Guard(bucket.GetMutex());
        bucket.GetMap().clear();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTransactionPresenceCache::TTransactionPresenceCache(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , EvictionCallback_(
        BIND(&TTransactionPresenceCache::OnEviction, MakeWeak(this)))
    , DynamicConfigChangedCallback_(
        BIND(&TTransactionPresenceCache::OnDynamicConfigChanged, MakeWeak(this)))
    , AutomatonInvoker_(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::TransactionManager))
{ }

void TTransactionPresenceCache::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(!EvictionExecutor_);

    EvictionExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
        EvictionCallback_,
        GetDynamicConfig()->EvictionCheckPeriod);
    EvictionExecutor_->Start();

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);

    Started_ = true;
}

void TTransactionPresenceCache::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Started_ = false;

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

    YT_UNUSED_FUTURE(EvictionExecutor_->Stop());
    EvictionExecutor_.Reset();
}

void TTransactionPresenceCache::Clear()
{
    ClearConcurrentHashMap(&ReplicatedTransactions_);
    ClearConcurrentHashMap(&TransactionReplicationSubscriptions_);
    ReplicatedTransactionCount_ = 0;
    SubscribedRemoteTransactionReplicationCount_ = 0;
    RecentlyFinishedTransactions_.clear();
}

i64 TTransactionPresenceCache::GetReplicatedTransactionCount() const
{
    return ReplicatedTransactionCount_;
}

i64 TTransactionPresenceCache::GetRecentlyFinishedTransactionCount() const
{
    return RecentlyFinishedTransactions_.size();
}

i64 TTransactionPresenceCache::GetSubscribedRemoteTransactionReplicationCount() const
{
    return SubscribedRemoteTransactionReplicationCount_;
}

ETransactionPresence TTransactionPresenceCache::GetTransactionPresence(TTransactionId transactionId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!transactionId) {
        return ETransactionPresence::Replicated;
    }

    if (CellTagFromId(transactionId) == Bootstrap_->GetCellTag()) {
        return ETransactionPresence::Replicated;
    }

    if (TypeFromId(transactionId) == EObjectType::UploadTransaction ||
        TypeFromId(transactionId) == EObjectType::UploadNestedTransaction)
    {
        return ETransactionPresence::Replicated;
    }

    if (!Started_) {
        return ETransactionPresence::Unknown;
    }

    auto result = ETransactionPresence::None;
    ReplicatedTransactions_.Get(transactionId, result);
    return result;
}

void TTransactionPresenceCache::SetTransactionReplicated(TTransactionId transactionId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ReplicatedTransactions_.Insert(transactionId, ETransactionPresence::Replicated);
    ++ReplicatedTransactionCount_;

    // No point in notifying anybody when the cache is just being filled for the
    // first time (after loading snapshot).
    if (Started_) {
        NotifyRemoteTransactionReplicated(transactionId);
    }
}

void TTransactionPresenceCache::SetTransactionRecentlyFinished(TTransactionId transactionId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(Started_);

    auto newTransaction = false;
    auto newlyFinishedTransaction = false;

    {
        auto& bucket = ReplicatedTransactions_.GetBucketForKey(transactionId);
        auto guard = Guard(bucket.GetMutex());
        auto [it, emplaced] = bucket.GetMap().emplace(transactionId, ETransactionPresence::RecentlyFinished);
        newTransaction = emplaced;
        if (it->second != ETransactionPresence::RecentlyFinished) {
            it->second = ETransactionPresence::RecentlyFinished;
            newlyFinishedTransaction = true;
        }
    }

    if (newTransaction) {
        ++ReplicatedTransactionCount_;
    }

    if (newTransaction || newlyFinishedTransaction) {
        RecentlyFinishedTransactions_.emplace(NProfiling::GetInstant(), transactionId);
    }

    NotifyRemoteTransactionReplicated(transactionId);
}

void TTransactionPresenceCache::OnEviction()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!Started_) {
        return;
    }

    const auto maxEvictionCount = GetDynamicConfig()->MaxEvictedTransactionsPerCheck;
    const auto evictionDelay = GetDynamicConfig()->FinishedTransactionEvictionDelay;

    auto now = NProfiling::GetInstant();

    auto it = RecentlyFinishedTransactions_.begin();
    auto ite = RecentlyFinishedTransactions_.end();
    for (auto i = 0; i < maxEvictionCount && it != ite; ++i, ++it) {
        auto [finishTime, transactionId] = *it;
        if (finishTime + evictionDelay >= now) {
            break;
        }

        {
            auto& bucket = ReplicatedTransactions_.GetBucketForKey(transactionId);
            auto guard = Guard(bucket.GetMutex());
            bucket.GetMap().erase(transactionId);
        }

        --ReplicatedTransactionCount_;
    }

    RecentlyFinishedTransactions_.erase(RecentlyFinishedTransactions_.begin(), it);
}

const TTransactionPresenceCacheConfigPtr& TTransactionPresenceCache::GetDynamicConfig()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->TransactionManager->TransactionPresenceCache;
}

void TTransactionPresenceCache::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    EvictionExecutor_->SetPeriod(GetDynamicConfig()->EvictionCheckPeriod);
}

TFuture<void> TTransactionPresenceCache::SubscribeRemoteTransactionReplicated(TTransactionId transactionId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    switch (GetTransactionPresence(transactionId)) {
        case ETransactionPresence::None:
            break;

        case ETransactionPresence::Unknown:
            return {};

        case ETransactionPresence::Replicated:
        case ETransactionPresence::RecentlyFinished:
            return VoidFuture;

        default:
            YT_ABORT();
    }

    auto& bucket = TransactionReplicationSubscriptions_.GetBucketForKey(transactionId);
    TFuture<void> result;
    {
        auto guard = Guard(bucket.GetMutex());
        auto& bucketMap = bucket.GetMap();
        auto it = bucketMap.find(transactionId);

        TPromise<void>* subscriptionPromise = nullptr;
        if (it == bucketMap.end()) {
            subscriptionPromise = &bucketMap[transactionId];
            *subscriptionPromise = NewPromise<void>();
            ++SubscribedRemoteTransactionReplicationCount_;
        } else {
            subscriptionPromise = &it->second;
        }

        result = subscriptionPromise->ToFuture();
    }

    // Recheck to avoid races with transactions that became replicated while we were subscribing.
    switch (GetTransactionPresence(transactionId)) {
        case ETransactionPresence::None:
            break;

        case ETransactionPresence::Unknown:
            return {};

        case ETransactionPresence::Replicated:
        case ETransactionPresence::RecentlyFinished:
            NotifyRemoteTransactionReplicated(transactionId);
            break;

        default:
            YT_ABORT();
    }

    return result;
}

void TTransactionPresenceCache::NotifyRemoteTransactionReplicated(TTransactionId transactionId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TPromise<void> promise;
    auto& bucket = TransactionReplicationSubscriptions_.GetBucketForKey(transactionId);
    {
        auto guard = Guard(bucket.GetMutex());
        auto& bucketMap = bucket.GetMap();
        auto it = bucketMap.find(transactionId);
        if (it != bucketMap.end()) {
            promise = std::move(it->second);
            bucketMap.erase(it);
        }
    }

    if (promise) {
        --SubscribedRemoteTransactionReplicationCount_;

        // Makes sure the futures aren't set from mutation context (which is dangerous).
        AutomatonInvoker_->Invoke(BIND([promise = std::move(promise)] () {
            promise.TrySet();
        }));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
