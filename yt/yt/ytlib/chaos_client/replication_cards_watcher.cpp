#include "replication_cards_watcher.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NChaosClient {

using namespace NConcurrency;
using namespace NThreading;
using namespace NObjectClient;
using namespace NTransactionClient;

using TCtxReplicationCardWatchPtr = IReplicationCardsWatcher::TCtxReplicationCardWatchPtr;

////////////////////////////////////////////////////////////////////////////////

static const auto Logger  = NLogging::TLogger("CardWatcher");

////////////////////////////////////////////////////////////////////////////////

struct TReplicationCardWatcher
{
    TCtxReplicationCardWatchPtr Context;
    TInstant RequestStart;
};

////////////////////////////////////////////////////////////////////////////////

struct TReplicationCardWatchersList
{
    TReplicationCardWatchersList(
        TTimestamp currentCacheTimestamp,
        TReplicationCardPtr replicationCard)
        : CurrentCacheTimestamp(currentCacheTimestamp)
        , ReplicationCard(std::move(replicationCard))
    { }

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
    TTimestamp CurrentCacheTimestamp = NullTimestamp;
    TReplicationCardPtr ReplicationCard;
    std::vector<TReplicationCardWatcher> Watchers;
    std::atomic<TInstant> LastSeenWatchers;
};

////////////////////////////////////////////////////////////////////////////////

void ReplyWithCard(
    const TReplicationCardPtr& replicationCard,
    TTimestamp timestamp,
    const TCtxReplicationCardWatchPtr& context)
{
    auto* response = context->Response().mutable_replication_card_changed();
    response->set_replication_card_cache_timestamp(timestamp);
    ToProto(
        response->mutable_replication_card(),
        *replicationCard,
        MinimalFetchOptions);
    context->Reply();
}

void ReplyMigrated(const TCellId& destination, const TCtxReplicationCardWatchPtr& context)
{
    auto& response = context->Response();
    auto* migrateToCellId = response.mutable_replication_card_migrated()->mutable_migrate_to_cell_id();
    ToProto(migrateToCellId, destination);
    context->Reply();
}

void ReplyDeleted(const TCtxReplicationCardWatchPtr& context)
{
    auto& response = context->Response();
    response.mutable_replication_card_deleted();
    context->Reply();
}

void ReplyInstanceIsNotLeader(const TCtxReplicationCardWatchPtr& context)
{
    auto& response = context->Response();
    response.mutable_instance_is_not_leader();
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardsWatcher
    : public IReplicationCardsWatcher
{
public:
    TReplicationCardsWatcher(
        TDuration expirationTime,
        TDuration goneCardsExpirationTime,
        TDuration expirationSweepPeriod,
        IInvokerPtr invoker)
        : ExpirationExecutor_(New<TPeriodicExecutor>(
            invoker,
            BIND(&TReplicationCardsWatcher::OnExpirationSweep, MakeWeak(this)),
            expirationSweepPeriod))
        , ExpirationTime_(expirationTime)
        , GoneCardsExpirationTime_(goneCardsExpirationTime)
    { }

    void Start(const std::vector<std::pair<TReplicationCardId, TReplicationCardPtr>>& replicationCards) override
    {
        auto writeGuard = WriterGuard(EntriesLock_);
        WatchersByCardId_.reserve(replicationCards.size());
        for (const auto& [id, replicationCard] : replicationCards) {
            WatchersByCardId_.emplace(
                id,
                std::make_unique<TReplicationCardWatchersList>(
                    replicationCard->CurrentTimestamp,
                    replicationCard));
        }

        IsRunning_.store(true);
        writeGuard.Release();
        ExpirationExecutor_->Start();
    }

    void Stop() override
    {
        auto writeGuard = WriterGuard(EntriesLock_);
        IsRunning_.store(false);
        for (const auto& [id, watchersList] : WatchersByCardId_) {
            for (const auto& watcher : watchersList->Watchers) {
                ReplyInstanceIsNotLeader(watcher.Context);
            }
        }

        WatchersByCardId_.clear();
        writeGuard.Release();
        WaitFor(ExpirationExecutor_->Stop()).ThrowOnError();
    }

    void RegisterReplicationCard(
        const TReplicationCardId& replicationCardId,
        const TReplicationCardPtr& replicationCard,
        TTimestamp timestamp) override
    {
        {
            auto writeGuard = WriterGuard(EntriesLock_);
            EmplaceOrCrash(
                WatchersByCardId_,
                replicationCardId,
                std::make_unique<TReplicationCardWatchersList>(
                    timestamp,
                    replicationCard));
        }

        auto migratedCardsGuard = WriterGuard(MigratedCardsLock_);
        MigratedCards_.erase(replicationCardId);
    }

    void OnReplcationCardUpdated(
        const TReplicationCardId& replicationCardId,
        const TReplicationCardPtr& replicationCard,
        TTimestamp timestamp) override
    {
        YT_LOG_DEBUG("Replication card updated in watcher (ReplicationCardId: %v, Timestamp: %v)",
                replicationCardId,
                timestamp);

        auto readGuard = ReaderGuard(EntriesLock_);
        auto it = WatchersByCardId_.find(replicationCardId);
        if (it == WatchersByCardId_.end()) {
            YT_LOG_WARNING("Replication card was not registered in watcher for update (ReplicationCardId: %v)",
                replicationCardId);
            return;
        }

        std::vector<TReplicationCardWatcher> watchers;
        {
            auto& entry = it->second;
            auto entryGuard = Guard(entry->Lock);
            watchers.swap(entry->Watchers);
            entry->CurrentCacheTimestamp = timestamp;
            entry->ReplicationCard = replicationCard;
            if (!watchers.empty()) {
                entry->LastSeenWatchers.store(TInstant::Now());
            }
        }

        readGuard.Release();

        for (auto& watcher : watchers) {
            ReplyWithCard(replicationCard, timestamp, watcher.Context);
        }
    }

    void OnReplicationCardRemoved(const TReplicationCardId& replicationCardId) override
    {
        YT_LOG_DEBUG("Replication card removed from watcher (ReplicationCardId: %v)",
                replicationCardId);

        {
            TInstant now = TInstant::Now();
            auto deletedCardsGuard = WriterGuard(DeletedCardsLock_);
            DeletedCards_.emplace(replicationCardId, now);
        }

        std::vector<TReplicationCardWatcher> watchers;
        {
            auto writeGuard = WriterGuard(EntriesLock_);
            auto it = WatchersByCardId_.find(replicationCardId);
            if (it == WatchersByCardId_.end()) {
                writeGuard.Release();
                YT_LOG_DEBUG("Replication card was not registered for remove in watcher (ReplicationCardId: %v)",
                    replicationCardId);
                return;
            }

            auto& entry = it->second;
            watchers = std::move(entry->Watchers);
            WatchersByCardId_.erase(it);
        }

        for (auto& watcher : watchers) {
            auto& response = watcher.Context->Response();
            response.mutable_replication_card_deleted();
            watcher.Context->Reply();
        }
    }

    void OnReplicationCardMigrated(const std::vector<std::pair<TReplicationCardId, TCellId>>& replicationCardIds) override
    {
        YT_LOG_DEBUG("Replication cards migrated: start notifying watching clients");

        struct TReplicationCardMigrationDescriptor {
            TReplicationCardId ReplicationCardId;
            TCellId DestinationCellId;
            std::vector<TReplicationCardWatcher> WatchersList;
        };

        {
            TInstant now = TInstant::Now();
            auto migratedCardsGuard = WriterGuard(MigratedCardsLock_);
            for (const auto& [replicationCardId, cellId] : replicationCardIds) {
                MigratedCards_.emplace(
                    replicationCardId,
                    TMigratedReplicationCardEntry{
                        .Destination = cellId,
                        .When = now
                    });
            }
        }

        std::vector<TReplicationCardMigrationDescriptor> replicationCardId2Watchers;
        replicationCardId2Watchers.reserve(replicationCardIds.size());

        {
            auto writeGuard = WriterGuard(EntriesLock_);
            for (const auto& [replicationCardId, cellId] : replicationCardIds) {
                auto it = WatchersByCardId_.find(replicationCardId);
                if (it == WatchersByCardId_.end()) {
                    YT_LOG_WARNING("Replication card was not registered for migration in watcher (ReplicationCardId: %v)",
                        replicationCardId);
                    continue;
                }

                auto& entry = it->second;
                replicationCardId2Watchers.emplace_back(replicationCardId, cellId, std::move(entry->Watchers));
                WatchersByCardId_.erase(it);
            }
        }

        for (const auto& descriptor : replicationCardId2Watchers) {
            for (auto& watcher : descriptor.WatchersList) {
                ReplyMigrated(descriptor.DestinationCellId, watcher.Context);
            }
        }
    }

    EReplicationCardWatherState WatchReplicationCard(
        const TReplicationCardId& replicationCardId,
        TTimestamp cacheTimestamp,
        TCtxReplicationCardWatchPtr context,
        bool allowUnregistered) override
    {
        if (!IsRunning_.load()) {
            ReplyInstanceIsNotLeader(context);
            return EReplicationCardWatherState::Normal;
        }

        {
            auto migratedCardsGuard = ReaderGuard(MigratedCardsLock_);
            if (auto it = MigratedCards_.find(replicationCardId); it != MigratedCards_.end()) {
                auto destination = it->second.Destination;
                migratedCardsGuard.Release();
                YT_LOG_DEBUG("Replication card was already migrated (ReplicationCardId: %v)",
                    replicationCardId);
                ReplyMigrated(destination, context);
                return EReplicationCardWatherState::Migrated;
            }
        }

        {
            auto deletedCardsGuard = ReaderGuard(DeletedCardsLock_);
            if (DeletedCards_.contains(replicationCardId)) {
                deletedCardsGuard.Release();
                YT_LOG_DEBUG("Replication card was already deleted (ReplicationCardId: %v)",
                    replicationCardId);
                ReplyDeleted(context);
                return EReplicationCardWatherState::Deleted;
            }
        }

        {
            auto readGuard = ReaderGuard(EntriesLock_);
            auto it = WatchersByCardId_.find(replicationCardId);
            if (it != WatchersByCardId_.end()) {
                auto& entry = it->second;
                entry->LastSeenWatchers.store(TInstant::Now());
                if (entry->CurrentCacheTimestamp > cacheTimestamp) {
                    auto replicationCard = entry->ReplicationCard;
                    auto timestamp = entry->CurrentCacheTimestamp;
                    readGuard.Release();
                    YT_LOG_DEBUG(
                        "Replication card updated between watches "
                        "(ReplicationCardId: %v, CurrentCacheTimestamp: %v, CacheTimestamp: %v)",
                        replicationCardId, timestamp, cacheTimestamp);
                    ReplyWithCard(replicationCard, timestamp, context);
                    return EReplicationCardWatherState::Normal;
                }

                auto entryGuard = Guard(entry->Lock);
                entry->Watchers.push_back(TReplicationCardWatcher{
                    .Context = std::move(context),
                    .RequestStart = TInstant::Now()
                });

                YT_LOG_DEBUG("Added request to watchers list (ReplicationCardId: %v)",
                    replicationCardId);

                return EReplicationCardWatherState::Normal;
            }
        }

        if (allowUnregistered) {
            auto writeGuard = WriterGuard(EntriesLock_);
            auto& entry = WatchersByCardId_[replicationCardId];
            if (entry == nullptr) {
                entry = std::make_unique<TReplicationCardWatchersList>(
                    NullTimestamp,
                    nullptr);
            }

            entry->LastSeenWatchers.store(TInstant::Now());
            if (entry->CurrentCacheTimestamp > cacheTimestamp) {
                auto replicationCard = entry->ReplicationCard;
                auto timestamp = entry->CurrentCacheTimestamp;
                writeGuard.Release();
                ReplyWithCard(replicationCard, timestamp, context);
                return EReplicationCardWatherState::Normal;
            }

            auto entryGuard = Guard(entry->Lock);
            entry->Watchers.push_back(TReplicationCardWatcher{
                .Context = std::move(context),
                .RequestStart = TInstant::Now()
            });
            return EReplicationCardWatherState::Normal;
        }

        YT_LOG_WARNING("Replication card was not registered for update in watcher (ReplicationCardId: %v)",
            replicationCardId);
        auto& response = context->Response();
        response.mutable_unknown_replication_card();
        context->Reply();
        return EReplicationCardWatherState::Unknown;
    }

    bool TryUnregisterReplicationCard(const TReplicationCardId& replicationCardId) override
    {
        auto writeGuard = WriterGuard(EntriesLock_);
        auto it = WatchersByCardId_.find(replicationCardId);
        if (it == WatchersByCardId_.end()) {
            return true;
        }

        auto& entry = it->second;
        if (!entry->Watchers.empty()) {
            entry->LastSeenWatchers.store(TInstant::Now());
            return false;
        }

        WatchersByCardId_.erase(it);
        return true;
    }

    TInstant GetLastSeenWatchers(const TReplicationCardId& replicationCardId) override
    {
        auto readGuard = ReaderGuard(EntriesLock_);
        auto it = WatchersByCardId_.find(replicationCardId);
        if (it == WatchersByCardId_.end()) {
            return TInstant::Zero();
        }
        return it->second->LastSeenWatchers.load();
    }

private:
    const TPeriodicExecutorPtr ExpirationExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, EntriesLock_);
    THashMap<TReplicationCardId, std::unique_ptr<TReplicationCardWatchersList>> WatchersByCardId_;

    struct TMigratedReplicationCardEntry
    {
        TCellId Destination;
        TInstant When;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, MigratedCardsLock_);
    THashMap<TReplicationCardId, TMigratedReplicationCardEntry> MigratedCards_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, DeletedCardsLock_);
    THashMap<TReplicationCardId, TInstant> DeletedCards_;

    std::atomic<TDuration> ExpirationTime_;
    std::atomic<TDuration> GoneCardsExpirationTime_;
    std::atomic_bool IsRunning_ = false;

    void OnExpirationSweep()
    {
        YT_LOG_DEBUG("Started expired watchers sweep");

        std::vector<TReplicationCardWatcher> expiredWatchers;
        {
            TInstant deadLine = TInstant::Now() - ExpirationTime_.load();
            auto readGuard = ReaderGuard(EntriesLock_);
            for (auto& [replicationCardId, entry]: WatchersByCardId_) {
                auto entryGuard = Guard(entry->Lock);
                if (!entry->Watchers.empty()) {
                    entry->LastSeenWatchers.store(TInstant::Now());
                }

                auto expiredWatchersIterator = entry->Watchers.begin();
                while (expiredWatchersIterator != entry->Watchers.end()) {
                    if (expiredWatchersIterator->RequestStart > deadLine) {
                        break;
                    }

                    expiredWatchers.push_back(std::move(*expiredWatchersIterator));
                    ++expiredWatchersIterator;
                }

                entry->Watchers.erase(entry->Watchers.begin(), expiredWatchersIterator);
            }
        }

        for (auto& expiredWatcher : expiredWatchers) {
            auto& response = expiredWatcher.Context->Response();
            response.mutable_replication_card_not_changed();
            expiredWatcher.Context->Reply();
        }

        TInstant goneCardsDeadLine = TInstant::Now() - GoneCardsExpirationTime_.load();
        std::vector<TReplicationCardId> idsToRemove;
        {
            auto migratedCardsGuard = ReaderGuard(MigratedCardsLock_);
            for (auto it = MigratedCards_.begin(); it != MigratedCards_.end(); ++it) {
                if (it->second.When < goneCardsDeadLine) {
                    idsToRemove.push_back(it->first);
                }
            }
        }

        if (!idsToRemove.empty()) {
            auto migratedCardsGuard = WriterGuard(MigratedCardsLock_);
            for (const auto& replicationCardId : idsToRemove) {
                MigratedCards_.erase(replicationCardId);
            }
        }

        idsToRemove.clear();

        {
            auto deletedCardsGuard = ReaderGuard(DeletedCardsLock_);
            for (auto it = DeletedCards_.begin(); it != DeletedCards_.end(); ++it) {
                if (it->second < goneCardsDeadLine) {
                    idsToRemove.push_back(it->first);
                }
            }
        }

        if (!idsToRemove.empty()) {
            auto deletedCardsGuard = WriterGuard(DeletedCardsLock_);
            for (const auto& replicationCardId : idsToRemove) {
                DeletedCards_.erase(replicationCardId);
            }
        }

        idsToRemove.clear();

        YT_LOG_DEBUG("Finished expired watchers sweep");
    }

};

IReplicationCardsWatcherPtr CreateReplicationCardsWatcher(
    const TReplicationCardsWatcherConfigPtr& config,
    IInvokerPtr invoker)
{
    return New<TReplicationCardsWatcher>(
        config->PollExpirationTime,
        config->GoneCardsExpirationTime,
        config->ExpirationSweepPeriod,
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
