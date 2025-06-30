#include "replication_cards_watcher.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NChaosClient {

using namespace NConcurrency;
using namespace NThreading;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ReplicationCardWatcherLogger;

////////////////////////////////////////////////////////////////////////////////

struct TReplicationCardWatcherEntry
{
    IReplicationCardWatcherCallbacksPtr Callbacks;
    TInstant RequestStartTime;
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
    std::vector<TReplicationCardWatcherEntry> WatcherEntries;
    std::atomic<TInstant> LastSeenWatchers;
};

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardsWatcher
    : public IReplicationCardsWatcher
{
public:
    TReplicationCardsWatcher(
        TReplicationCardsWatcherConfigPtr config,
        IInvokerPtr invoker)
        : ExpirationExecutor_(New<TPeriodicExecutor>(
            invoker,
            BIND(&TReplicationCardsWatcher::OnExpirationSweep, MakeWeak(this)),
            config->ExpirationSweepPeriod))
        , ExpirationTime_(config->PollExpirationTime)
        , GoneCardsExpirationTime_(config->GoneCardsExpirationTime)
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
        auto aliveWatchers = THashMap<TReplicationCardId, std::unique_ptr<TReplicationCardWatchersList>>();
        {
            auto writeGuard = WriterGuard(EntriesLock_);
            IsRunning_.store(false);
            aliveWatchers = std::move(WatchersByCardId_);
            WatchersByCardId_.clear();
        }

        for (const auto& [id, watchersList] : aliveWatchers) {
            for (const auto& watcher : watchersList->WatcherEntries) {
                watcher.Callbacks->OnInstanceIsNotLeader();
            }
        }

        auto stopResult = WaitFor(ExpirationExecutor_->Stop());
        if (!stopResult.IsOK()) {
            YT_LOG_WARNING(stopResult, "Failed to stop expiration executor");
        }
    }

    void RegisterReplicationCard(
        TReplicationCardId replicationCardId,
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

        {
            auto deletedCardsGuard = WriterGuard(DeletedCardsLock_);
            DeletedCards_.erase(replicationCardId);
        }

        {
            auto migratedCardsGuard = WriterGuard(MigratedCardsLock_);
            MigratedCards_.erase(replicationCardId);
        }
    }

    void OnReplcationCardUpdated(
        TReplicationCardId replicationCardId,
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

        std::vector<TReplicationCardWatcherEntry> watcherEntries;
        {
            auto& entry = it->second;
            auto entryGuard = Guard(entry->Lock);
            watcherEntries.swap(entry->WatcherEntries);
            entry->CurrentCacheTimestamp = timestamp;
            entry->ReplicationCard = replicationCard;
            if (!watcherEntries.empty()) {
                entry->LastSeenWatchers.store(TInstant::Now());
            }
        }

        readGuard.Release();

        for (const auto& watcher : watcherEntries) {
            watcher.Callbacks->OnReplicationCardChanged(replicationCard, timestamp);
        }
    }

    void OnReplicationCardRemoved(TReplicationCardId replicationCardId) override
    {
        YT_LOG_DEBUG("Replication card removed from watcher (ReplicationCardId: %v)",
                replicationCardId);

        {
            TInstant now = TInstant::Now();
            auto deletedCardsGuard = WriterGuard(DeletedCardsLock_);
            DeletedCards_.emplace(replicationCardId, now);
        }

        std::vector<TReplicationCardWatcherEntry> watcherEntries;
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
            watcherEntries = std::move(entry->WatcherEntries);
            WatchersByCardId_.erase(it);
        }

        for (const auto& watcher : watcherEntries) {
            watcher.Callbacks->OnReplicationCardDeleted();
        }
    }

    void OnReplicationCardMigrated(const std::vector<std::pair<TReplicationCardId, TCellId>>& replicationCardIds) override
    {
        YT_LOG_DEBUG("Replication cards migrated: start notifying watching clients");

        struct TReplicationCardMigrationDescriptor {
            TReplicationCardId ReplicationCardId;
            TCellId DestinationCellId;
            std::vector<TReplicationCardWatcherEntry> WatcherEntries;
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
                replicationCardId2Watchers.emplace_back(replicationCardId, cellId, std::move(entry->WatcherEntries));
                WatchersByCardId_.erase(it);
            }
        }

        for (const auto& descriptor : replicationCardId2Watchers) {
            for (const auto& watcher : descriptor.WatcherEntries) {
                watcher.Callbacks->OnReplicationCardMigrated(descriptor.DestinationCellId);
            }
        }
    }

    EReplicationCardWatherState WatchReplicationCard(
        TReplicationCardId replicationCardId,
        TTimestamp cacheTimestamp,
        IReplicationCardWatcherCallbacksPtr callbacks,
        bool allowUnregistered) override
    {
        if (!IsRunning_.load()) {
            callbacks->OnInstanceIsNotLeader();
            return EReplicationCardWatherState::Normal;
        }

        {
            auto migratedCardsGuard = ReaderGuard(MigratedCardsLock_);
            if (auto it = MigratedCards_.find(replicationCardId); it != MigratedCards_.end()) {
                auto destination = it->second.Destination;
                migratedCardsGuard.Release();
                YT_LOG_DEBUG("Replication card was already migrated (ReplicationCardId: %v)",
                    replicationCardId);
                callbacks->OnReplicationCardMigrated(destination);
                return EReplicationCardWatherState::Migrated;
            }
        }

        {
            auto deletedCardsGuard = ReaderGuard(DeletedCardsLock_);
            if (DeletedCards_.contains(replicationCardId)) {
                deletedCardsGuard.Release();
                YT_LOG_DEBUG("Replication card was already deleted (ReplicationCardId: %v)",
                    replicationCardId);
                callbacks->OnReplicationCardDeleted();
                return EReplicationCardWatherState::Deleted;
            }
        }

        {
            auto readGuard = ReaderGuard(EntriesLock_);
            auto it = WatchersByCardId_.find(replicationCardId);
            if (it != WatchersByCardId_.end()) {
                auto& entry = it->second;
                entry->LastSeenWatchers.store(TInstant::Now());
                auto entryGuard = Guard(entry->Lock);
                if (entry->CurrentCacheTimestamp > cacheTimestamp) {
                    auto replicationCard = entry->ReplicationCard;
                    auto timestamp = entry->CurrentCacheTimestamp;
                    entryGuard.Release();
                    readGuard.Release();

                    YT_LOG_DEBUG(
                        "Replication card updated between watches "
                        "(ReplicationCardId: %v, CurrentCacheTimestamp: %v, CacheTimestamp: %v)",
                        replicationCardId, timestamp, cacheTimestamp);

                    callbacks->OnReplicationCardChanged(replicationCard, timestamp);
                    return EReplicationCardWatherState::Normal;
                }

                entry->WatcherEntries.push_back(TReplicationCardWatcherEntry{
                    .Callbacks = std::move(callbacks),
                    .RequestStartTime = TInstant::Now(),
                });

                entryGuard.Release();
                readGuard.Release();

                YT_LOG_DEBUG("Added request to watchers list (ReplicationCardId: %v)",
                    replicationCardId);

                return EReplicationCardWatherState::Normal;
            }
        }

        if (allowUnregistered) {
            auto writeGuard = WriterGuard(EntriesLock_);
            auto& entry = WatchersByCardId_[replicationCardId];
            if (!entry) {
                entry = std::make_unique<TReplicationCardWatchersList>(
                    NullTimestamp,
                    nullptr);
            }

            entry->LastSeenWatchers.store(TInstant::Now());
            if (entry->CurrentCacheTimestamp > cacheTimestamp) {
                auto replicationCard = entry->ReplicationCard;
                auto timestamp = entry->CurrentCacheTimestamp;
                writeGuard.Release();
                callbacks->OnReplicationCardChanged(replicationCard, timestamp);
                return EReplicationCardWatherState::Normal;
            }

            auto entryGuard = Guard(entry->Lock);
            entry->WatcherEntries.push_back(TReplicationCardWatcherEntry{
                .Callbacks = std::move(callbacks),
                .RequestStartTime = TInstant::Now(),
            });
            return EReplicationCardWatherState::Normal;
        }

        YT_LOG_WARNING("Replication card was not registered for update in watcher (ReplicationCardId: %v)",
            replicationCardId);
        callbacks->OnUnknownReplicationCard();
        return EReplicationCardWatherState::Unknown;
    }

    bool TryUnregisterReplicationCard(TReplicationCardId replicationCardId) override
    {
        auto writeGuard = WriterGuard(EntriesLock_);
        auto it = WatchersByCardId_.find(replicationCardId);
        if (it == WatchersByCardId_.end()) {
            return true;
        }

        auto& entry = it->second;
        if (!entry->WatcherEntries.empty()) {
            entry->LastSeenWatchers.store(TInstant::Now());
            return false;
        }

        WatchersByCardId_.erase(it);
        return true;
    }

    TInstant GetLastSeenWatchers(TReplicationCardId replicationCardId) override
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
    std::atomic<bool> IsRunning_ = false;

    void OnExpirationSweep()
    {
        YT_LOG_DEBUG("Started expired watchers sweep");

        std::vector<TReplicationCardWatcherEntry> expiredWatcherEntries;
        {
            auto deadLine = TInstant::Now() - ExpirationTime_.load();
            auto readGuard = ReaderGuard(EntriesLock_);
            for (auto& [replicationCardId, entry]: WatchersByCardId_) {
                auto entryGuard = Guard(entry->Lock);
                if (!entry->WatcherEntries.empty()) {
                    entry->LastSeenWatchers.store(TInstant::Now());
                }

                auto expiredWatchersIterator = entry->WatcherEntries.begin();
                while (expiredWatchersIterator != entry->WatcherEntries.end()) {
                    if (expiredWatchersIterator->RequestStartTime > deadLine) {
                        break;
                    }

                    expiredWatcherEntries.push_back(std::move(*expiredWatchersIterator));
                    ++expiredWatchersIterator;
                }

                entry->WatcherEntries.erase(entry->WatcherEntries.begin(), expiredWatchersIterator);
            }
        }

        for (const auto& expiredWatcherEntry : expiredWatcherEntries) {
            expiredWatcherEntry.Callbacks->OnNothingChanged();
        }

        auto goneCardsDeadLine = TInstant::Now() - GoneCardsExpirationTime_.load();
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
            for (auto replicationCardId : idsToRemove) {
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
            for (auto replicationCardId : idsToRemove) {
                DeletedCards_.erase(replicationCardId);
            }
        }

        idsToRemove.clear();

        YT_LOG_DEBUG("Finished expired watchers sweep");
    }
};

IReplicationCardsWatcherPtr CreateReplicationCardsWatcher(
    TReplicationCardsWatcherConfigPtr config,
    IInvokerPtr invoker)
{
    return New<TReplicationCardsWatcher>(
        std::move(config),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
