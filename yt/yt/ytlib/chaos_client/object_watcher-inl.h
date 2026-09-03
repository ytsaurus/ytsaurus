#ifndef OBJECT_WATCHER_INL_H_
#error "Direct inclusion of this file is not allowed, include object_watcher.h"
// For the sake of sane code completion.
#include "object_watcher.h"
#endif

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

template <class TObjectPtr, class TWatcherInterface>
TObjectWatcher<TObjectPtr, TWatcherInterface>::TWatchersList::TWatchersList(
    NTransactionClient::TTimestamp currentCacheTimestamp,
    TObjectPtr object)
    : CurrentCacheTimestamp(currentCacheTimestamp)
    , Object(std::move(object))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class TObjectPtr, class TWatcherInterface>
TObjectWatcher<TObjectPtr, TWatcherInterface>::TObjectWatcher(
    IInvokerPtr invoker,
    TDuration expirationSweepPeriod,
    TDuration pollExpirationTime,
    TDuration goneObjectsExpirationTime,
    NLogging::TLogger logger,
    std::string objectName)
    : ExpirationExecutor_(New<NConcurrency::TPeriodicExecutor>(
        std::move(invoker),
        BIND(&TObjectWatcher::OnExpirationSweep, MakeWeak(this)),
        expirationSweepPeriod))
    , ExpirationTime_(pollExpirationTime)
    , GoneObjectsExpirationTime_(goneObjectsExpirationTime)
    , Logger(logger.WithTag("ObjectType", objectName))
{ }

template <class TObjectPtr, class TWatcherInterface>
void TObjectWatcher<TObjectPtr, TWatcherInterface>::Start(
    const std::vector<TSnapshot>& objects)
{
    auto writeGuard = WriterGuard(EntriesLock_);
    WatchersByObjectId_.reserve(objects.size());
    for (const auto& object : objects) {
        WatchersByObjectId_.emplace(
            object.ObjectId,
            std::make_unique<TWatchersList>(
                object.CacheTimestamp,
                object.Object));
    }

    IsRunning_.store(true);
    writeGuard.Release();
    ExpirationExecutor_->Start();
}

template <class TObjectPtr, class TWatcherInterface>
void TObjectWatcher<TObjectPtr, TWatcherInterface>::Stop()
{
    THashMap<TChaosObjectId, std::unique_ptr<TWatchersList>> aliveWatchers;
    {
        auto writeGuard = WriterGuard(EntriesLock_);
        IsRunning_.store(false);
        aliveWatchers = std::move(WatchersByObjectId_);
        WatchersByObjectId_.clear();
    }

    for (const auto& [objectId, watchersList] : aliveWatchers) {
        for (const auto& watcher : watchersList->WatcherEntries) {
            watcher.Callbacks->OnInstanceIsNotLeader();
        }
    }

    auto stopResult = NConcurrency::WaitFor(ExpirationExecutor_->Stop());
    if (!stopResult.IsOK()) {
        YT_TLOG_WARNING("Failed to stop object watcher expiration executor")
            .With(stopResult);
    }
}

template <class TObjectPtr, class TWatcherInterface>
void TObjectWatcher<TObjectPtr, TWatcherInterface>::RegisterObject(
    TChaosObjectId objectId,
    const TObjectPtr& object,
    NTransactionClient::TTimestamp timestamp)
{
    {
        auto writeGuard = WriterGuard(EntriesLock_);
        EmplaceOrCrash(
            WatchersByObjectId_,
            objectId,
            std::make_unique<TWatchersList>(timestamp, object));
    }

    {
        auto deletedObjectsGuard = WriterGuard(DeletedObjectsLock_);
        DeletedObjects_.erase(objectId);
    }

    {
        auto migratedObjectsGuard = WriterGuard(MigratedObjectsLock_);
        MigratedObjects_.erase(objectId);
    }
}

template <class TObjectPtr, class TWatcherInterface>
void TObjectWatcher<TObjectPtr, TWatcherInterface>::OnObjectUpdated(
    TChaosObjectId objectId,
    const TObjectPtr& object,
    NTransactionClient::TTimestamp timestamp)
{
    YT_TLOG_DEBUG("Object updated in watcher")
        .With("ObjectId", objectId)
        .With("Timestamp", timestamp);

    auto readGuard = ReaderGuard(EntriesLock_);
    auto it = WatchersByObjectId_.find(objectId);
    if (it == WatchersByObjectId_.end()) {
        YT_TLOG_WARNING("Object was not registered in watcher for update")
            .With("ObjectId", objectId);
        return;
    }

    std::vector<TWatcherEntry> watcherEntries;
    {
        auto& entry = it->second;
        auto entryGuard = Guard(entry->Lock);
        watcherEntries.swap(entry->WatcherEntries);
        entry->CurrentCacheTimestamp = timestamp;
        entry->Object = object;
        if (!watcherEntries.empty()) {
            entry->LastSeenWatchers.store(TInstant::Now());
        }
    }

    readGuard.Release();

    for (const auto& watcher : watcherEntries) {
        watcher.Callbacks->OnObjectChanged(object, timestamp);
    }
}

template <class TObjectPtr, class TWatcherInterface>
void TObjectWatcher<TObjectPtr, TWatcherInterface>::OnObjectRemoved(TChaosObjectId objectId)
{
    YT_TLOG_DEBUG("Object removed from watcher")
        .With("ObjectId", objectId);

    {
        auto deletedObjectsGuard = WriterGuard(DeletedObjectsLock_);
        DeletedObjects_.emplace(objectId, TInstant::Now());
    }

    std::vector<TWatcherEntry> watcherEntries;
    {
        auto writeGuard = WriterGuard(EntriesLock_);
        auto it = WatchersByObjectId_.find(objectId);
        if (it == WatchersByObjectId_.end()) {
            writeGuard.Release();
            YT_TLOG_DEBUG("Object was not registered for remove in watcher")
                .With("ObjectId", objectId);
            return;
        }

        auto& entry = it->second;
        watcherEntries = std::move(entry->WatcherEntries);
        WatchersByObjectId_.erase(it);
    }

    for (const auto& watcher : watcherEntries) {
        watcher.Callbacks->OnObjectDeleted();
    }
}

template <class TObjectPtr, class TWatcherInterface>
void TObjectWatcher<TObjectPtr, TWatcherInterface>::OnObjectsMigrated(
    const std::vector<std::pair<TChaosObjectId, NObjectClient::TCellId>>& objectIds)
{
    YT_TLOG_DEBUG("Objects migrated: start notifying watching clients");

    {
        auto migratedObjectsGuard = WriterGuard(MigratedObjectsLock_);
        auto now = TInstant::Now();
        for (const auto& [objectId, cellId] : objectIds) {
            MigratedObjects_.emplace(
                objectId,
                TMigratedObjectEntry{
                    .Destination = cellId,
                    .When = now,
                });
        }
    }

    std::vector<TMigrationDescriptor> objectIdToWatchers;
    objectIdToWatchers.reserve(objectIds.size());

    {
        auto writeGuard = WriterGuard(EntriesLock_);
        for (const auto& [objectId, cellId] : objectIds) {
            auto it = WatchersByObjectId_.find(objectId);
            if (it == WatchersByObjectId_.end()) {
                YT_TLOG_WARNING("Object was not registered for migration in watcher")
                    .With("ObjectId", objectId);
                continue;
            }

            auto& entry = it->second;
            objectIdToWatchers.push_back(TMigrationDescriptor{
                .ObjectId = objectId,
                .DestinationCellId = cellId,
                .WatcherEntries = std::move(entry->WatcherEntries),
            });
            WatchersByObjectId_.erase(it);
        }
    }

    for (const auto& descriptor : objectIdToWatchers) {
        for (const auto& watcher : descriptor.WatcherEntries) {
            watcher.Callbacks->OnObjectMigrated(descriptor.DestinationCellId);
        }
    }
}

template <class TObjectPtr, class TWatcherInterface>
EObjectWatcherState TObjectWatcher<TObjectPtr, TWatcherInterface>::WatchObject(
    TChaosObjectId objectId,
    NTransactionClient::TTimestamp cacheTimestamp,
    IObjectWatcherCallbacksPtr<TObjectPtr> callbacks,
    bool allowUnregistered)
{
    if (!IsRunning_.load()) {
        callbacks->OnInstanceIsNotLeader();
        return EObjectWatcherState::Normal;
    }

    {
        auto migratedObjectsGuard = ReaderGuard(MigratedObjectsLock_);
        if (auto it = MigratedObjects_.find(objectId); it != MigratedObjects_.end()) {
            auto destination = it->second.Destination;
            migratedObjectsGuard.Release();
            YT_TLOG_DEBUG("Object was already migrated")
                .With("ObjectId", objectId);
            callbacks->OnObjectMigrated(destination);
            return EObjectWatcherState::Migrated;
        }
    }

    {
        auto deletedObjectsGuard = ReaderGuard(DeletedObjectsLock_);
        if (DeletedObjects_.contains(objectId)) {
            deletedObjectsGuard.Release();
            YT_TLOG_DEBUG("Object was already deleted")
                .With("ObjectId", objectId);
            callbacks->OnObjectDeleted();
            return EObjectWatcherState::Deleted;
        }
    }

    {
        auto readGuard = ReaderGuard(EntriesLock_);
        auto it = WatchersByObjectId_.find(objectId);
        if (it != WatchersByObjectId_.end()) {
            auto& entry = it->second;
            entry->LastSeenWatchers.store(TInstant::Now());
            auto entryGuard = Guard(entry->Lock);
            if (entry->CurrentCacheTimestamp > cacheTimestamp) {
                auto object = entry->Object;
                auto timestamp = entry->CurrentCacheTimestamp;
                entryGuard.Release();
                readGuard.Release();

                YT_TLOG_DEBUG("Object updated between watches")
                    .With("ObjectId", objectId)
                    .With("CurrentCacheTimestamp", timestamp)
                    .With("CacheTimestamp", cacheTimestamp);

                callbacks->OnObjectChanged(object, timestamp);
                return EObjectWatcherState::Normal;
            }

            entry->WatcherEntries.push_back(TWatcherEntry{
                .Callbacks = std::move(callbacks),
                .RequestStartTime = TInstant::Now(),
            });

            entryGuard.Release();
            readGuard.Release();

            YT_TLOG_DEBUG("Added request to watchers list")
                .With("ObjectId", objectId);

            return EObjectWatcherState::Normal;
        }
    }

    if (allowUnregistered) {
        auto writeGuard = WriterGuard(EntriesLock_);
        auto& entry = WatchersByObjectId_[objectId];
        if (!entry) {
            entry = std::make_unique<TWatchersList>(
                NTransactionClient::NullTimestamp,
                nullptr);
        }

        entry->LastSeenWatchers.store(TInstant::Now());
        if (entry->CurrentCacheTimestamp > cacheTimestamp) {
            auto object = entry->Object;
            auto timestamp = entry->CurrentCacheTimestamp;
            writeGuard.Release();
            callbacks->OnObjectChanged(object, timestamp);
            return EObjectWatcherState::Normal;
        }

        auto entryGuard = Guard(entry->Lock);
        entry->WatcherEntries.push_back(TWatcherEntry{
            .Callbacks = std::move(callbacks),
            .RequestStartTime = TInstant::Now(),
        });
        return EObjectWatcherState::Normal;
    }

    YT_TLOG_WARNING("Object was not registered for update in watcher")
        .With("ObjectId", objectId);
    callbacks->OnUnknownObject();
    return EObjectWatcherState::Unknown;
}

template <class TObjectPtr, class TWatcherInterface>
bool TObjectWatcher<TObjectPtr, TWatcherInterface>::TryUnregisterObject(TChaosObjectId objectId)
{
    auto writeGuard = WriterGuard(EntriesLock_);
    auto it = WatchersByObjectId_.find(objectId);
    if (it == WatchersByObjectId_.end()) {
        return true;
    }

    auto& entry = it->second;
    if (!entry->WatcherEntries.empty()) {
        entry->LastSeenWatchers.store(TInstant::Now());
        return false;
    }

    WatchersByObjectId_.erase(it);
    return true;
}

template <class TObjectPtr, class TWatcherInterface>
TInstant TObjectWatcher<TObjectPtr, TWatcherInterface>::GetLastSeenWatchersTime(TChaosObjectId objectId)
{
    auto readGuard = ReaderGuard(EntriesLock_);
    auto it = WatchersByObjectId_.find(objectId);
    if (it == WatchersByObjectId_.end()) {
        return TInstant::Zero();
    }
    return it->second->LastSeenWatchers.load();
}

template <class TObjectPtr, class TWatcherInterface>
void TObjectWatcher<TObjectPtr, TWatcherInterface>::OnExpirationSweep()
{
    YT_TLOG_DEBUG("Started expired watchers sweep");

    std::vector<TWatcherEntry> expiredWatcherEntries;
    {
        auto deadline = TInstant::Now() - ExpirationTime_;
        auto readGuard = ReaderGuard(EntriesLock_);
        for (auto& [objectId, entry] : WatchersByObjectId_) {
            auto entryGuard = Guard(entry->Lock);
            if (!entry->WatcherEntries.empty()) {
                entry->LastSeenWatchers.store(TInstant::Now());
            }

            auto expiredWatchersIterator = entry->WatcherEntries.begin();
            while (expiredWatchersIterator != entry->WatcherEntries.end()) {
                if (expiredWatchersIterator->RequestStartTime > deadline) {
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

    auto goneObjectsDeadline = TInstant::Now() - GoneObjectsExpirationTime_;
    std::vector<TChaosObjectId> idsToRemove;
    {
        auto migratedObjectsGuard = ReaderGuard(MigratedObjectsLock_);
        for (const auto& [objectId, entry] : MigratedObjects_) {
            if (entry.When < goneObjectsDeadline) {
                idsToRemove.push_back(objectId);
            }
        }
    }

    if (!idsToRemove.empty()) {
        auto migratedObjectsGuard = WriterGuard(MigratedObjectsLock_);
        for (auto objectId : idsToRemove) {
            MigratedObjects_.erase(objectId);
        }
    }

    idsToRemove.clear();

    {
        auto deletedObjectsGuard = ReaderGuard(DeletedObjectsLock_);
        for (const auto& [objectId, deletionTime] : DeletedObjects_) {
            if (deletionTime < goneObjectsDeadline) {
                idsToRemove.push_back(objectId);
            }
        }
    }

    if (!idsToRemove.empty()) {
        auto deletedObjectsGuard = WriterGuard(DeletedObjectsLock_);
        for (auto objectId : idsToRemove) {
            DeletedObjects_.erase(objectId);
        }
    }

    YT_TLOG_DEBUG("Finished expired watchers sweep");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
