#pragma once

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <string>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EObjectWatcherState, ui8,
    ((Normal)   (0))
    ((Deleted)  (1))
    ((Migrated) (2))
    ((Unknown)  (3))
);

template <class TObjectPtr>
struct IObjectWatcherCallbacks
    : public virtual TRefCounted
{
    virtual void OnObjectChanged(
        const TObjectPtr& object,
        NTransactionClient::TTimestamp timestamp) = 0;

    virtual void OnObjectMigrated(NObjectClient::TCellId destination) = 0;
    virtual void OnObjectDeleted() = 0;
    virtual void OnInstanceIsNotLeader() = 0;
    virtual void OnNothingChanged() = 0;
    virtual void OnUnknownObject() = 0;
};

template <class TObjectPtr>
using IObjectWatcherCallbacksPtr = TIntrusivePtr<IObjectWatcherCallbacks<TObjectPtr>>;

template <class TObjectPtr>
struct IObjectWatcher
    : public virtual TRefCounted
{
    struct TSnapshot
    {
        TChaosObjectId ObjectId;
        TObjectPtr Object;
        NTransactionClient::TTimestamp CacheTimestamp = NTransactionClient::NullTimestamp;
    };

    virtual void RegisterObject(
        TChaosObjectId objectId,
        const TObjectPtr& object,
        NTransactionClient::TTimestamp timestamp) = 0;

    virtual void OnObjectUpdated(
        TChaosObjectId objectId,
        const TObjectPtr& object,
        NTransactionClient::TTimestamp timestamp) = 0;

    virtual void OnObjectRemoved(TChaosObjectId objectId) = 0;
    virtual void OnObjectsMigrated(
        const std::vector<std::pair<TChaosObjectId, NObjectClient::TCellId>>& objectIds) = 0;

    virtual EObjectWatcherState WatchObject(
        TChaosObjectId objectId,
        NTransactionClient::TTimestamp cacheTimestamp,
        IObjectWatcherCallbacksPtr<TObjectPtr> callbacks,
        bool allowUnregistered = false) = 0;

    virtual bool TryUnregisterObject(TChaosObjectId objectId) = 0;
    virtual TInstant GetLastSeenWatchersTime(TChaosObjectId objectId) = 0;

    virtual void Start(const std::vector<TSnapshot>& objects) = 0;
    virtual void Stop() = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TObjectPtr, class TWatcherInterface>
class TObjectWatcher
    : public TWatcherInterface
{
public:
    using TSnapshot = typename TWatcherInterface::TSnapshot;

    TObjectWatcher(
        IInvokerPtr invoker,
        TDuration expirationSweepPeriod,
        TDuration pollExpirationTime,
        TDuration goneObjectsExpirationTime,
        NLogging::TLogger logger,
        std::string objectName);

    void Start(const std::vector<TSnapshot>& objects) override;
    void Stop() override;

    void RegisterObject(
        TChaosObjectId objectId,
        const TObjectPtr& object,
        NTransactionClient::TTimestamp timestamp) override;

    void OnObjectUpdated(
        TChaosObjectId objectId,
        const TObjectPtr& object,
        NTransactionClient::TTimestamp timestamp) override;

    void OnObjectRemoved(TChaosObjectId objectId) override;
    void OnObjectsMigrated(
        const std::vector<std::pair<TChaosObjectId, NObjectClient::TCellId>>& objectIds) override;

    EObjectWatcherState WatchObject(
        TChaosObjectId objectId,
        NTransactionClient::TTimestamp cacheTimestamp,
        IObjectWatcherCallbacksPtr<TObjectPtr> callbacks,
        bool allowUnregistered) override;

    bool TryUnregisterObject(TChaosObjectId objectId) override;
    TInstant GetLastSeenWatchersTime(TChaosObjectId objectId) override;

private:
    struct TWatcherEntry
    {
        IObjectWatcherCallbacksPtr<TObjectPtr> Callbacks;
        TInstant RequestStartTime;
    };

    struct TWatchersList
    {
        TWatchersList(
            NTransactionClient::TTimestamp currentCacheTimestamp,
            TObjectPtr object);

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
        NTransactionClient::TTimestamp CurrentCacheTimestamp = NTransactionClient::NullTimestamp;
        TObjectPtr Object;
        std::vector<TWatcherEntry> WatcherEntries;
        std::atomic<TInstant> LastSeenWatchers = TInstant::Zero();
    };

    struct TMigratedObjectEntry
    {
        NObjectClient::TCellId Destination;
        TInstant When;
    };

    struct TMigrationDescriptor
    {
        TChaosObjectId ObjectId;
        NObjectClient::TCellId DestinationCellId;
        std::vector<TWatcherEntry> WatcherEntries;
    };

    const NConcurrency::TPeriodicExecutorPtr ExpirationExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, EntriesLock_);
    THashMap<TChaosObjectId, std::unique_ptr<TWatchersList>> WatchersByObjectId_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, MigratedObjectsLock_);
    THashMap<TChaosObjectId, TMigratedObjectEntry> MigratedObjects_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, DeletedObjectsLock_);
    THashMap<TChaosObjectId, TInstant> DeletedObjects_;

    const TDuration ExpirationTime_;
    const TDuration GoneObjectsExpirationTime_;
    std::atomic<bool> IsRunning_ = false;

    const NLogging::TLogger Logger;

    void OnExpirationSweep();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

#define OBJECT_WATCHER_INL_H_
#include "object_watcher-inl.h"
#undef OBJECT_WATCHER_INL_H_
