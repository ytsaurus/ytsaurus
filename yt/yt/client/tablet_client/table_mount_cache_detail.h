#pragma once

#include "table_mount_cache.h"

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

class TTabletInfoCache
{
public:
    explicit TTabletInfoCache(NLogging::TLogger logger);

    TTabletInfoPtr Find(TTabletId tabletId);
    TTabletInfoPtr Insert(const TTabletInfoPtr& tabletInfo);
    void Clear();

private:
    const NLogging::TLogger Logger;

    std::atomic<NProfiling::TCpuInstant> ExpiredEntriesSweepDeadline_ = 0;

    YT_DECLARE_SPINLOCK(NThreading::TReaderWriterSpinLock, MapLock_);
    THashMap<TTabletId, TWeakPtr<TTabletInfo>> Map_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, GCLock_);
    std::queue<TTabletId> GCQueue_;
    std::vector<TTabletId> ExpiredTabletIds_;

    void SweepExpiredEntries();
    void ProcessNextGCQueueEntry();
};

///////////////////////////////////////////////////////////////////////////////

class TTableMountCacheBase
    : public ITableMountCache
    , public TAsyncExpiringCache<NYPath::TYPath, TTableMountInfoPtr>
{
public:
    TTableMountCacheBase(
        TTableMountCacheConfigPtr config,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler = {});

    TFuture<TTableMountInfoPtr> GetTableInfo(const NYPath::TYPath& path) override;
    TTabletInfoPtr FindTabletInfo(TTabletId tabletId) override;
    void InvalidateTablet(TTabletInfoPtr tabletInfo) override;
    std::pair<std::optional<TErrorCode>, TTabletInfoPtr> InvalidateOnError(
        const TError& error,
        bool forceRetry) override;

    void Clear() override;

protected:
    const TTableMountCacheConfigPtr Config_;
    const NLogging::TLogger Logger;

    TTabletInfoCache TabletInfoCache_;

    virtual void InvalidateTable(const TTableMountInfoPtr& tableInfo) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
