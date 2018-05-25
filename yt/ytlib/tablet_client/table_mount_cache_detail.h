#pragma once

#include "public.h"

#include "table_mount_cache.h"

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/async_expiring_cache.h>

namespace NYT {
namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

class TTabletCache
{
public:
    TTabletInfoPtr Find(const TTabletId& tabletId);
    TTabletInfoPtr Insert(TTabletInfoPtr tabletInfo);

private:
    void RemoveExpiredEntries();

    THashMap<TTabletId, TWeakPtr<TTabletInfo>> Map_;
    NConcurrency::TReaderWriterSpinLock SpinLock_;
    TInstant LastExpiredRemovalTime_;
};

///////////////////////////////////////////////////////////////////////////////

class TTableMountCacheBase
    : public ITableMountCache
    , public TAsyncExpiringCache<NYPath::TYPath, TTableMountInfoPtr>
{
public:
    TTableMountCacheBase(TTableMountCacheConfigPtr config, const NLogging::TLogger& logger);

    virtual TFuture<TTableMountInfoPtr> GetTableInfo(const NYPath::TYPath& path) override;
    virtual TTabletInfoPtr FindTablet(const TTabletId& tabletId) override;
    virtual void InvalidateTablet(TTabletInfoPtr tabletInfo) override;
    virtual std::pair<bool, TTabletInfoPtr> InvalidateOnError(const TError& error) override;
    virtual void Clear();

protected:
    const TTableMountCacheConfigPtr Config_;
    const NLogging::TLogger Logger;
    TTabletCache TabletCache_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT
