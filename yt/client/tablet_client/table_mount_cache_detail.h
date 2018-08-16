#pragma once

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

struct TTableMountCacheKey
{
    NYPath::TYPath Path;
    TNullable<i64> RefreshPrimaryRevision;
    TNullable<i64> RefreshSecondaryRevision;

    TTableMountCacheKey(
        const NYPath::TYPath& path,
        TNullable<i64> refreshPrimaryRevision = Null,
        TNullable<i64> refreshSecondaryRevision = Null);

    operator size_t() const;
    bool operator == (const TTableMountCacheKey& other) const;
};

void FormatValue(TStringBuilder* builder, const TTableMountCacheKey& key, TStringBuf /*spec*/);
TString ToString(const TTableMountCacheKey& key);

///////////////////////////////////////////////////////////////////////////////

class TTableMountCacheBase
    : public ITableMountCache
    , public TAsyncExpiringCache<TTableMountCacheKey, TTableMountInfoPtr>
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

    virtual void InvalidateTable(const TTableMountInfoPtr& tableInfo) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT
