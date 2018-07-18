#include "table_mount_cache_detail.h"

#include "config.h"

namespace NYT {
namespace NTabletClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const TDuration TabletCacheExpireTimeout = TDuration::Seconds(1);

///////////////////////////////////////////////////////////////////////////////

TTabletInfoPtr TTabletCache::Find(const TTabletId& tabletId)
{
    TReaderGuard guard(SpinLock_);
    RemoveExpiredEntries();
    auto it = Map_.find(tabletId);
    return it != Map_.end() ? it->second.Lock() : nullptr;
}

TTabletInfoPtr TTabletCache::Insert(TTabletInfoPtr tabletInfo)
{
    TWriterGuard guard(SpinLock_);
    auto it = Map_.find(tabletInfo->TabletId);
    if (it != Map_.end()) {
        if (auto existingTabletInfo = it->second.Lock()) {
            if (tabletInfo->MountRevision < existingTabletInfo->MountRevision) {
                THROW_ERROR_EXCEPTION("Tablet mount revision %llx is outdated",
                    tabletInfo->MountRevision);
            }

            for (const auto& owner : existingTabletInfo->Owners) {
                if (!owner.IsExpired()) {
                    tabletInfo->Owners.push_back(owner);
                }
            }
        }

        it->second = MakeWeak(tabletInfo);
    } else {
        YCHECK(Map_.insert({tabletInfo->TabletId, tabletInfo}).second);
    }
    return tabletInfo;
}

void TTabletCache::RemoveExpiredEntries()
{
    if (LastExpiredRemovalTime_ + TabletCacheExpireTimeout < Now()) {
        return;
    }

    std::vector<TTabletId> removeIds;
    for (auto it = Map_.begin(); it != Map_.end(); ++it) {
        if (it->second.IsExpired()) {
            removeIds.push_back(it->first);
        }
    }
    for (const auto& tabletId : removeIds) {
        Map_.erase(tabletId);
    }

    LastExpiredRemovalTime_ = Now();
}

////////////////////////////////////////////////////////////////////////////////

TTableMountCacheBase::TTableMountCacheBase(
    TTableMountCacheConfigPtr config,
    const NLogging::TLogger& logger)
    : TAsyncExpiringCache(config)
    , Config_(std::move(config))
    , Logger(logger)
{ }

TFuture<TTableMountInfoPtr> TTableMountCacheBase::GetTableInfo(const NYPath::TYPath& path)
{
    return TAsyncExpiringCache::Get(path);
}

TTabletInfoPtr TTableMountCacheBase::FindTablet(const TTabletId& tabletId)
{
    return TabletCache_.Find(tabletId);
}

void TTableMountCacheBase::InvalidateTablet(TTabletInfoPtr tabletInfo)
{
    for (const auto& weakOwner : tabletInfo->Owners) {
        if (auto owner = weakOwner.Lock()) {
            Invalidate(owner->Path);
        }
    }
}

std::pair<bool, TTabletInfoPtr> TTableMountCacheBase::InvalidateOnError(const TError& error)
{
    static std::vector<NTabletClient::EErrorCode> retriableCodes = {
        NTabletClient::EErrorCode::NoSuchTablet,
        NTabletClient::EErrorCode::TabletNotMounted,
        NTabletClient::EErrorCode::InvalidMountRevision
    };

    if (!error.IsOK()) {
        for (auto errCode : retriableCodes) {
            if (auto retriableError = error.FindMatching(errCode)) {
                // COMPAT(savrus) Not all above exceptions had tablet_id attribute in early 19.2 versions.
                auto tabletId = retriableError->Attributes().Find<TTabletId>("tablet_id");
                if (!tabletId) {
                    continue;
                }
                auto tabletInfo = FindTablet(*tabletId);
                if (tabletInfo) {
                    LOG_DEBUG(error, "Invalidating tablet in table mount cache (TabletId: %v, Owners:%v)",
                        tabletInfo->TabletId,
                        MakeFormattableRange(tabletInfo->Owners, [] (TStringBuilder* builder, const TWeakPtr<TTableMountInfo>& weakOwner) {
                            if (auto owner = weakOwner.Lock()) {
                                FormatValue(builder, owner->Path, TStringBuf());
                            }
                        }));

                    LOG_DEBUG(error, "Invalidating tablet in table mount cache (TabletId: %v)", *tabletId);
                    InvalidateTablet(tabletInfo);
                }
                return std::make_pair(true, tabletInfo);
            }
        }
    }

    return std::make_pair(false, nullptr);
}

void TTableMountCacheBase::Clear()
{
    TAsyncExpiringCache::Clear();
    LOG_DEBUG("Table mount info cache cleared");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT
