#include "table_mount_cache_detail.h"

#include "config.h"

#include <yt/core/misc/hash.h>

namespace NYT::NTabletClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const TDuration TabletCacheExpireTimeout = TDuration::Seconds(1);

///////////////////////////////////////////////////////////////////////////////

TTabletInfoPtr TTabletCache::Find(TTabletId tabletId)
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
                THROW_ERROR_EXCEPTION(
                    EErrorCode::InvalidMountRevision,
                    "Tablet mount revision %llx is outdated",
                    tabletInfo->MountRevision)
                    << TErrorAttribute("tablet_id", tabletInfo->TabletId);
            }

            for (const auto& owner : existingTabletInfo->Owners) {
                if (!owner.IsExpired()) {
                    tabletInfo->Owners.push_back(owner);
                }
            }
        }

        it->second = MakeWeak(tabletInfo);
    } else {
        YT_VERIFY(Map_.insert({tabletInfo->TabletId, tabletInfo}).second);
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
    for (auto tabletId : removeIds) {
        Map_.erase(tabletId);
    }

    LastExpiredRemovalTime_ = Now();
}

////////////////////////////////////////////////////////////////////////////////

TTableMountCacheKey::TTableMountCacheKey(
    const NYPath::TYPath& path,
    NHydra::TRevision refreshPrimaryRevision,
    NHydra::TRevision refreshSecondaryRevision)
    : Path(path)
    , RefreshPrimaryRevision(refreshPrimaryRevision)
    , RefreshSecondaryRevision(refreshSecondaryRevision)
{ }

TTableMountCacheKey::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, Path);
    return result;
}

bool TTableMountCacheKey::operator == (const TTableMountCacheKey& other) const
{
    return Path == other.Path;
}

void FormatValue(TStringBuilderBase* builder, const TTableMountCacheKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("{%v %v %v}",
        key.Path,
        key.RefreshPrimaryRevision,
        key.RefreshSecondaryRevision);
}

TString ToString(const TTableMountCacheKey& key)
{
    return ToStringViaBuilder(key);
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
    auto [future, requestInitialized] = TAsyncExpiringCache::GetExtended(path);

    if (Config_->RejectIfEntryIsRequestedButNotReady && !requestInitialized && !future.IsSet()) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable,
            "Mount info is unavailable, please try again")
            << TError(NTabletClient::EErrorCode::TableMountInfoNotReady,
                "Table mount info is not ready, but has already been requested")
                << TErrorAttribute("path", path);
    }

    return future;
}

TTabletInfoPtr TTableMountCacheBase::FindTablet(TTabletId tabletId)
{
    return TabletCache_.Find(tabletId);
}

void TTableMountCacheBase::InvalidateTablet(TTabletInfoPtr tabletInfo)
{
    for (const auto& weakOwner : tabletInfo->Owners) {
        if (auto owner = weakOwner.Lock()) {
            InvalidateTable(owner);
        }
    }
}

std::pair<bool, TTabletInfoPtr> TTableMountCacheBase::InvalidateOnError(const TError& error)
{
    static std::vector<TErrorCode> retriableCodes = {
        NTabletClient::EErrorCode::NoSuchTablet,
        NTabletClient::EErrorCode::TabletNotMounted,
        NTabletClient::EErrorCode::InvalidMountRevision,
        NYTree::EErrorCode::ResolveError
    };

    if (!error.IsOK()) {
        for (auto code : retriableCodes) {
            if (auto retriableError = error.FindMatching(code)) {
                auto tabletId = retriableError->Attributes().Find<TTabletId>("tablet_id");
                if (!tabletId) {
                    continue;
                }
                auto tabletInfo = FindTablet(*tabletId);
                if (tabletInfo) {
                    YT_LOG_DEBUG(error, "Invalidating tablet in table mount cache (TabletId: %v, CellId: %v, MountRevision: %llx, Owners: %v)",
                        tabletInfo->TabletId,
                        tabletInfo->CellId,
                        tabletInfo->MountRevision,
                        MakeFormattableView(tabletInfo->Owners, [] (auto* builder, const auto& weakOwner) {
                            if (auto owner = weakOwner.Lock()) {
                                FormatValue(builder, owner->Path, TStringBuf());
                            }
                        }));

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
    YT_LOG_DEBUG("Table mount info cache cleared");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
