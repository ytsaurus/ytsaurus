#include "table_mount_cache_detail.h"

#include "config.h"

#include <yt/yt/core/misc/hash.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NTabletClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto TabletCacheSweepPeriod = TDuration::Seconds(60);

///////////////////////////////////////////////////////////////////////////////

TTabletInfoPtr TTabletInfoCache::Find(TTabletId tabletId)
{
    SweepExpiredEntries();

    auto guard = ReaderGuard(MapLock_);
    auto it = Map_.find(tabletId);
    return it != Map_.end() ? it->second.Lock() : nullptr;
}

TTabletInfoPtr TTabletInfoCache::Insert(const TTabletInfoPtr& tabletInfo)
{
    SweepExpiredEntries();

    auto guard = WriterGuard(MapLock_);
    typename decltype(Map_)::insert_ctx context;
    auto it = Map_.find(tabletInfo->TabletId, context);
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
        Map_.emplace_direct(context, tabletInfo->TabletId, tabletInfo);
    }
    return tabletInfo;
}

void TTabletInfoCache::Clear()
{
    auto guard = WriterGuard(MapLock_);
    Map_.clear();
}

void TTabletInfoCache::SweepExpiredEntries()
{
    auto now = NProfiling::GetCpuInstant();
    auto deadline = ExpiredEntriesSweepDeadline_.load(std::memory_order_relaxed);
    if (now < deadline) {
        return;
    }

    if (!ExpiredEntriesSweepDeadline_.compare_exchange_strong(deadline, now + NProfiling::DurationToCpuDuration(TabletCacheSweepPeriod))) {
        return;
    }

    std::vector<TTabletId> expiredIds;

    {
        auto guard = ReaderGuard(MapLock_);
        for (auto it = Map_.begin(); it != Map_.end(); ++it) {
            if (it->second.IsExpired()) {
                expiredIds.push_back(it->first);
            }
        }
    }

    if (!expiredIds.empty()) {
        auto guard = WriterGuard(MapLock_);
        for (auto id : expiredIds) {
            if (auto it = Map_.find(id); it && it->second.IsExpired()) {
                Map_.erase(it);
            }
        }
    }
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
    NLogging::TLogger logger)
    : TAsyncExpiringCache(
        config,
        logger.WithTag("Cache: TableMount"))
    , Config_(std::move(config))
    , Logger(std::move(logger))
{ }

TFuture<TTableMountInfoPtr> TTableMountCacheBase::GetTableInfo(const NYPath::TYPath& path)
{
    auto [future, requestInitialized] = TAsyncExpiringCache::GetExtended(path);

    if (Config_->RejectIfEntryIsRequestedButNotReady && !requestInitialized && !future.IsSet()) {
        // COMPAT(babenko): replace with TransientFailure error code.
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable,
            "Mount info is unavailable, please try again")
            << TError(NTabletClient::EErrorCode::TableMountInfoNotReady,
                "Table mount info is not ready, but has already been requested")
                << TErrorAttribute("path", path);
    }

    return future;
}

TTabletInfoPtr TTableMountCacheBase::FindTabletInfo(TTabletId tabletId)
{
    return TabletInfoCache_.Find(tabletId);
}

void TTableMountCacheBase::InvalidateTablet(TTabletInfoPtr tabletInfo)
{
    for (const auto& weakOwner : tabletInfo->Owners) {
        if (auto owner = weakOwner.Lock()) {
            InvalidateTable(owner);
        }
    }
}

std::pair<bool, TTabletInfoPtr> TTableMountCacheBase::InvalidateOnError(const TError& error, bool forceRetry)
{
    static std::vector<TErrorCode> retriableCodes = {
        NTabletClient::EErrorCode::NoSuchTablet,
        NTabletClient::EErrorCode::NoSuchCell,
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

                auto isTabletUnmounted = retriableError->Attributes().Get<bool>("is_tablet_unmounted", false);
                auto tabletInfo = FindTabletInfo(*tabletId);
                if (tabletInfo) {
                    YT_LOG_DEBUG(error,
                        "Invalidating tablet in table mount cache "
                        "(TabletId: %v, CellId: %v, MountRevision: %llx, IsTabletUnmounted: %v, Owners: %v)",
                        tabletInfo->TabletId,
                        tabletInfo->CellId,
                        tabletInfo->MountRevision,
                        isTabletUnmounted,
                        MakeFormattableView(tabletInfo->Owners, [] (auto* builder, const auto& weakOwner) {
                            if (auto owner = weakOwner.Lock()) {
                                FormatValue(builder, owner->Path, TStringBuf());
                            } else {
                                builder->AppendString(TStringBuf("<expired>"));
                            }
                        }));

                    InvalidateTablet(tabletInfo);
                }

                bool dontRetry =
                    code == NTabletClient::EErrorCode::TabletNotMounted &&
                    isTabletUnmounted &&
                    !forceRetry;

                return std::make_pair(!dontRetry, tabletInfo);
            }
        }
    }

    return std::make_pair(false, nullptr);
}

void TTableMountCacheBase::Clear()
{
    TAsyncExpiringCache::Clear();
    TabletInfoCache_.Clear();
    YT_LOG_DEBUG("Table mount info cache cleared");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
