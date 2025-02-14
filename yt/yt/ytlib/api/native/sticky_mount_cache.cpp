#include "sticky_mount_cache.h"

namespace NYT::NApi::NNative {

using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

class TStickyTableMountInfoCache
    : public IStickyMountCache
{
public:
    explicit TStickyTableMountInfoCache(ITableMountCachePtr underlying)
        : Underlying_(std::move(underlying))
    { }

    TFuture<TTableMountInfoPtr> GetTableInfo(const NYPath::TYPath& path) override
    {
        auto guard = Guard(SpinLock_);

        auto [it, inserted] = TableInfoMap_.insert({path, {}});
        if (!inserted) {
            return it->second;
        }

        it->second = Underlying_->GetTableInfo(path);
        it->second
            .AsVoid()
            .Subscribe(BIND(&TStickyTableMountInfoCache::AddWaitTime, MakeWeak(this), TInstant::Now()));

        return it->second;
    }

    void InvalidateTablet(TTabletId /*tablet*/) override
    {
        YT_ABORT();
    }

    TInvalidationResult InvalidateOnError(const TError& /*error*/, bool /*forceRetry*/) override
    {
        YT_ABORT();
    }

    void Clear() override
    {
        YT_ABORT();
    }

    void Reconfigure(NTabletClient::TTableMountCacheConfigPtr /*config*/) override
    {
        YT_ABORT();
    }

    TDuration GetWaitTime() const override
    {
        return TDuration::MicroSeconds(TotalWaitTime_.load(std::memory_order_relaxed));
    }

private:
    const ITableMountCachePtr Underlying_;

    std::atomic<ui64> TotalWaitTime_{};

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<NYPath::TYPath, TFuture<TTableMountInfoPtr>> TableInfoMap_;

    void AddWaitTime(TInstant start, const TError& /*error*/)
    {
        TotalWaitTime_.fetch_add((TInstant::Now() - start).MicroSeconds());
    }
};

////////////////////////////////////////////////////////////////////////////////

IStickyMountCachePtr CreateStickyCache(ITableMountCachePtr cache)
{
    return New<TStickyTableMountInfoCache>(std::move(cache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
