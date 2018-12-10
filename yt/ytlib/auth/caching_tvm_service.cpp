#include "caching_tvm_service.h"
#include "tvm_service.h"

#include <yt/core/misc/async_expiring_cache.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

class TCachingTvmService
    : public ITvmService
{
public:
    TCachingTvmService(
        ITvmServicePtr underlying,
        TAsyncExpiringCacheConfigPtr config,
        NProfiling::TProfiler profiler)
        : TicketCache_(New<TTicketCache>(config, underlying, profiler))
    { }

    virtual TFuture<TString> GetTicket(const TString& serviceId) override
    {
        return TicketCache_->Get(serviceId);
    }

private:
    class TTicketCache
        : public TAsyncExpiringCache<TString, TString>
    {
    public:
        TTicketCache(
            TAsyncExpiringCacheConfigPtr config,
            ITvmServicePtr underlying,
            NProfiling::TProfiler profiler)
            : TAsyncExpiringCache(std::move(config), std::move(profiler))
            , Underlying_(std::move(underlying))
        { }

    private:
        const ITvmServicePtr Underlying_;

    private:
        virtual TFuture<TString> DoGet(const TString& serviceId) override
        {
            return Underlying_->GetTicket(serviceId);
        }
    };

private:
    const TIntrusivePtr<TTicketCache> TicketCache_;

};

ITvmServicePtr CreateCachingTvmService(
    ITvmServicePtr underlying,
    TAsyncExpiringCacheConfigPtr config,
    NProfiling::TProfiler profiler)
{
    return New<TCachingTvmService>(
        std::move(underlying),
        std::move(config),
        std::move(profiler));
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT

