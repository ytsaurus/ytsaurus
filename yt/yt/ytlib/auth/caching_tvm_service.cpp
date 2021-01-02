#include "caching_tvm_service.h"
#include "tvm_service.h"
#include "private.h"

#include <yt/core/misc/async_expiring_cache.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TCachingTvmService
    : public ITvmService
{
public:
    TCachingTvmService(
        ITvmServicePtr underlying,
        TAsyncExpiringCacheConfigPtr config,
        NProfiling::TRegistry profiler)
        : TicketCache_(New<TTicketCache>(config, underlying, profiler))
        , Underlying_(std::move(underlying))
    { }

    virtual TFuture<TString> GetTicket(const TString& serviceId) override
    {
        return TicketCache_->Get(serviceId);
    }

    virtual TErrorOr<TParsedTicket> ParseUserTicket(const TString& ticket) override
    {
        return Underlying_->ParseUserTicket(ticket);
    }

private:
    class TTicketCache
        : public TAsyncExpiringCache<TString, TString>
    {
    public:
        TTicketCache(
            TAsyncExpiringCacheConfigPtr config,
            ITvmServicePtr underlying,
            NProfiling::TRegistry profiler)
            : TAsyncExpiringCache(
                std::move(config),
                AuthLogger.WithTag("Cache: TvmTicket"),
                std::move(profiler))
            , Underlying_(std::move(underlying))
        { }

    private:
        const ITvmServicePtr Underlying_;

        virtual TFuture<TString> DoGet(const TString& serviceId, bool /*isPeriodicUpdate*/) noexcept override
        {
            return Underlying_->GetTicket(serviceId);
        }
    };

private:
    const TIntrusivePtr<TTicketCache> TicketCache_;
    const ITvmServicePtr Underlying_;

};

ITvmServicePtr CreateCachingTvmService(
    ITvmServicePtr underlying,
    TAsyncExpiringCacheConfigPtr config,
    NProfiling::TRegistry profiler)
{
    return New<TCachingTvmService>(
        std::move(underlying),
        std::move(config),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
