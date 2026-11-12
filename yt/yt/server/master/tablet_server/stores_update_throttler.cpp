#include "stores_update_throttler.h"

#include "config.h"
#include "private.h"
#include "public.h"

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NTabletServer {

using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TStoresUpdateThrottler
    : public IStoresUpdateThrottler
{
public:
    explicit TStoresUpdateThrottler(TStoresUpdateThrottlerConfigPtr config)
        : Profiler_(TabletServerProfiler().WithSparse().WithPrefix("/stores_update_throttler"))
        , Throttler_(CreateNamedReconfigurableThroughputThrottler(
            config->Throttler,
            "StoresUpdate",
            Logger(),
            Profiler_.WithPrefix("/total")))
        , Config_(std::move(config))
    { }

    void Acquire(const std::string& bundleName, ETabletStoresUpdateReason updateReason, int value) override
    {
        auto throttlers = GetThrottlers(bundleName, updateReason);
        for (const auto& throttler : throttlers) {
            throttler->Acquire(value);
        }
    }

    bool TryAcquire(const std::string& bundleName, ETabletStoresUpdateReason updateReason, int value) override
    {
        auto throttlers = GetThrottlers(bundleName, updateReason);
        for (const auto& throttler : throttlers) {
            if (throttler->IsOverdraft()) {
                return false;
            }
        }

        for (const auto& throttler : throttlers) {
            throttler->Acquire(value);
        }

        return true;
    }

    void Reconfigure(const TStoresUpdateThrottlerConfigPtr& newConfig) override
    {
        Throttler_->Reconfigure(newConfig->Throttler);

        for (const auto& [bundleName, bundleThrottler] : BundleThrottlers_) {
            auto newLimits = GetBundleLimits(newConfig, bundleName);
            if (Config_->Throttler->Period != newConfig->Throttler->Period ||
                GetBundleLimits(Config_, bundleName) != newLimits)
            {
                bundleThrottler->Reconfigure(newConfig, newLimits);
            }
        }

        Config_ = newConfig;
    }

private:
    struct TBundleLimits
    {
        int BundleLimit;
        double FlushRelativeLimit;
        double RegularRelativeLimit;

        bool operator==(const TBundleLimits&) const = default;
    };

    struct TBundleThrottler final
    {
        const IReconfigurableThroughputThrottlerPtr Throttler;
        const IReconfigurableThroughputThrottlerPtr FlushThrottler;
        const IReconfigurableThroughputThrottlerPtr RegularThrottler;

        TBundleThrottler(
            const TStoresUpdateThrottlerConfigPtr& config,
            const TBundleLimits& limits,
            const TProfiler& profiler,
            const TLogger& logger)
            : Throttler(CreateNamedReconfigurableThroughputThrottler(
                config->Throttler,
                "BundleStoresUpdate",
                logger,
                profiler.WithPrefix("/total")))
            , FlushThrottler(CreateNamedReconfigurableThroughputThrottler(
                config->Throttler,
                "FlushStoresUpdate",
                logger,
                profiler.WithPrefix("/by_reason").WithTag("reason", "flush")))
            , RegularThrottler(CreateNamedReconfigurableThroughputThrottler(
                config->Throttler,
                "RegularStoresUpdate",
                logger,
                profiler.WithPrefix("/by_reason").WithTag("reason", "regular")))
        {
            Reconfigure(config, limits);
        }

        void Reconfigure(const TStoresUpdateThrottlerConfigPtr& config, const TBundleLimits& limits)
        {
            auto reconfigure = [&] (const IReconfigurableThroughputThrottlerPtr& throttler, double throttlerLimit) {
                auto throttlerConfig = CloneYsonStruct(config->Throttler);
                throttlerConfig->Limit = throttlerLimit;
                throttler->Reconfigure(std::move(throttlerConfig));
            };

            int limit = limits.BundleLimit;
            reconfigure(Throttler, limit);
            reconfigure(FlushThrottler, std::ceil(limits.FlushRelativeLimit * limit));
            reconfigure(RegularThrottler, std::ceil(limits.RegularRelativeLimit * limit));
        }
    };

    using TBundleThrottlerPtr = TIntrusivePtr<TBundleThrottler>;

    const TProfiler Profiler_;
    const IReconfigurableThroughputThrottlerPtr Throttler_;
    TStoresUpdateThrottlerConfigPtr Config_;
    // TODO(alexelexa): Drop throttlers of destroyed bundles.
    THashMap<std::string, TBundleThrottlerPtr> BundleThrottlers_;

    static TBundleLimits GetBundleLimits(
        const TStoresUpdateThrottlerConfigPtr& config,
        const std::string& bundleName)
    {
        TBundleLimits limits{
            .BundleLimit = config->BundleLimit,
            .FlushRelativeLimit = config->FlushRelativeLimit,
            .RegularRelativeLimit = config->RegularRelativeLimit,
        };

        if (auto it = config->Bundles.find(bundleName); it != config->Bundles.end()) {
            const auto& bundleConfig = it->second;
            limits.BundleLimit = bundleConfig->BundleLimit.value_or(limits.BundleLimit);
            limits.FlushRelativeLimit = bundleConfig->FlushRelativeLimit.value_or(limits.FlushRelativeLimit);
            limits.RegularRelativeLimit = bundleConfig->RegularRelativeLimit.value_or(limits.RegularRelativeLimit);
        }

        return limits;
    }

    std::vector<IReconfigurableThroughputThrottlerPtr> GetThrottlers(
        const std::string& bundleName,
        ETabletStoresUpdateReason updateReason)
    {
        if (bundleName.empty()) {
            return {Throttler_};
        }

        auto bundleThrottlerIt = BundleThrottlers_.find(bundleName);
        if (bundleThrottlerIt == BundleThrottlers_.end()) {
            bundleThrottlerIt = EmplaceOrCrash(
                BundleThrottlers_,
                bundleName,
                New<TBundleThrottler>(
                    Config_,
                    GetBundleLimits(Config_, bundleName),
                    Profiler_
                        .WithPrefix("/bundle")
                        .WithTag("tablet_cell_bundle", bundleName),
                    Logger().WithTag("Bundle", bundleName)));
        }

        const auto& reasonThrottler = updateReason == ETabletStoresUpdateReason::Flush
            ? bundleThrottlerIt->second->FlushThrottler
            : bundleThrottlerIt->second->RegularThrottler;

        return {Throttler_, bundleThrottlerIt->second->Throttler, reasonThrottler};
    }
};

////////////////////////////////////////////////////////////////////////////////

IStoresUpdateThrottlerPtr CreateStoresUpdateThrottler(TStoresUpdateThrottlerConfigPtr config)
{
    return New<TStoresUpdateThrottler>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
