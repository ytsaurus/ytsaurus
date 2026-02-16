#include "pool_weight_provider.h"

#include <yt/yt/core/misc/async_expiring_cache.h>

#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NLogging;
using namespace NQueryClient;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPoolWeightCache)

class TPoolWeightCache
    : public TAsyncExpiringCache<std::string, double>
    , public ICypressPoolWeightProvider
{
public:
    TPoolWeightCache(
        TAsyncExpiringCacheConfigPtr config,
        TWeakPtr<IClient> client,
        IInvokerPtr invoker,
        TLogger logger)
        : TAsyncExpiringCache(
            std::move(config),
            invoker,
            std::move(logger).WithTag("Cache: PoolWeight"))
        , Client_(std::move(client))
        , Invoker_(std::move(invoker))
    { }

    double GetWeight(const std::string& poolName) override
    {
        {
            auto guard = ReaderGuard(SpinLock_);

            if (auto it = WeightOverrides_.find(poolName); it != WeightOverrides_.end()) {
                return it->second;
            }
        }

        if (auto optionalWeightOrError = this->Get(poolName).TryGet()) {
            return optionalWeightOrError->ValueOrThrow();
        }

        return DefaultPoolWeight;
    }

    void SetOverrides(THashMap<std::string, double> weights) override
    {
        auto guard = WriterGuard(SpinLock_);

        WeightOverrides_ = std::move(weights);
    }

    void SetClient(TWeakPtr<IClient> client) override
    {
        auto guard = WriterGuard(SpinLock_);

        Client_ = std::move(client);
    }

private:
    static constexpr double DefaultPoolWeight = 1.0;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, SpinLock_);
    THashMap<std::string, double> WeightOverrides_;
    TWeakPtr<IClient> Client_;
    IInvokerPtr Invoker_;

    TFuture<double> DoGet(
        const std::string& poolName,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        IClientPtr client;

        {
            auto guard = ReaderGuard(SpinLock_);
            client = Client_.Lock();
        }

        if (!client) {
            return MakeFuture(1.0);
        }

        return BIND(GetPoolWeight, client, poolName, Logger_)
            .AsyncVia(Invoker_)
            .Run();
    }

    static double GetPoolWeight(
        const NApi::NNative::IClientPtr& client,
        const std::string& poolName,
        const TLogger& Logger)
    {
        auto path = QueryPoolsPath + "/" + NYPath::ToYPathLiteral(poolName);

        NApi::TGetNodeOptions options;
        options.ReadFrom = NApi::EMasterChannelKind::Cache;
        auto rspOrError = WaitFor(client->GetNode(path + "/@weight", options));

        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            return DefaultPoolWeight;
        }

        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Failed to get pool info from Cypress, assuming defaults (Pool: %v)",
                poolName);
            return DefaultPoolWeight;
        }

        try {
            auto weight = ConvertTo<double>(rspOrError.Value());
            THROW_ERROR_EXCEPTION_IF(!std::isfinite(weight) || weight <= 0,
                "Weight must be a finite positive number");
            return weight;
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error parsing pool weight retrieved from Cypress, assuming default (Pool: %v)",
                poolName);
            return DefaultPoolWeight;
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TPoolWeightCache)

////////////////////////////////////////////////////////////////////////////////

ICypressPoolWeightProviderPtr CreateCachingCypressPoolWeightProvider(
    TAsyncExpiringCacheConfigPtr config,
    TWeakPtr<IClient> client,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
{
    return New<TPoolWeightCache>(
        std::move(config),
        std::move(client),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
