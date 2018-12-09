#include "caching_secret_vault_service.h"
#include "secret_vault_service.h"
#include "config.h"
#include "private.h"

#include <yt/core/misc/async_expiring_cache.h>
//#include <yt/core/concurrency/periodic_executor.h>
//#include <yt/core/concurrency/throughput_throttler.h>
//
//#include <yt/core/rpc/dispatcher.h>
//
//#include <queue>

namespace NYT {
namespace NAuth {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TCachingSecretVaultService
    : public ISecretVaultService
    , public TAsyncExpiringCache<
        ISecretVaultService::TSecretSubrequest,
        ISecretVaultService::TSecretSubresponse
     >
{
public:
    TCachingSecretVaultService(
        TCachingSecretVaultServiceConfigPtr config,
        ISecretVaultServicePtr underlying,
        NProfiling::TProfiler profiler)
        : TAsyncExpiringCache(config->Cache, std::move(profiler))
        , Underlying_(std::move(underlying))
    { }

    virtual TFuture<std::vector<TErrorOrSecretSubresponse>> GetSecrets(const std::vector<TSecretSubrequest>& subrequests) override
    {
        std::vector<TFuture<TSecretSubresponse>> asyncResults;
        THashMap<TSecretSubrequest, TFuture<TSecretSubresponse>> subrequestToAsyncResult;
        for (const auto& subrequest : subrequests) {
            auto it = subrequestToAsyncResult.find(subrequest);
            if (it == subrequestToAsyncResult.end()) {
                auto asyncResult = Get(subrequest);
                YCHECK(subrequestToAsyncResult.emplace(subrequest, asyncResult).second);
                asyncResults.push_back(std::move(asyncResult));
            } else {
                asyncResults.push_back(it->second);
            }
        }
        return CombineAll(asyncResults);
    }

private:
    const ISecretVaultServicePtr Underlying_;

    virtual TFuture<TSecretSubresponse> DoGet(const TSecretSubrequest& subrequest) override
    {
        return Underlying_->GetSecrets({subrequest})
            .Apply(BIND([] (const std::vector<TErrorOrSecretSubresponse>& result) {
                YCHECK(result.size() == 1);
                return result[0].ValueOrThrow();
            }));
    }
};

ISecretVaultServicePtr CreateCachingSecretVaultService(
    TCachingSecretVaultServiceConfigPtr config,
    ISecretVaultServicePtr underlying,
    NProfiling::TProfiler profiler)
{
    return New<TCachingSecretVaultService>(
        std::move(config),
        std::move(underlying),
        profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
