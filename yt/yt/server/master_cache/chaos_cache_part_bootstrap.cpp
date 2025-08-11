#include "chaos_cache_part_bootstrap.h"

#include "config.h"
#include "part_bootstrap_detail.h"
#include "private.h"

#include <yt/yt/server/lib/chaos_cache/chaos_cache.h>
#include <yt/yt/server/lib/chaos_cache/chaos_cache_service.h>
#include <yt/yt/server/lib/chaos_cache/config.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/rpc/server.h>

namespace NYT::NMasterCache {

using namespace NApi;
using namespace NChaosCache;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TChaosCachePartBootstrap
    : public TPartBootstrapBase
{
public:
    explicit TChaosCachePartBootstrap(IBootstrap* bootstrap)
        : TPartBootstrapBase(bootstrap)
        , WorkerPool_(CreateThreadPool(
            GetConfig()->ChaosCache->WorkerThreadCount,
            "ChaosCache"))
    { }

    void Initialize() override
    {
        auto client = GetConnection()->CreateNativeClient(
            TClientOptions::FromUser(NSecurityClient::RootUserName));

        ChaosCache_ = New<TChaosCache>(
            GetConfig()->ChaosCache,
            MasterCacheProfiler().WithPrefix("/chaos_cache"),
            MasterCacheLogger());

        ChaosCacheService_ = CreateChaosCacheService(
            GetConfig()->ChaosCache,
            WorkerPool_->GetInvoker(),
            client,
            ChaosCache_,
            GetNativeAuthenticator(),
            MasterCacheLogger());

        GetRpcServer()->RegisterService(ChaosCacheService_);
    }

private:
    const NConcurrency::IThreadPoolPtr WorkerPool_;

    TChaosCachePtr ChaosCache_;
    NRpc::IServicePtr ChaosCacheService_;
};

////////////////////////////////////////////////////////////////////////////////

IPartBootstrapPtr CreateChaosCachePartBootstrap(IBootstrap* bootstrap)
{
    return New<TChaosCachePartBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
