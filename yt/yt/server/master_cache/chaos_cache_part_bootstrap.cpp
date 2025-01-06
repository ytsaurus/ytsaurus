#include "chaos_cache_part_bootstrap.h"

#include "chaos_cache_service.h"
#include "part_bootstrap_detail.h"
#include "chaos_cache.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/lib/chaos_cache/config.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/server.h>

namespace NYT::NMasterCache {

using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TChaosCachePartBootstrap
    : public TPartBootstrapBase
{
public:
    explicit TChaosCachePartBootstrap(IBootstrap* bootstrap)
        : TPartBootstrapBase(bootstrap)
        , ChaosCacheQueue_(New<TActionQueue>("ChaosCache"))
    { }

    void Initialize() override
    {
        auto client = GetConnection()->CreateNativeClient(
            TClientOptions::FromUser(NSecurityClient::RootUserName));

        ChaosCache_ = New<TChaosCache>(
            GetConfig()->ChaosCache,
            MasterCacheProfiler().WithPrefix("/chaos_cache"));

        ChaosCacheService_ = CreateChaosCacheService(
            GetConfig()->ChaosCache,
            ChaosCacheQueue_->GetInvoker(),
            client,
            ChaosCache_,
            GetNativeAuthenticator());

        GetRpcServer()->RegisterService(ChaosCacheService_);
    }

private:
    const TActionQueuePtr ChaosCacheQueue_;

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
