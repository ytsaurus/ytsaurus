#include "chaos_cache_service.h"

#include "bootstrap.h"
#include "chaos_cache.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/lib/chaos_cache/config.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

namespace NYT::NMasterCache {

using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TChaosCacheBootstrap
    : public TBootstrapBase
{
public:
    using TBootstrapBase::TBootstrapBase;

    void Initialize() override
    {
        auto client = GetConnection()->CreateNativeClient(
            TClientOptions::FromUser(NSecurityClient::RootUserName));

        ChaosCacheQueue_ = New<TActionQueue>("ChaosCache");

        ChaosCache_ = New<TChaosCache>(
            GetConfig()->ChaosCache,
            MasterCacheProfiler.WithPrefix("/chaos_cache"));

        ChaosCacheService_ = CreateChaosCacheService(
            ChaosCacheQueue_->GetInvoker(),
            client,
            ChaosCache_,
            GetNativeAuthenticator());

        GetRpcServer()->RegisterService(ChaosCacheService_);
    }

    void Run() override
    { }

private:
    TActionQueuePtr ChaosCacheQueue_;
    TChaosCachePtr ChaosCache_;
    NRpc::IServicePtr ChaosCacheService_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateChaosCacheBootstrap(IBootstrap* bootstrap)
{
    return std::make_unique<TChaosCacheBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
