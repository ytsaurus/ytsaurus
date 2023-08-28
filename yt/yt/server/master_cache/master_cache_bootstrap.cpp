#include "master_cache_bootstrap.h"

#include "bootstrap.h"
#include "private.h"
#include "config.h"
#include "dynamic_config_manager.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/ytlib/object_client/caching_object_service.h>
#include <yt/yt/ytlib/object_client/object_service_cache.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/caching_channel_factory.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NMasterCache {

using namespace NApi;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = MasterCacheLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheBootstrap
    : public TBootstrapBase
{
public:
    using TBootstrapBase::TBootstrapBase;

    void Initialize() override
    {
        MasterCacheQueue_ = New<TActionQueue>("MasterCache");

        ObjectServiceCache_ = New<TObjectServiceCache>(
            GetConfig()->CachingObjectService,
            GetNullMemoryUsageTracker(),
            Logger,
            MasterCacheProfiler.WithPrefix("/object_service_cache"));

        auto initCachingObjectService = [&] (const auto& masterConfig) {
            return CreateCachingObjectService(
                GetConfig()->CachingObjectService,
                MasterCacheQueue_->GetInvoker(),
                GetConnection(),
                ObjectServiceCache_,
                masterConfig->CellId,
                Logger,
                GetNativeAuthenticator());
        };

        auto connectionStaticConfig = ConvertTo<NNative::TConnectionStaticConfigPtr>(GetConfig()->ClusterConnection);

        CachingObjectServices_.push_back(initCachingObjectService(connectionStaticConfig->PrimaryMaster));

        for (const auto& masterConfig : connectionStaticConfig->SecondaryMasters) {
            CachingObjectServices_.push_back(initCachingObjectService(masterConfig));
        }

        for (const auto& cachingObjectService : CachingObjectServices_) {
            GetRpcServer()->RegisterService(cachingObjectService);
        }

        SetBuildAttributes(
            GetOrchidRoot(),
            "master_cache");
        SetNodeByYPath(
            GetOrchidRoot(),
            "/object_service_cache",
            CreateVirtualNode(ObjectServiceCache_->GetOrchidService()));

        const auto& dynamicConfigManager = GetDynamicConfigManger();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TMasterCacheBootstrap::OnDynamicConfigChanged, Unretained(this)));
    }

    void Run() override
    { }

private:
    TActionQueuePtr MasterCacheQueue_;
    TObjectServiceCachePtr ObjectServiceCache_;
    std::vector<ICachingObjectServicePtr> CachingObjectServices_;

    void OnDynamicConfigChanged(
        const TMasterCacheDynamicConfigPtr& /*oldConfig*/,
        const TMasterCacheDynamicConfigPtr& newConfig)
    {
        ObjectServiceCache_->Reconfigure(newConfig->CachingObjectService);
        for (const auto& cachingObjectService : CachingObjectServices_) {
            cachingObjectService->Reconfigure(newConfig->CachingObjectService);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateMasterCacheBootstrap(IBootstrap* bootstrap)
{
    return std::make_unique<TMasterCacheBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
