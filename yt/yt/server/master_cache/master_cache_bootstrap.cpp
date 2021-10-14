#include "master_cache_bootstrap.h"

#include "bootstrap.h"
#include "private.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/ytlib/object_client/caching_object_service.h>
#include <yt/yt/ytlib/object_client/object_service_cache.h>

#include <yt/yt/ytlib/program/build_attributes.h>

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
                CreateDefaultTimeoutChannel(
                    CreatePeerChannel(
                        masterConfig,
                        GetMasterConnection()->GetChannelFactory(),
                        EPeerKind::Follower),
                    masterConfig->RpcTimeout),
                ObjectServiceCache_,
                masterConfig->CellId,
                Logger);
        };

        CachingObjectServices_.push_back(initCachingObjectService(
            GetConfig()->ClusterConnection->PrimaryMaster));

        for (const auto& masterConfig : GetConfig()->ClusterConnection->SecondaryMasters) {
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
            CreateVirtualNode(ObjectServiceCache_->GetOrchidService()
                ->Via(GetControlInvoker())));
    }

    void Run() override
    { }

private:
    TActionQueuePtr MasterCacheQueue_;
    TObjectServiceCachePtr ObjectServiceCache_;
    std::vector<ICachingObjectServicePtr> CachingObjectServices_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateMasterCacheBootstrap(IBootstrap* bootstrap)
{
    return std::make_unique<TMasterCacheBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
