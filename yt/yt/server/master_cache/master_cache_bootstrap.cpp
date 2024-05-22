#include "master_cache_bootstrap.h"

#include "bootstrap.h"
#include "private.h"
#include "config.h"
#include "dynamic_config_manager.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/ytlib/object_client/config.h>
#include <yt/yt/ytlib/object_client/caching_object_service.h>
#include <yt/yt/ytlib/object_client/object_service_cache.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/caching_channel_factory.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NMasterCache {

using namespace NApi;
using namespace NConcurrency;
using namespace NCellMasterClient;
using namespace NElection;
using namespace NHydra;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const auto static& Logger = MasterCacheLogger;

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
            MasterCacheLogger(),
            MasterCacheProfiler.WithPrefix("/object_service_cache"));

        const auto& connection = GetConnection();
        {
            // NB: initialize happens after master cell directory synchronization starts.
            auto guard = Guard(Lock_);
            AddCachingObjectService(connection->GetPrimaryMasterCellId());
            for (const auto& cellId : connection->GetMasterCellDirectory()->GetSecondaryMasterCellIds()) {
                AddCachingObjectService(cellId);
            }
        }

        SetBuildAttributes(
            GetOrchidRoot(),
            "master_cache");
        SetNodeByYPath(
            GetOrchidRoot(),
            "/object_service_cache",
            CreateVirtualNode(ObjectServiceCache_->GetOrchidService()));

        const auto& dynamicConfigManager = GetDynamicConfigManger();
        dynamicConfigManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TMasterCacheBootstrap::OnDynamicConfigChanged, Unretained(this)));
        connection->GetMasterCellDirectory()->SubscribeCellDirectoryChanged(BIND_NO_PROPAGATE(&TMasterCacheBootstrap::OnMasterCellDirectoryChanged, Unretained(this)));
    }

    void Run() override
    { }

private:
    TActionQueuePtr MasterCacheQueue_;
    TObjectServiceCachePtr ObjectServiceCache_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TCellTag, ICachingObjectServicePtr> CachingObjectServices_;

    void OnDynamicConfigChanged(
        const TMasterCacheDynamicConfigPtr& /*oldConfig*/,
        const TMasterCacheDynamicConfigPtr& newConfig)
    {
        ObjectServiceCache_->Reconfigure(newConfig->CachingObjectService);
        {
            auto guard = Guard(Lock_);
            for (const auto& [_, cachingObjectService] : CachingObjectServices_) {
                cachingObjectService->Reconfigure(newConfig->CachingObjectService);
            }
        }
    }

    void AddCachingObjectService(TCellId masterCellId)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        auto cachingObjectService = CreateCachingObjectService(
            GetConfig()->CachingObjectService,
            MasterCacheQueue_->GetInvoker(),
            CreateMasterChannelForCache(GetConnection(), masterCellId),
            ObjectServiceCache_,
            masterCellId,
            Logger(),
            MasterCacheProfiler.WithPrefix("/caching_object_service"),
            GetNativeAuthenticator());

        EmplaceOrCrash(CachingObjectServices_, CellTagFromId(masterCellId), cachingObjectService);
        GetRpcServer()->RegisterService(std::move(cachingObjectService));
    }

    void OnMasterCellDirectoryChanged(
        const TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs,
        const TSecondaryMasterConnectionConfigs& changedSecondaryMasterConfigs,
        const THashSet<TCellTag>& removedSecondaryMasterCellTags)
    {
        YT_LOG_ALERT_UNLESS(
            removedSecondaryMasterCellTags.empty(),
            "Some cells disappeared in received configuration of secondary masters (RemovedCellTags: %v)",
            removedSecondaryMasterCellTags);

        {
            auto guard = Guard(Lock_);
            for (const auto& [cellTag, masterConfig] : newSecondaryMasterConfigs) {
                AddCachingObjectService(masterConfig->CellId);
            }
        }

        auto makeFormattableCellTagsView = [] (const auto& secondaryMasterConfigs) {
            return MakeFormattableView(secondaryMasterConfigs, [] (auto* builder, const auto& pair) {
                builder->AppendFormat("%v", pair.first);
            });
        };

        YT_LOG_INFO("Received new master cell cluster configuration "
            "(NewCellTags: %v, ChangedCellTags: %v, RemovedCellTags: %v)",
            makeFormattableCellTagsView(newSecondaryMasterConfigs),
            makeFormattableCellTagsView(changedSecondaryMasterConfigs),
            removedSecondaryMasterCellTags);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateMasterCacheBootstrap(IBootstrap* bootstrap)
{
    return std::make_unique<TMasterCacheBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
