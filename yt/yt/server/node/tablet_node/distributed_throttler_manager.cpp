#include "distributed_throttler_manager.h"
#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>


#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/discovery_client/config.h>

#include <yt/yt/ytlib/distributed_throttler/distributed_throttler.h>

#include <yt/yt/core/net/local_address.h>

namespace NYT::NTabletNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NDistributedThrottler;
using namespace NObjectClient;
using namespace NYPath;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerManager
    : public IDistributedThrottlerManager
{
public:
    TDistributedThrottlerManager(
        IBootstrap* bootstrap,
        NDiscoveryClient::TMemberId memberId)
        : Bootstrap_(bootstrap)
        , MemberId_(std::move(memberId))
        , DynamicConfigCallback_(BIND_NO_PROPAGATE(&TDistributedThrottlerManager::OnDynamicConfigChanged, MakeWeak(this)))
    {
        Bootstrap_->GetDynamicConfigManager()->SubscribeConfigChanged(DynamicConfigCallback_);
    }

    ~TDistributedThrottlerManager()
    {
        Bootstrap_->GetDynamicConfigManager()->UnsubscribeConfigChanged(DynamicConfigCallback_);
    }

    IReconfigurableThroughputThrottlerPtr GetOrCreateThrottler(
        const TYPath& tablePath,
        TCellTag cellTag,
        const TThroughputThrottlerConfigPtr& config,
        const TThrottlerId& throttlerId,
        ETabletDistributedThrottlerKind kind,
        TDuration rpcTimeout,
        bool admitUnlimitedThrottler,
        NProfiling::TProfiler profiler) override
    {
        if (!config->Limit) {
            return admitUnlimitedThrottler
                ? GetUnlimitedThrottler()
                : nullptr;
        }

        TKey key(tablePath, kind);

        IDistributedThrottlerFactoryPtr factory;

        // Fast path.
        {
            auto guard = ReaderGuard(SpinLock_);

            if (auto it = Factories_.find(key); it != Factories_.end()) {
                factory = it->second;
            }
        }

        // Slow path.
        if (!factory) {
            auto guard = WriterGuard(SpinLock_);

            auto [it, inserted] = Factories_.emplace(key, nullptr);
            if (inserted) {
                try {
                    auto factoryName = MakeFactoryName(tablePath, kind);

                    YT_LOG_DEBUG("Creating distributed throttler factory "
                        "(TablePath: %v, ThrottlerKind: %v, RpcTimeout: %v, GroupId: %v)",
                        tablePath,
                        kind,
                        rpcTimeout,
                        factoryName);

                    it->second = DoCreateFactory(factoryName, cellTag, kind, profiler);
                } catch (const std::exception& ex) {
                    YT_LOG_ERROR(ex, "Failed to create distributed throttler factory "
                        "(TablePath: %v, ThrottlerKind: %v, RpcTimeout: %v)",
                        tablePath,
                        kind,
                        rpcTimeout);

                    Factories_.erase(it);
                    return admitUnlimitedThrottler
                        ? GetUnlimitedThrottler()
                        : nullptr;
                }
            }

            factory = it->second;
        }

        return factory->GetOrCreateThrottler(throttlerId, config, rpcTimeout);
    }

private:
    using TDynamicConfigChangedCallback = TCallback<void(
        const TClusterNodeDynamicConfigPtr& oldNodeConfig,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)>;
    using TKey = std::tuple<TYPath, ETabletDistributedThrottlerKind>;

    IBootstrap* const Bootstrap_;
    const NDiscoveryClient::TMemberId MemberId_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<TKey, IDistributedThrottlerFactoryPtr> Factories_;

    const TDynamicConfigChangedCallback DynamicConfigCallback_;

    static NDiscoveryClient::TGroupId MakeFactoryName(const TYPath& tablePath, ETabletDistributedThrottlerKind kind)
    {
        return Format("/dynamic_table_node%v/%v",
            tablePath.substr(1),
            kind);
    }

    static TDistributedThrottlerConfigPtr GetThrottlerConfig(
        TEnumIndexedArray<ETabletDistributedThrottlerKind, TDistributedThrottlerConfigPtr> throttlerConfigs,
        ETabletDistributedThrottlerKind kind)
    {
        return throttlerConfigs[kind] ? throttlerConfigs[kind] : New<TDistributedThrottlerConfig>();
    }

    IDistributedThrottlerFactoryPtr DoCreateFactory(
        const NDiscoveryClient::TGroupId& factoryName,
        NObjectClient::TCellTag /*cellTag*/,
        ETabletDistributedThrottlerKind kind,
        NProfiling::TProfiler profiler)
    {
        auto throttlerConfigs = Bootstrap_->GetDynamicConfigManager()->GetConfig()->TabletNode->DistributedThrottlers;

        return CreateDistributedThrottlerFactory(
            GetThrottlerConfig(throttlerConfigs, kind),
            Bootstrap_->GetConnection()->GetChannelFactory(),
            Bootstrap_->GetConnection(),
            Bootstrap_->GetControlInvoker(),
            factoryName,
            MemberId_,
            Bootstrap_->GetRpcServer(),
            BuildServiceAddress(GetLocalHostName(), Bootstrap_->GetConfig()->RpcPort),
            TabletNodeLogger(),
            Bootstrap_->GetNativeAuthenticator(),
            profiler);
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        const auto& throttlerConfigs = newNodeConfig->TabletNode->DistributedThrottlers;

        auto guard = ReaderGuard(SpinLock_);

        for (const auto& [key, factory] : Factories_) {
            const auto& [path, kind] = key;
            factory->Reconfigure(GetThrottlerConfig(throttlerConfigs, kind));
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerManager)

////////////////////////////////////////////////////////////////////////////////

IDistributedThrottlerManagerPtr CreateDistributedThrottlerManager(
    IBootstrap* bootstrap,
    NDiscoveryClient::TMemberId memberId)
{
    return New<TDistributedThrottlerManager>(bootstrap, std::move(memberId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
