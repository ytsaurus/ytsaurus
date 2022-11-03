#include "distributed_throttler_manager.h"
#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/config.h>

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

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerManager
    : public IDistributedThrottlerManager
{
public:
    TDistributedThrottlerManager(
        IBootstrap* bootstrap,
        TCellId cellId)
        : Bootstrap_(bootstrap)
        , MemberId_(ToString(cellId))
    { }

    IThroughputThrottlerPtr GetOrCreateThrottler(
        const TYPath& tablePath,
        TCellTag cellTag,
        const TThroughputThrottlerConfigPtr& config,
        const TString& throttlerId,
        EDistributedThrottlerMode mode,
        TDuration rpcTimeout,
        bool admitUnlimitedThrottler) override
    {
        if (!config->Limit) {
            return admitUnlimitedThrottler
                ? GetUnlimitedThrottler()
                : nullptr;
        }

        TKey key(tablePath, mode);

        auto [it, inserted] = Factories_.emplace(key, nullptr);
        auto& factory = it->second;
        if (inserted) {
            try {
                auto factoryName = MakeFactoryName(tablePath, mode);

                YT_LOG_DEBUG("Creating distributed throttler factory "
                    "(TablePath: %v, ThrottlerMode: %v, RpcTimeout: %v, GroupId: %v)",
                    tablePath,
                    mode,
                    rpcTimeout,
                    factoryName);

                factory = DoCreateFactory(factoryName, cellTag, mode);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Failed to create distributed throttler factory "
                    "(TablePath: %v, ThrottlerMode: %v, RpcTimeout: %v)",
                    tablePath,
                    mode,
                    rpcTimeout);
                Factories_.erase(it);
                return admitUnlimitedThrottler
                    ? GetUnlimitedThrottler()
                    : nullptr;
            }
        }

        return factory->GetOrCreateThrottler(throttlerId, config, rpcTimeout);
    }

private:
    using TKey = std::tuple<TString, NDistributedThrottler::EDistributedThrottlerMode>;

    IBootstrap* const Bootstrap_;
    const TString MemberId_;

    THashMap<TKey, NDistributedThrottler::IDistributedThrottlerFactoryPtr> Factories_;

    static TString MakeFactoryName(
        const TString& tablePath,
        NDistributedThrottler::EDistributedThrottlerMode mode)
    {
        return Format("/dynamic_table_node%v/%v",
            tablePath.substr(1),
            mode);
    }

    NDistributedThrottler::IDistributedThrottlerFactoryPtr DoCreateFactory(
        const TString& factoryName,
        NObjectClient::TCellTag /*cellTag*/,
        NDistributedThrottler::EDistributedThrottlerMode mode)
    {
        auto config = New<TDistributedThrottlerConfig>();
        config->Mode = mode;

        // TODO(ifsmirnov,aleksandra-zh): YT-13318, reconfigure factories on the fly
        // via dynamic node config.

        return CreateDistributedThrottlerFactory(
            config,
            Bootstrap_->GetConnection()->GetChannelFactory(),
            Bootstrap_->GetConnection(),
            Bootstrap_->GetControlInvoker(),
            factoryName,
            MemberId_,
            Bootstrap_->GetRpcServer(),
            BuildServiceAddress(GetLocalHostName(), Bootstrap_->GetConfig()->RpcPort),
            TabletNodeLogger,
            Bootstrap_->GetNativeAuthenticator());
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerManager)

////////////////////////////////////////////////////////////////////////////////

IDistributedThrottlerManagerPtr CreateDistributedThrottlerManager(
    IBootstrap* bootstrap,
    TCellId cellId)
{
    return New<TDistributedThrottlerManager>(bootstrap, cellId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
