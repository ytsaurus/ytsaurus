#include "distributed_throttler_manager.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/discovery_client/config.h>

#include <yt/yt/ytlib/distributed_throttler/distributed_throttler.h>

namespace NYT::NTabletNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NDistributedThrottler;
using namespace NObjectClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerManager
    : public IDistributedThrottlerManager
{
public:
    TDistributedThrottlerManager(
        NClusterNode::TBootstrap* bootstrap,
        TCellId cellId)
        : Bootstrap_(bootstrap)
        , MemberId_(ToString(cellId))
    { }

    virtual IThroughputThrottlerPtr GetOrCreateThrottler(
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
                    "(TablePath: %v, ThrottlerMode: %v, GroupId: %v)",
                    tablePath,
                    mode,
                    rpcTimeout,
                    factoryName);

                factory = DoCreateFactory(factoryName, cellTag, mode);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Failed to create distributed throttler factory "
                    "(TablePath: %v, ThrottlerMode: %v)",
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

    NClusterNode::TBootstrap* const Bootstrap_;
    const TString MemberId_;

    THashMap<TKey, NDistributedThrottler::TDistributedThrottlerFactoryPtr> Factories_;

    static TString MakeFactoryName(
        const TString& tablePath,
        NDistributedThrottler::EDistributedThrottlerMode mode)
    {
        return Format("/dynamic_table_node%v/%v",
            tablePath.substr(1),
            mode);
    }

    NDistributedThrottler::TDistributedThrottlerFactoryPtr DoCreateFactory(
        const TString& factoryName,
        NObjectClient::TCellTag cellTag,
        NDistributedThrottler::EDistributedThrottlerMode mode)
    {
        auto config = New<TDistributedThrottlerConfig>();

        auto serverAddresses = Bootstrap_->GetMasterAddressesOrThrow(cellTag);
        config->MemberClient->ServerAddresses = serverAddresses;
        config->DiscoveryClient->ServerAddresses = serverAddresses;
        config->MemberClient->WriteQuorum = (serverAddresses.size() + 1) / 2;
        config->DiscoveryClient->ReadQuorum = (serverAddresses.size() + 1) / 2;

        config->Mode = mode;

        // TODO(ifsmirnov,aleksandra-zh): YT-13318, reconfigure factories on the fly
        // via dynamic node config.

        auto address = Bootstrap_->GetDefaultLocalAddressOrThrow();

        return New<TDistributedThrottlerFactory>(
            config,
            Bootstrap_->GetMasterConnection()->GetChannelFactory(),
            Bootstrap_->GetControlInvoker(),
            factoryName,
            MemberId_,
            Bootstrap_->GetRpcServer(),
            address,
            TabletNodeLogger);
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerManager)

////////////////////////////////////////////////////////////////////////////////

IDistributedThrottlerManagerPtr CreateDistributedThrottlerManager(
    NClusterNode::TBootstrap* bootstrap,
    TCellId cellId)
{
    return New<TDistributedThrottlerManager>(bootstrap, cellId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
