#include "discovery_client.h"
#include "helpers.h"
#include "private.h"
#include "public.h"
#include "request_session.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/caching_channel_factory.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NDiscoveryClient {

using namespace NYTree;
using namespace NBus;
using namespace NRpc;
using namespace NConcurrency;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryClient
    : public IDiscoveryClient
{
public:
    TDiscoveryClient(
        TDiscoveryConnectionConfigPtr connectionConfig,
        TDiscoveryClientConfigPtr clientConfig,
        NRpc::IChannelFactoryPtr channelFactory)
        : ChannelFactory_(CreateCachingChannelFactory(std::move(channelFactory)))
        , AddressPool_(New<TServerAddressPool>(
            DiscoveryClientLogger,
            connectionConfig))
        , ConnectionConfig_(std::move(connectionConfig))
        , ClientConfig_(std::move(clientConfig))
    { }

    TFuture<std::vector<TMemberInfo>> ListMembers(
        const TString& groupId,
        const TListMembersOptions& options) override
    {
        auto guard = ReaderGuard(Lock_);

        return New<TListMembersRequestSession>(
            AddressPool_,
            ConnectionConfig_,
            ClientConfig_,
            ChannelFactory_,
            Logger,
            groupId,
            options)
            ->Run();
    }

    TFuture<TGroupMeta> GetGroupMeta(const TString& groupId) override
    {
        auto guard = ReaderGuard(Lock_);

        return New<TGetGroupMetaRequestSession>(
            AddressPool_,
            ConnectionConfig_,
            ClientConfig_,
            ChannelFactory_,
            Logger,
            groupId)
            ->Run();
    }

    void Reconfigure(TDiscoveryClientConfigPtr config) override
    {
        auto guard = WriterGuard(Lock_);

        ClientConfig_ = std::move(config);
    }

    TFuture<void> GetReadyEvent() const override
    {
        return AddressPool_->GetReadyEvent();
    }

private:
    const NLogging::TLogger Logger;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TServerAddressPoolPtr AddressPool_;
    const TDiscoveryConnectionConfigPtr ConnectionConfig_;

    NThreading::TReaderWriterSpinLock Lock_;
    TDiscoveryClientConfigPtr ClientConfig_;
};

IDiscoveryClientPtr CreateDiscoveryClient(
    TDiscoveryConnectionConfigPtr connectionConfig,
    TDiscoveryClientConfigPtr clientConfig,
    NRpc::IChannelFactoryPtr channelFactory)
{
    return New<TDiscoveryClient>(
        std::move(connectionConfig),
        std::move(clientConfig),
        std::move(channelFactory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
