#include "discovery_client.h"
#include "helpers.h"
#include "private.h"
#include "public.h"
#include "request_session.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/rpc/caching_channel_factory.h>

#include <yt/core/rpc/bus/channel.h>

namespace NYT::NDiscoveryClient {

using namespace NYTree;
using namespace NBus;
using namespace NRpc;
using namespace NConcurrency;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TDiscoveryClient::TDiscoveryClient(
    TDiscoveryClientConfigPtr config,
    IChannelFactoryPtr channelFactory)
    : ChannelFactory_(CreateCachingChannelFactory(std::move(channelFactory)))
    , AddressPool_(New<TServerAddressPool>(
        config->ServerBanTimeout,
        DiscoveryClientLogger,
        config->ServerAddresses))
    , Config_(std::move(config))
{ }

TFuture<std::vector<TMemberInfo>> TDiscoveryClient::ListMembers(
    const TString& groupId,
    const TListMembersOptions& options)
{
    TReaderGuard guard(Lock_);

    auto session = New<TListMembersRequestSession>(
        AddressPool_,
        Config_,
        ChannelFactory_,
        Logger,
        groupId,
        options);
    return session->Run();
}

TFuture<TGroupMeta> TDiscoveryClient::GetGroupMeta(const TString& groupId)
{
    TReaderGuard guard(Lock_);

    auto session = New<TGetGroupMetaRequestSession>(
        AddressPool_,
        Config_,
        ChannelFactory_,
        Logger,
        groupId);
    return session->Run();
}

void TDiscoveryClient::Reconfigure(TDiscoveryClientConfigPtr config)
{
    TWriterGuard guard(Lock_);

    if (config->ServerBanTimeout != Config_->ServerBanTimeout) {
        AddressPool_->SetBanTimeout(config->ServerBanTimeout);
    }
    if (config->ServerAddresses != Config_->ServerAddresses) {
        AddressPool_->SetAddresses(config->ServerAddresses);
    }

    Config_ = std::move(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
