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
    : Config_(std::move(config))
    , ChannelFactory_(CreateCachingChannelFactory(std::move(channelFactory)))
    , AddressPool_(New<TServerAddressPool>(
        Config_->ServerBanTimeout,
        DiscoveryClientLogger,
        Config_->ServerAddresses))
{ }

TFuture<std::vector<TMemberInfo>> TDiscoveryClient::ListMembers(
    const TString& groupId,
    const TListMembersOptions& options)
{
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
    auto session = New<TGetGroupMetaRequestSession>(
        AddressPool_,
        Config_,
        ChannelFactory_,
        Logger,
        groupId);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
