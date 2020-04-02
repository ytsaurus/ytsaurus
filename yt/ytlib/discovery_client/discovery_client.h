#pragma once

#include "config.h"
#include "discovery_client_service_proxy.h"
#include "helpers.h"
#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/rpc/public.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryClient
    : public TRefCounted
{
public:
    TDiscoveryClient(
        TDiscoveryClientConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory);

    TFuture<std::vector<TMemberInfo>> ListMembers(const TString& groupId, const TListMembersOptions& option);

    TFuture<TGroupMeta> GetGroupMeta(const TString& groupId);

private:
    const NLogging::TLogger Logger;
    const TDiscoveryClientConfigPtr Config_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const NRpc::TServerAddressPoolPtr AddressPool_;
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
