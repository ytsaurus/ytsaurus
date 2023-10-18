#pragma once

#include "config.h"
#include "discovery_client_service_proxy.h"
#include "helpers.h"
#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

struct IDiscoveryClient
    : public virtual TRefCounted
{
    //! GetReadyEvent's future should be awaited before discovery client usage.
    virtual TFuture<void> GetReadyEvent() const = 0;

    virtual TFuture<std::vector<TMemberInfo>> ListMembers(
        const TString& groupId,
        const TListMembersOptions& option) = 0;

    virtual TFuture<TGroupMeta> GetGroupMeta(const TString& groupId) = 0;

    virtual void Reconfigure(TDiscoveryClientConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDiscoveryClient)

IDiscoveryClientPtr CreateDiscoveryClient(
    TDiscoveryConnectionConfigPtr connectionConfig,
    TDiscoveryClientConfigPtr clientConfig,
    NRpc::IChannelFactoryPtr channelFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
