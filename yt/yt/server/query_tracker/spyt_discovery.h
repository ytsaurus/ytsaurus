#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NQueryTracker {

///////////////////////////////////////////////////////////////////////////////

struct ISpytDiscovery
    : public TRefCounted
{
    virtual std::optional<TString> GetVersion() const = 0;

    virtual std::optional<TString> GetLivyUrl() const = 0;

    virtual std::optional<TString> GetMasterWebUIUrl() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISpytDiscovery)
DECLARE_REFCOUNTED_STRUCT(ISpytDiscovery)

///////////////////////////////////////////////////////////////////////////////

ISpytDiscoveryPtr CreateDiscoveryV1(
    NApi::IClientPtr queryClient,
    NYPath::TYPath discoveryPath);

ISpytDiscoveryPtr CreateDiscoveryV2(
    NApi::NNative::IConnectionPtr connection,
    TString discoveryGroup,
    NRpc::IChannelFactoryPtr channelFactory,
    NLogging::TLogger logger = {});

}
