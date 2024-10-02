#pragma once

#include "public.h"

#include <yt/yt/core/rpc/peer_discovery.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

struct IOrmPeerDiscovery
    : public NRpc::IPeerDiscovery
{
public:
    virtual TFuture<TGetMastersResult> GetMasters(
        NRpc::IChannelPtr channel,
        TDuration timeout = TDuration::Seconds(60)) = 0;

    virtual std::optional<TString> GetAddress(TMasterInstanceTag instanceTag) const = 0;

    virtual std::optional<TString> ResolveIP6Address(const TString& address) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IOrmPeerDiscovery)

////////////////////////////////////////////////////////////////////////////////

template <typename TDiscoveryServiceProxy>
IOrmPeerDiscoveryPtr CreateOrmPeerDiscovery(
    NLogging::TLogger logger,
    std::optional<TString> balancerAddress = {},
    const std::optional<TString>& balancerIP6Address = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative

#define PEER_DISCOVERY_INL_H_
#include "peer_discovery-inl.h"
#undef PEER_DISCOVERY_INL_H_
