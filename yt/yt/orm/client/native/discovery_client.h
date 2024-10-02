#pragma once

#include "response.h"

namespace NYT::NOrm::NClient::NNative::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TDiscoveryServiceProxy>
TFuture<TGetMastersResult> GetMasters(
    const NRpc::IChannelPtr& channel,
    const NLogging::TLogger& Logger,
    const TDuration timeout = TDuration::Seconds(60));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative::NDetail

#define DISCOVERY_CLIENT_INL_H_
#include "discovery_client-inl.h"
#undef DISCOVERY_CLIENT_INL_H_
