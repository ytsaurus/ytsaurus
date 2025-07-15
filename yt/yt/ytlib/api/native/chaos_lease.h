#pragma once

#include "client.h"

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

NApi::IPrerequisitePtr CreateChaosLease(
    IClientPtr client,
    NRpc::IChannelPtr channel,
    NChaosClient::TChaosLeaseId id,
    TDuration timeout,
    bool pingAncestors,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
