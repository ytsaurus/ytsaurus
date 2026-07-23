#pragma once

#include <yt/yt/client/api/public.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

NApi::IPrerequisitePtr CreateChaosLease(
    NApi::IClientPtr client,
    NRpc::IChannelPtr channel,
    NChaosClient::TChaosLeaseId id,
    TDuration timeout,
    bool pingAncestors,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
