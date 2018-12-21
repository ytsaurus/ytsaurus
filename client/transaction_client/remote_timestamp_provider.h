#pragma once

#include "public.h"

#include <yt/core/rpc/public.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateBatchingTimestampProvider(
    ITimestampProviderPtr underlying,
    TDuration updatePeriod);

ITimestampProviderPtr CreateRemoteTimestampProvider(
    TRemoteTimestampProviderConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory);
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
