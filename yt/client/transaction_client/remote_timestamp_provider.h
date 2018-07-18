#pragma once

#include "public.h"

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateBatchingTimestampProvider(
    ITimestampProviderPtr underlying,
    TDuration updatePeriod);

ITimestampProviderPtr CreateRemoteTimestampProvider(
    TRemoteTimestampProviderConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory);
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
