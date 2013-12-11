#pragma once

#include "public.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateRemoteTimestampProvider(
    TRemoteTimestampProviderConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory);
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
