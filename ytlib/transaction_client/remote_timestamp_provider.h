#pragma once

#include "public.h"

#include <yt/ytlib/object_client/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateRemoteTimestampProvider(
    NObjectClient::TCellTag cellTag,
    TRemoteTimestampProviderConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory);
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
