#pragma once

#include "private.h"

#include <yt/core/rpc/public.h>

#include <yt/ytlib/transaction_client/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NTransactionClient::ITimestampProviderPtr CreateTimestampProvider(
    NRpc::IChannelPtr channel,
    TDuration rpcTimeout);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
