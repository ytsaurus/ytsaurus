#pragma once

#include "private.h"

#include <yt/core/rpc/public.h>

#include <yt/client/transaction_client/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NTransactionClient::ITimestampProviderPtr CreateTimestampProvider(
    NRpc::IChannelPtr channel,
    TDuration rpcTimeout);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
