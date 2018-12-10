#pragma once

#include "private.h"

#include <yt/core/rpc/public.h>

#include <yt/client/api/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NApi::ITransactionPtr CreateTransaction(
    TConnectionPtr connection,
    TClientPtr client,
    NRpc::IChannelPtr channel,
    const NTransactionClient::TTransactionId& id,
    NTransactionClient::TTimestamp startTimestamp,
    NTransactionClient::ETransactionType type,
    NTransactionClient::EAtomicity atomicity,
    NTransactionClient::EDurability durability,
    TDuration timeout,
    std::optional<TDuration> pingPeriod,
    bool sticky);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
