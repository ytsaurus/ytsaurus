#pragma once

#include "private.h"

#include <yt/ytlib/transaction_client/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NTransactionClient::ITimestampProviderPtr CreateTimestampProvider(
    TWeakPtr<TConnection> connection,
    TDuration rpcTimeout);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
