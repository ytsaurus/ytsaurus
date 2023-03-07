#pragma once

#include "public.h"

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateTimestampProxyService(NTransactionClient::ITimestampProviderPtr provider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
