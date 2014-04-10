#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateTimestampProxyService(NTransactionClient::ITimestampProviderPtr provider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
