#pragma once

#include "public.h"

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateTimestampProxyService(
    NTransactionClient::ITimestampProviderPtr provider,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
