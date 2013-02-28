#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/rpc/public.h>

namespace NYT {
namespace NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

//! Sets "authenticated_user" attribute.
void SetRpcAuthenticatedUser(NRpc::IClientRequestPtr request, const Stroka& user);

//! Returns the value of "authenticated_user" attribute or |Null| if no such attribute is found.
TNullable<Stroka> FindRpcAuthenticatedUser(NRpc::IServiceContextPtr context);

//! Returns the value of "authenticated_user" attribute. Throws if the attribute is missing.
Stroka GetRpcAuthenticatedUser(NRpc::IServiceContextPtr context);

//! Returns a wrapper that attaches "authenticated_user" attribute to every request.
NRpc::IChannelPtr CreateAuthenticatedChannel(
    NRpc::IChannelPtr underlyingChannel,
    const Stroka& user);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT
