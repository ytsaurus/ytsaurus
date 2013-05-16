#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/rpc/public.h>
#include <ytlib/rpc/rpc.pb.h>

namespace NYT {
namespace NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

//! Sets "authenticated_user" in header.
void SetAuthenticatedUser(NRpc::NProto::TRequestHeader* header, const Stroka& user);

//! Sets "authenticated_user" in header.
void SetAuthenticatedUser(NRpc::IClientRequestPtr request, const Stroka& user);

//! Returns the value of "authenticated_user" from header or |Null| if missing.
TNullable<Stroka> FindAuthenticatedUser(const NRpc::NProto::TRequestHeader& header);

//! Returns the value of "authenticated_user" from header or |Null| if missing.
TNullable<Stroka> FindAuthenticatedUser(NRpc::IServiceContextPtr context);

//! Returns the value of "authenticated_user" from header. Throws if missing.
Stroka GetAuthenticatedUserOrThrow(NRpc::IServiceContextPtr context);

//! Returns a wrapper that sets "authenticated_user" attribute in every request.
NRpc::IChannelPtr CreateAuthenticatedChannel(
    NRpc::IChannelPtr underlyingChannel,
    const Stroka& user);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT
