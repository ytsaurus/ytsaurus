#pragma once

#include "public.h"

#include <yt/core/net/address.h>

#include <yt/core/actions/future.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TAuthenticationResult
{
    TString User;
    TString Realm;
};

struct TAuthenticationContext
{
    const NRpc::NProto::TRequestHeader* Header;
    NNet::TNetworkAddress UserIP;
};

struct IAuthenticator
    : public virtual TRefCounted
{
    //! Validates authentication credentials in #context.
    //! Returns either an error or authentication result containing
    //! the actual (and validated) username.
    //! Returns null if #context contains no credentials that can be parsed by
    //! this authenticator.
    virtual TFuture<TAuthenticationResult> Authenticate(
        const TAuthenticationContext& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAuthenticator)

////////////////////////////////////////////////////////////////////////////////

//! Returns the authenticator that sequentially forwards the request to
//! all elements of #authenticators until a non-null response arrives.
//! If no such response is discovered, an error is returned.
IAuthenticatorPtr CreateCompositeAuthenticator(
    std::vector<IAuthenticatorPtr> authenticators);

//! Returns the authenticator that accepts any request.
IAuthenticatorPtr CreateNoopAuthenticator();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
