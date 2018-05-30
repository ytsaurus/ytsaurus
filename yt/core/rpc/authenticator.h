#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TAuthenticationResult
{
    TString User;
};

struct IAuthenticator
    : public virtual TRefCounted
{
    //! Validates authentication credentials in #header.
    //! Either returns an error or updates |user| field in #header to contain
    //! the actual (and validated) username.
    //! Returns null if #header contains no credentials that can be parsed by
    //! this authenticator.
    virtual TFuture<TAuthenticationResult> Authenticate(const NRpc::NProto::TRequestHeader& header) = 0;
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

} // namespace NRpc
} // namespace NYT
