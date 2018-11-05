#pragma once

#include "public.h"

#include <yt/client/api/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/actions/public.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

class TAuthenticationManager
    : public TRefCounted
{
public:
    TAuthenticationManager(
        TAuthenticationManagerConfigPtr config,
        IInvokerPtr invoker,
        NApi::IClientPtr client);

    const NRpc::IAuthenticatorPtr& GetRpcAuthenticator() const;
    const NAuth::ITokenAuthenticatorPtr& GetTokenAuthenticator() const;
    const NAuth::ICookieAuthenticatorPtr& GetCookieAuthenticator() const;
    const NAuth::ITicketAuthenticatorPtr& GetTicketAuthenticator() const;

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TAuthenticationManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
