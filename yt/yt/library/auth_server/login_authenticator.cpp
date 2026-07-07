#include "login_authenticator.h"
#include "helpers.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TCompositeLoginAuthenticator
    : public ILoginAuthenticator
{
public:
    explicit TCompositeLoginAuthenticator(std::vector<ILoginAuthenticatorPtr> authenticators)
        : Authenticators_(std::move(authenticators))
    { }

    TFuture<TLoginResult> Authenticate(const TLoginCredentials& credentials) override
    {
        return New<TCompositeAuthSession<ILoginAuthenticator, TLoginCredentials, TLoginResult>>(
            Authenticators_,
            credentials,
            TError(NRpc::EErrorCode::InvalidCredentials, "Authentication failed"))
            ->GetResult();
    }

private:
    const std::vector<ILoginAuthenticatorPtr> Authenticators_;
};

////////////////////////////////////////////////////////////////////////////////

ILoginAuthenticatorPtr CreateCompositeLoginAuthenticator(
    std::vector<ILoginAuthenticatorPtr> authenticators)
{
    return New<TCompositeLoginAuthenticator>(std::move(authenticators));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
