#include "authenticator.h"

#include <yt/core/rpc/proto/rpc.pb.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TCompositeAuthenticator
    : public IAuthenticator
{
public:
    explicit TCompositeAuthenticator(std::vector<IAuthenticatorPtr> authenticators)
        : Authenticators_(std::move(authenticators))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(
        const TAuthenticationContext& context) override
    {
        for (const auto& authenticator : Authenticators_) {
            auto asyncResult = authenticator->Authenticate(context);
            if (asyncResult) {
                return asyncResult;
            }
        }
        return MakeFuture<TAuthenticationResult>(TError(
            NYT::NRpc::EErrorCode::AuthenticationError,
            "Request is missing credentials"));
    }

private:
    const std::vector<IAuthenticatorPtr> Authenticators_;
};

IAuthenticatorPtr CreateCompositeAuthenticator(
    std::vector<IAuthenticatorPtr> authenticators)
{
    return New<TCompositeAuthenticator>(std::move(authenticators));
}

////////////////////////////////////////////////////////////////////////////////

class TNoopAuthenticator
    : public IAuthenticator
{
public:
    virtual TFuture<TAuthenticationResult> Authenticate(
        const TAuthenticationContext& context) override
    {
        static const auto Realm = TString("noop");
        TAuthenticationResult result{
            context.Header->has_user() ? context.Header->user() : RootUserName,
            Realm
        };
        return MakeFuture<TAuthenticationResult>(result);
    }
};

IAuthenticatorPtr CreateNoopAuthenticator()
{
    return New<TNoopAuthenticator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

