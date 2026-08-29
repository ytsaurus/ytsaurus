#include "native_authenticator.h"

#include "native_authentication_manager.h"

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <yt/yt/core/rpc/authenticator.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

namespace NYT::NAuth {

using namespace NRpc;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

class TNativeAuthenticator
    : public IAuthenticator
{
public:
    explicit TNativeAuthenticator(std::function<TError(TTvmId)> sourceValidator)
        : AuthenticationManager_(TNativeAuthenticationManager::Get())
        , TvmService_(AuthenticationManager_->GetTvmService())
        , SourceValidator_(std::move(sourceValidator))
    { }

    bool CanAuthenticate(const TAuthenticationContext& context) override
    {
        if (!NeedsAuthentication(context)) {
            return true;
        }

        if (!context.Header->HasExtension(TCredentialsExt::credentials_ext)) {
            return false;
        }

        const auto& ext = context.Header->GetExtension(TCredentialsExt::credentials_ext);
        return ext.has_service_ticket();
    }

    TFuture<NRpc::TAuthenticationResult> AsyncAuthenticate(
        const TAuthenticationContext& context) override
    {
        auto error = TryAuthenticate(context);
        if (error.IsOK()) {
            return MakeResult(context);
        } else if (NeedsAuthentication(context))
            return MakeFuture<NRpc::TAuthenticationResult>(std::move(error));
        else {
            YT_VERIFY(WarnOnUnauthenticated(context));
            return MakeResult(context, std::move(error));
        }
    }

private:
    TNativeAuthenticationManager* const AuthenticationManager_;
    const ITvmServicePtr TvmService_;
    const std::function<TError(TTvmId)> SourceValidator_;

    bool NeedsAuthentication(const TAuthenticationContext& context) const
    {
        return !context.IsLocal && TvmService_ && AuthenticationManager_->IsValidationEnabled();
    }

    bool WarnOnUnauthenticated(const TAuthenticationContext& context) const
    {
        return !context.IsLocal && TvmService_ && AuthenticationManager_->WarnOnUnauthenticated();
    }

    TFuture<NRpc::TAuthenticationResult> MakeResult(const TAuthenticationContext& context, TError warning = TError())
    {
        NRpc::TAuthenticationResult result{
            .User = context.Header->has_user() ? context.Header->user() : RootUserName,
            .Realm = "native",
            .Warning = std::move(warning),
        };
        return MakeFuture<NRpc::TAuthenticationResult>(std::move(result));
    }

    TError TryAuthenticate(const TAuthenticationContext& context)
    {
        if (!NeedsAuthentication(context) && !WarnOnUnauthenticated(context)) {
            return TError();
        }

        if (!context.Header->HasExtension(TCredentialsExt::credentials_ext)) {
            return TError(
                NRpc::EErrorCode::AuthenticationError,
                "Request is missing credentials");
        }

        const auto& ext = context.Header->GetExtension(TCredentialsExt::credentials_ext);
        if (!ext.has_service_ticket()) {
            return TError(
                NRpc::EErrorCode::AuthenticationError,
                "Request is missing credentials");
        }

        try {
            auto ticket = TvmService_->ParseServiceTicket(ext.service_ticket());
            SourceValidator_(ticket.TvmId)
                .ThrowOnError();
            return TError();
        } catch (const std::exception& ex) {
            return TError(
                NRpc::EErrorCode::AuthenticationError,
                "Error validating service ticket")
                .With(ex);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IAuthenticatorPtr CreateNativeAuthenticator(std::function<TError(TTvmId)> sourceValidator)
{
    return New<TNativeAuthenticator>(std::move(sourceValidator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
