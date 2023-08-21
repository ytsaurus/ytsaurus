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
    explicit TNativeAuthenticator(std::function<bool(TTvmId)> sourceValidator)
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
        if (!NeedsAuthentication(context)) {
            return MakeResult(context);
        }

        if (!context.Header->HasExtension(TCredentialsExt::credentials_ext)) {
            return MakeFuture<NRpc::TAuthenticationResult>(TError(
                NRpc::EErrorCode::AuthenticationError,
                "Request is missing credentials"));
        }

        const auto& ext = context.Header->GetExtension(TCredentialsExt::credentials_ext);
        if (!ext.has_service_ticket()) {
            return MakeFuture<NRpc::TAuthenticationResult>(TError(
                NRpc::EErrorCode::AuthenticationError,
                "Request is missing credentials"));
        }

        try {
            auto ticket = TvmService_->ParseServiceTicket(ext.service_ticket());

            if (!SourceValidator_(ticket.TvmId)) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::AuthenticationError,
                    "Source TVM id %v is rejected", ticket.TvmId);
            }

            return MakeResult(context);
        } catch (const std::exception& ex) {
            return MakeFuture<NRpc::TAuthenticationResult>(TError(
                NRpc::EErrorCode::AuthenticationError,
                "Error validating service ticket")
                << ex);
        }
    }

private:
    TNativeAuthenticationManager* AuthenticationManager_;
    const ITvmServicePtr TvmService_;
    const std::function<bool(TTvmId)> SourceValidator_;

    bool NeedsAuthentication(const TAuthenticationContext& context) const
    {
        return !context.IsLocal && TvmService_ && AuthenticationManager_->IsValidationEnabled();
    }

    TFuture<NRpc::TAuthenticationResult> MakeResult(const TAuthenticationContext& context)
    {
        NRpc::TAuthenticationResult result{
            .User = context.Header->has_user() ? context.Header->user() : RootUserName,
            .Realm = "native",
            .UserTicket = "",
        };
        return MakeFuture<NRpc::TAuthenticationResult>(std::move(result));
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IAuthenticatorPtr CreateNativeAuthenticator(std::function<bool(TTvmId)> sourceValidator)
{
    return New<TNativeAuthenticator>(std::move(sourceValidator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
