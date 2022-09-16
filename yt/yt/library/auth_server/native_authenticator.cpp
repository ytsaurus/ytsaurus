#include "native_authenticator.h"

#include "tvm_service.h"

#include <yt/yt/core/rpc/authenticator.h>

namespace NYT::NAuth {

using namespace NRpc;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

class TNativeAuthenticator
    : public IAuthenticator
{
public:
    TNativeAuthenticator(
        ITvmServicePtr tvmService,
        std::function<bool(TTvmId)> sourceValidator)
        : TvmService_(std::move(tvmService))
        , SourceValidator_(std::move(sourceValidator))
    { }

    bool CanAuthenticate(const NRpc::TAuthenticationContext& context) override
    {
        if (!context.Header->HasExtension(TCredentialsExt::credentials_ext)) {
            return false;
        }
        const auto& ext = context.Header->GetExtension(TCredentialsExt::credentials_ext);
        return ext.has_service_ticket();
    }

    TFuture<NRpc::TAuthenticationResult> AsyncAuthenticate(
        const TAuthenticationContext& context) override
    {
        YT_ASSERT(CanAuthenticate(context));
        const auto& ext = context.Header->GetExtension(TCredentialsExt::credentials_ext);

        try {
            auto ticket = TvmService_->ParseServiceTicket(ext.service_ticket());

            if (!SourceValidator_(ticket.TvmId)) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::AuthenticationError,
                    "Source TVM id %v is rejected", ticket.TvmId);
            }

            NRpc::TAuthenticationResult result{
                .User = context.Header->has_user() ? context.Header->user() : RootUserName,
                .Realm = "native",
                .UserTicket = "",
            };

            return MakeFuture(std::move(result));
        } catch (const std::exception& ex) {
            return MakeFuture<NRpc::TAuthenticationResult>(TError(
                NRpc::EErrorCode::AuthenticationError,
                "Error validating service ticket")
                << ex);
        }
    }

private:
    const ITvmServicePtr TvmService_;
    const std::function<bool(TTvmId)> SourceValidator_;
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IAuthenticatorPtr CreateNativeAuthenticator(
    ITvmServicePtr tvmService,
    std::function<bool(TTvmId)> sourceValidator)
{
    return New<TNativeAuthenticator>(std::move(tvmService), std::move(sourceValidator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
