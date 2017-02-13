#include "api_service.h"
#include "public.h"
#include "private.h"

#include <yt/server/cell_proxy/bootstrap.h>

#include <yt/server/blackbox/cookie_authenticator.h>
#include <yt/server/blackbox/token_authenticator.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/rpc_proxy/api_service_proxy.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NRpcProxy {

using namespace NApi;
using namespace NYTree;
using namespace NConcurrency;
using namespace NRpc;
using namespace NCompression;
using namespace NBlackbox;

////////////////////////////////////////////////////////////////////////////////

class TApiService
    : public TServiceBase
{
public:
    TApiService(
        NCellProxy::TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(), // TODO(sandello): Better threading here.
            TApiServiceProxy::GetDescriptor(),
            RpcProxyLogger)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetNode));
    }

private:
    NCellProxy::TBootstrap* const Bootstrap_;

    TSpinLock SpinLock_;
    // TODO(sandello): Introduce expiration times for clients.
    yhash_map<Stroka, INativeClientPtr> AuthenticatedClients_;

    INativeClientPtr GetAuthenticatedClientOrAbortContext(const IServiceContextPtr& context)
    {
        auto replyWithMissingCredentials = [&] () {
            context->Reply(TError(
                NSecurityClient::EErrorCode::AuthenticationError,
                "Request is missing credentials"));
        };

        auto replyWithMissingUserIP = [&] () {
            context->Reply(TError(
                NSecurityClient::EErrorCode::AuthenticationError,
                "Request is missing originating address in credentials"));
        };

        const auto& header = context->GetRequestHeader();
        if (!header.HasExtension(NProto::TCredentialsExt::credentials_ext)) {
            replyWithMissingCredentials();
            return nullptr;
        }

        // TODO(sandello): Use a cache here.
        TAuthenticationResult authenticationResult;
        const auto& credentials = header.GetExtension(NProto::TCredentialsExt::credentials_ext);
        if (!credentials.has_userip()) {
            replyWithMissingUserIP();
            return nullptr;
        }
        if (credentials.has_sessionid() || credentials.has_sslsessionid()) {
            auto asyncAuthenticationResult = Bootstrap_->GetCookieAuthenticator()->Authenticate(
                credentials.sessionid(),
                credentials.sslsessionid(),
                credentials.domain(),
                credentials.userip());
            authenticationResult = WaitFor(asyncAuthenticationResult)
                .ValueOrThrow();
        } else if (credentials.has_token()) {
            auto asyncAuthenticationResult = Bootstrap_->GetTokenAuthenticator()->Authenticate(
                credentials.token(),
                credentials.userip());
            authenticationResult = WaitFor(asyncAuthenticationResult)
                .ValueOrThrow();
        } else {
            replyWithMissingCredentials();
            return nullptr;
        }

        const auto& user = context->GetUser();
        if (user != authenticationResult.Login) {
            context->Reply(TError(
                NSecurityClient::EErrorCode::AuthenticationError,
                "Invalid credentials"));
            return nullptr;
        }

        {
            auto guard = Guard(SpinLock_);
            auto it = AuthenticatedClients_.find(user);
            auto jt = AuthenticatedClients_.end();
            if (it == jt) {
                const auto& connection = Bootstrap_->GetNativeConnection();
                auto client = connection->CreateNativeClient(TClientOptions(user));
                bool inserted = false;
                std::tie(it, inserted) = AuthenticatedClients_.insert(std::make_pair(user, client));
                YCHECK(inserted);
            }
            return it->second;
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcProxy::NProto, GetNode)
    {
        auto client = GetAuthenticatedClientOrAbortContext(context);
        if (!client) {
            return;
        }

        // TODO(sandello): Inject options into req/rsp structure.
        auto options = TGetNodeOptions();
        client->GetNode(request->path(), options).Subscribe(BIND([=] (const TErrorOr<NYson::TYsonString>& result) {
            if (!result.IsOK()) {
                context->Reply(result);
            } else {
                response->set_data(result.Value().GetData());
                context->Reply();
            }
        }));
    }
};

IServicePtr CreateApiService(
    NCellProxy::TBootstrap* bootstrap)
{
    return New<TApiService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT

