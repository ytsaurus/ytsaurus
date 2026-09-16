#include "token_service.h"

#include "private.h"
#include "token_manager.h"

#include <yt/yt/ytlib/yql_client/token_service_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NYqlAgent {

using namespace NRpc;
using namespace NYqlClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TokenServiceLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TTokenService
    : public TServiceBase
{
public:
    TTokenService(IInvokerPtr invoker, ITokenManagerPtr tokenManager)
        : TServiceBase(
            std::move(invoker),
            TTokenServiceProxy::GetDescriptor(),
            NYqlAgent::Logger())
        , TokenManager_(std::move(tokenManager))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(IssueQueryTemporaryToken));
    }

private:
    const ITokenManagerPtr TokenManager_;

    DECLARE_RPC_SERVICE_METHOD(NYqlClient::NProto, IssueQueryTemporaryToken)
    {
        context->SetRequestInfo("Cluster: %v", request->cluster());

        auto tokenFuture = TokenManager_->IssueQueryTemporaryToken(
            request->query_identity_token(),
            request->cluster());

        context->ReplyFrom(tokenFuture.Apply(BIND([context, response] (const TString& token) {
            response->set_token(token);
        })));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateTokenService(
    IInvokerPtr invoker,
    ITokenManagerPtr tokenManager)
{
    return New<TTokenService>(
        std::move(invoker),
        std::move(tokenManager));
}

} // namespace NYT::NYqlAgent
