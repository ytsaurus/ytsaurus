#include "clickhouse_proxy.h"

#include "private.h"

#include <yt/client/api/rpc_proxy/config.h>
#include <yt/client/api/rpc_proxy/connection.h>

#include <yt/core/http/http.h>

#include <library/string_utils/base64/base64.h>

#include <util/string/vector.h>

namespace NYT {
namespace NClickHouseProxy {

using namespace NConcurrency;
using namespace NHttp;

const auto& Logger = ClickHouseProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TClickHouseProxyHttpHandler
{
public:
    TClickHouseProxyHttpHandler(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp,
        const TClickHouseProxyConfigPtr& config)
        : Req_(req)
        , Rsp_(rsp)
        , Config_(config)
    { }

    void Handle()
    {
        try {
            ProcessHeaders();
        } catch (const std::exception& ex) {
            ReplyWithError(EStatusCode::InternalServerError, TError("Caught exception during preparation")
                << ex);
        }
        if (!Client_) {
            // We should have already replied with an error.
            return;
        }

        // TODO(max42).
    }

private:
    const IRequestPtr& Req_;
    const IResponseWriterPtr& Rsp_;
    const TClickHouseProxyConfigPtr& Config_;

    NApi::IClientPtr Client_;
    TString CliqueId_;

    void ReplyWithError(EStatusCode statusCode, TError error) const
    {
        LOG_INFO(error, "Request failed during preparation");
        Rsp_->SetStatus(statusCode);
        WaitFor(Rsp_->WriteBody(TSharedRef::FromString(ToString(error))))
            .ThrowOnError();
    }

    void ProcessHeaders()
    {
        const auto* authorization = Req_->GetHeaders()->Find("Authorization");
        if (!authorization || authorization->empty()) {
            ReplyWithError(EStatusCode::Unauthorized, TError("Authorization header missing"));
            return;
        }

        // The only supported Authorization kind is "Basic <base64-encoded clique-id:oauth-token>".
        auto authorizationTypeAndCredentials = SplitStroku(*authorization, " ", 2);
        const auto& authorizationType = authorizationTypeAndCredentials[0];
        TString cliqueId;
        TString oauthToken;
        if (authorizationType == "Basic" && authorizationTypeAndCredentials.size() == 2) {
            const auto& credentials = authorizationTypeAndCredentials[1];
            auto cliqueIdAndToken = SplitStroku(Base64Decode(credentials), ":", 2);
            if (cliqueIdAndToken.size() == 2) {
                cliqueId = cliqueIdAndToken[0];
                oauthToken = cliqueIdAndToken[1];
            } else {
                ReplyWithError(
                    EStatusCode::Unauthorized,
                    TError("Wrong 'Basic' authorization header format; 'clique-id:oauth-token' encoded with base64 expected"));
                return;
            }
        } else {
            ReplyWithError(
                EStatusCode::Unauthorized,
                TError("Unsupported type of authorization header (AuthorizationType: %v)", authorizationType));
            return;
        }

        const auto* host = Req_->GetHeaders()->Find("Host");
        if (!host) {
            ReplyWithError(EStatusCode::BadRequest, TError("Host header missing"));
        }

        LOG_INFO("Handling HTTP request (Host: %v)", *host);

        auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
        connectionConfig->ClusterUrl = *host;
        auto connection = NApi::NRpcProxy::CreateConnection(std::move(connectionConfig));
        NApi::TClientOptions clientOptions;
        clientOptions.Token = oauthToken;
        Client_ = connection->CreateClient(clientOptions);

        LOG_INFO("RPC proxy client created");
    }

};

////////////////////////////////////////////////////////////////////////////////

TClickHouseProxy::TClickHouseProxy(const TClickHouseProxyConfigPtr& config)
    : Config_(config)
{ }

void TClickHouseProxy::HandleHttpRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) const
{
    TClickHouseProxyHttpHandler(req, rsp, Config_).Handle();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseProxy
} // namespace NYT
