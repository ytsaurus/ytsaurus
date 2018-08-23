#include "clickhouse_proxy.h"

#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/ytlib/auth/authentication_manager.h>
#include <yt/ytlib/auth/token_authenticator.h>

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/client/api/rpc_proxy/config.h>
#include <yt/client/api/rpc_proxy/connection.h>

#include <yt/core/concurrency/thread_pool_poller.h>

#include <yt/core/http/http.h>
#include <yt/core/http/client.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/node.h>

#include <library/string_utils/base64/base64.h>

#include <util/string/vector.h>

#include <util/random/random.h>

namespace NYT {
namespace NClickHouseProxy {

using namespace NConcurrency;
using namespace NHttp;
using namespace NYTree;

const auto& Logger = ClickHouseProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TClickHouseProxyHttpHandler
{
public:
    TClickHouseProxyHttpHandler(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp,
        const TClickHouseProxyConfigPtr& config,
        const NAuth::TAuthenticationManagerPtr& authenticationManager,
        const NHttp::IClientPtr& httpClient)
        : Req_(req)
        , Rsp_(rsp)
        , Config_(config)
        , AuthenticationManager_(authenticationManager)
        , HttpClient_(httpClient)
    { }

    void Handle()
    {
        NHttp::IResponsePtr response;

        try {
            ProcessHeaders();

            if (!Client_) {
                // We should have already replied with an error.
                return;
            }

            Authenticate();

            NApi::TListNodeOptions listOptions;
            listOptions.Attributes = {"http_port", "host"};
            auto listingYson = WaitFor(Client_->ListNode(Config_->DiscoveryPath + "/" + CliqueId_, listOptions))
                .ValueOrThrow();
            auto listingVector = ConvertTo<std::vector<IStringNodePtr>>(listingYson);
            const auto& randomEntry = listingVector[RandomNumber(listingVector.size())];
            const auto& attributes = randomEntry->Attributes();
            auto host = attributes.Get<TString>("host");
            auto httpPort = attributes.Get<TString>("http_port");
            LOG_INFO("Forwarding query to a randomly chosen instance (InstanceId: %v, Host: %v, HttpPort: %v)",
                randomEntry->GetValue(),
                host,
                httpPort);

            auto body = Req_->ReadBody();
            if (!body) {
                ReplyWithError(EStatusCode::BadRequest, TError("Body should not be empty"));
                return;
            }

            auto headers = Req_->GetHeaders()->Duplicate();
            headers->RemoveOrThrow("Authorization");
            headers->Add("X-Yt-User", User_);
            headers->Add("X-Clickhouse-User", User_);

            auto url = "http://" + host + ":" + httpPort + Req_->GetUrl().Path + "?" + Req_->GetUrl().RawQuery;
            LOG_INFO("Querying instance (Url: %v)", url);
            response = WaitFor(HttpClient_->Post(url, body, headers))
                .ValueOrThrow();
        } catch (const std::exception& ex) {
            ReplyWithError(EStatusCode::InternalServerError, TError("Caught exception during preparation")
                << ex);
            return;
        }

        LOG_INFO("Got response from instance (StatusCode: %v)", response->GetStatusCode());
        Rsp_->SetStatus(response->GetStatusCode());
        Rsp_->GetHeaders()->MergeFrom(response->GetHeaders());
        PipeInputToOutput(response, Rsp_);
        LOG_INFO("Request handled");
        Client_->GetConnection()->Terminate();
    }

private:
    const IRequestPtr& Req_;
    const IResponseWriterPtr& Rsp_;
    const TClickHouseProxyConfigPtr& Config_;
    const NAuth::TAuthenticationManagerPtr& AuthenticationManager_;
    const NHttp::IClientPtr& HttpClient_;

    NApi::IClientPtr Client_;
    TString CliqueId_;
    TString Token_;
    TString User_;

    void ReplyWithError(EStatusCode statusCode, TError error) const
    {
        LOG_INFO(error, "Request failed during preparation");
        Rsp_->SetStatus(statusCode);
        WaitFor(Rsp_->WriteBody(TSharedRef::FromString(ToString(error))))
            .ThrowOnError();
        if (Client_) {
            Client_->GetConnection()->Terminate();
        }
    }

    void ProcessHeaders()
    {
        if (Req_->GetMethod() != NHttp::EMethod::Post) {
            ReplyWithError(EStatusCode::MethodNotAllowed, TError("Only POST requests are allowed"));
            return;
        }

        const auto* authorization = Req_->GetHeaders()->Find("Authorization");
        if (!authorization || authorization->empty()) {
            ReplyWithError(EStatusCode::Unauthorized, TError("Authorization header missing"));
            return;
        }

        // The only supported Authorization kind is "Basic <base64-encoded clique-id:oauth-token>".
        auto authorizationTypeAndCredentials = SplitStroku(*authorization, " ", 2);
        const auto& authorizationType = authorizationTypeAndCredentials[0];
        if (authorizationType == "Basic" && authorizationTypeAndCredentials.size() == 2) {
            const auto& credentials = authorizationTypeAndCredentials[1];
            auto cliqueIdAndToken = SplitStroku(Base64Decode(credentials), ":", 2);
            if (cliqueIdAndToken.size() == 2) {
                CliqueId_ = cliqueIdAndToken[0];
                Token_ = cliqueIdAndToken[1];
                LOG_INFO("Clique id and token parsed (CliqueId: %v)", CliqueId_);
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
            return;
        }

        if (!host->StartsWith("clickhouse.")) {
            ReplyWithError(
                EStatusCode::BadRequest,
                TError(
                    "Clickhouse proxy should be accessed via hostname of form clickhouse.<clustername>.yt.yandex.net "
                    "or clickhouse.*.yt.yandex-team.ru"));
            return;
        }



        LOG_INFO("Handling HTTP request (Host: %v)", *host);

        auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
        connectionConfig->ClusterUrl = host->substr(11 /* length of "clickhouse." */);
        auto connection = NApi::NRpcProxy::CreateConnection(std::move(connectionConfig));
        NApi::TClientOptions clientOptions;
        clientOptions.Token = Token_;
        Client_ = connection->CreateClient(clientOptions);

        LOG_INFO("RPC proxy client created");
    }

    void Authenticate()
    {
        NAuth::TTokenCredentials credentials;
        credentials.Token = Token_;
        User_ = WaitFor(AuthenticationManager_->GetTokenAuthenticator()->Authenticate(credentials))
            .ValueOrThrow()
            .Login;
        LOG_INFO("User authenticated (User: %v)", User_);
    }
};

////////////////////////////////////////////////////////////////////////////////

TClickHouseProxy::TClickHouseProxy(const TClickHouseProxyConfigPtr& config, TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , HttpClient_(NHttp::CreateClient(Config_->HttpClient, CreateThreadPoolPoller(1, "client")))
{ }

void TClickHouseProxy::HandleHttpRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) const
{
    TClickHouseProxyHttpHandler(req, rsp, Config_, Bootstrap_->GetAuthenticationManager(), HttpClient_).Handle();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseProxy
} // namespace NYT
