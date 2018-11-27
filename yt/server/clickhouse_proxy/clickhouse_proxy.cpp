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

#include <util/string/cgiparam.h>
#include <util/string/vector.h>

#include <util/random/random.h>

namespace NYT {
namespace NClickHouseProxy {

using namespace NConcurrency;
using namespace NHttp;
using namespace NYTree;
using namespace NYPath;

const auto& Logger = ClickHouseProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TClickHouseRequest
{
public:
    TClickHouseRequest(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp,
        const TClickHouseProxyConfigPtr& config,
        const NAuth::ITokenAuthenticatorPtr& tokenAuthenticator,
        const NHttp::IClientPtr& httpClient)
        : Req_(req)
        , Rsp_(rsp)
        , Config_(config)
        , TokenAuthenticator_(tokenAuthenticator)
        , HttpClient_(httpClient)
    { }

    void Handle()
    {
        NHttp::IResponsePtr response;

        try {
            CgiParameters_ = TCgiParameters(Req_->GetUrl().RawQuery);

            ProcessHeaders();

            if (!Client_) {
                // We should have already replied with an error.
                return;
            }

            Authenticate();

            CliqueId_ = CgiParameters_.Get("database");
            if (CliqueId_.StartsWith("*")) {
                ResolveAlias();
            }

            LOG_INFO("Clique id parsed (CliqueId: %v)", CliqueId_);

            PickRandomInstance();
            LOG_INFO("Forwarding query to a randomly chosen instance (InstanceId: %v, Host: %v, HttpPort: %v)",
                InstanceId_,
                InstanceHost_,
                InstanceHttpPort_);

            auto body = Req_->ReadBody();
            if (!body) {
                ReplyWithError(EStatusCode::BadRequest, TError("Body should not be empty"));
                return;
            }

            auto headers = Req_->GetHeaders()->Duplicate();
            headers->Remove("Authorization");
            headers->Add("X-Yt-User", User_);
            headers->Add("X-Clickhouse-User", User_);

            auto url = Format("http://%v:%v%v?%v",
                InstanceHost_,
                InstanceHttpPort_,
                Req_->GetUrl().Path,
                CgiParameters_.Print());
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
    const NAuth::ITokenAuthenticatorPtr TokenAuthenticator_;
    const NHttp::IClientPtr& HttpClient_;

    NApi::IClientPtr Client_;
    TCgiParameters CgiParameters_;
    TString CliqueId_;
    TString Token_;
    TString User_;
    TString InstanceId_;
    TString InstanceHost_;
    TString InstanceHttpPort_;

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

    void ResolveAlias()
    {
        auto alias = CliqueId_;
        LOG_INFO("Resolving alias (Alias: %v)", alias);
        try {
            auto operationId = ConvertTo<TGuid>(WaitFor(
                Client_->GetNode(
                    Format("//sys/scheduler/orchid/scheduler/operations/%v/operation_id",
                    ToYPathLiteral(alias))))
                    .ValueOrThrow());
            CliqueId_ = ToString(operationId);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error while resolving alias %Qv", alias)
                << ex;
        }

        LOG_INFO("Alias resolved (Alias: %v, CliqueId: %v)", alias, CliqueId_);
        CgiParameters_.ReplaceUnescaped("database", CliqueId_);
    }

    void ParseTokenFromAuthorizationHeader(const TString& authorization) {
        LOG_INFO("Parsing token from Authorization header");
        // Two supported Authorization kinds are "Basic <base64(clique-id:oauth-token)>" and "OAuth <oauth-token>".
        auto authorizationTypeAndCredentials = SplitString(authorization, " ", 2);
        const auto& authorizationType = authorizationTypeAndCredentials[0];
        if (authorizationType == "OAuth" && authorizationTypeAndCredentials.size() == 2) {
            Token_ = authorizationTypeAndCredentials[1];
        } else if (authorizationType == "Basic" && authorizationTypeAndCredentials.size() == 2) {
            const auto& credentials = authorizationTypeAndCredentials[1];
            auto fooAndToken = SplitString(Base64Decode(credentials), ":", 2);
            if (fooAndToken.size() == 2) {
                // First component (that should be username) is ignored.
                Token_ = fooAndToken[1];
            } else {
                ReplyWithError(
                    EStatusCode::Unauthorized,
                    TError("Wrong 'Basic' authorization header format; 'default:<oauth-token>' encoded with base64 expected"));
                return;
            }
        } else {
            ReplyWithError(
                EStatusCode::Unauthorized,
                TError("Unsupported type of authorization header (AuthorizationType: %v, TokenCount: %v)",
                       authorizationType,
                       authorizationTypeAndCredentials.size()));
            return;
        }
        LOG_INFO("Token parsed (AuthorizationType: %v)", authorizationType);

    }

    void ProcessHeaders()
    {
        if (Req_->GetMethod() != NHttp::EMethod::Post) {
            ReplyWithError(EStatusCode::MethodNotAllowed, TError("Only POST requests are allowed"));
            return;
        }

        const auto* authorization = Req_->GetHeaders()->Find("Authorization");
        if (authorization && !authorization->empty()) {
            ParseTokenFromAuthorizationHeader(*authorization);
        } else if (CgiParameters_.Has("password")) {
            Token_ = CgiParameters_.Get("password");
            CgiParameters_.Erase("password");
            CgiParameters_.Erase("user");
        } else {
            ReplyWithError(EStatusCode::Unauthorized,
                TError("Authorization should be perfomed either by setting `Authorization` header (`Basic` or `OAuth` schemes) "
                       "or `password` CGI parameter"));
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
                    "ClickHouse proxy should be accessed via hostname of form clickhouse.<clustername>.yt.yandex.net "
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
        User_ = WaitFor(TokenAuthenticator_->Authenticate(credentials))
            .ValueOrThrow()
            .Login;
        LOG_INFO("User authenticated (User: %v)", User_);
    }

    void PickRandomInstance()
    {
        NApi::TListNodeOptions listOptions;
        listOptions.Attributes = {"http_port", "host"};
        auto listingYson = WaitFor(Client_->ListNode(Config_->DiscoveryPath + "/" + CliqueId_, listOptions))
            .ValueOrThrow();
        auto listingVector = ConvertTo<std::vector<IStringNodePtr>>(listingYson);
        const auto& randomEntry = listingVector[RandomNumber(listingVector.size())];
        const auto& attributes = randomEntry->Attributes();
        InstanceId_ = randomEntry->GetValue();
        InstanceHost_ = attributes.Get<TString>("host");
        InstanceHttpPort_ = attributes.Get<TString>("http_port");
    }
};

////////////////////////////////////////////////////////////////////////////////

TClickHouseProxyHandler::TClickHouseProxyHandler(const TClickHouseProxyConfigPtr& config, TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , HttpClient_(NHttp::CreateClient(Config_->HttpClient, CreateThreadPoolPoller(1, "client")))
{ }

void TClickHouseProxyHandler::HandleRequest(
    const IRequestPtr& req,
    const IResponseWriterPtr& rsp)
{
    TClickHouseRequest(req, rsp, Config_, Bootstrap_->GetTokenAuthenticator(), HttpClient_).Handle();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseProxy
} // namespace NYT
