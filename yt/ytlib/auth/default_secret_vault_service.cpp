#include "default_secret_vault_service.h"
#include "secret_vault_service.h"
#include "tvm_service.h"
#include "config.h"
#include "private.h"

#include <yt/core/misc/string.h>

#include <yt/core/json/json_writer.h>
#include <yt/core/json/json_parser.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/tree_builder.h>
#include <yt/core/ytree/ephemeral_node_factory.h>

#include <yt/core/https/client.h>

#include <yt/core/http/client.h>
#include <yt/core/http/http.h>
#include <yt/core/http/helpers.h>

namespace NYT {
namespace NAuth {

using namespace NConcurrency;
using namespace NJson;
using namespace NYTree;
using namespace NHttp;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TDefaultSecretVaultService
    : public ISecretVaultService
{
public:
    TDefaultSecretVaultService(
        TDefaultSecretVaultServiceConfigPtr config,
        ITvmServicePtr tvmService,
        IPollerPtr poller)
        : Config_(std::move(config))
        , TvmService_(std::move(tvmService))
        , HttpClient_(NHttps::CreateClient(Config_->HttpClient, std::move(poller)))
    { }

    virtual TFuture<std::vector<TErrorOrSecretSubresponse>> GetSecrets(const std::vector<TSecretSubrequest>& subrequests) override
    {
        return TvmService_->GetTicket(Config_->VaultServiceId)
            .Apply(BIND(
                &TDefaultSecretVaultService::OnTvmCallResult,
                MakeStrong(this),
                subrequests));
    }

private:
    const TDefaultSecretVaultServiceConfigPtr Config_;
    const ITvmServicePtr TvmService_;
    const NHttp::IClientPtr HttpClient_;

    TFuture<std::vector<TErrorOrSecretSubresponse>> OnTvmCallResult(const std::vector<TSecretSubrequest>& subrequests, const TString& vaultTicket)
    {
        auto callId = TGuid::Create();

        LOG_DEBUG("Retrieving secrets from Vault (Count: %v, CallId: %v)",
            subrequests.size(),
            callId);

        auto body = MakeRequestBody(vaultTicket, subrequests);
        return HttpClient_->Post("/1/tokens", std::move(body))
            .Apply(BIND(
                &TDefaultSecretVaultService::OnVaultCallResult,
                MakeStrong(this),
                callId));
    }

    std::vector<TErrorOrSecretSubresponse> OnVaultCallResult(const TGuid& callId, const NHttp::IResponsePtr& rsp)
    {
        if (rsp->GetStatusCode() != EStatusCode::OK) {
            auto error = TError("Vault HTTP call returned an error")
                << TErrorAttribute("call_id", callId)
                << TErrorAttribute("status_code", rsp->GetStatusCode());
            LOG_DEBUG(error);
            THROW_ERROR(error);
        }

        IMapNodePtr rootNode;
        try {
            LOG_DEBUG("Started reading response body from Vault (CallId: %v)",
                callId);

            auto body = rsp->ReadAll();

            LOG_DEBUG("Finished reading response body from Vault (CallId: %v)",
                callId);

            TMemoryInput stream(body.Begin(), body.Size());
            auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
            ParseJson(&stream, builder.get());
            rootNode = builder->EndTree()->AsMap();
        } catch (const std::exception& ex) {
            auto error = TError(
                ESecretVaultErrorCode::MalformedResponse,
                "Error parsing Vault response")
                << TErrorAttribute("call_id", callId)
                << ex;
            LOG_DEBUG(error);
            THROW_ERROR(error);
        }

        auto responseError = GetErrorFromResponse(rootNode);
        THROW_ERROR_EXCEPTION_IF_FAILED(responseError);

        try {
            static const TString SecretsKey("secrets");
            auto secretsNode = rootNode->GetChild(SecretsKey)->AsList();

            std::vector<TErrorOrSecretSubresponse> subresponses;

            int successCount = 0;
            int errorCount = 0;
            for (const auto& secretNode : secretsNode->GetChildren()) {
                auto secretMapNode = secretNode->AsMap();
                auto subresponseError = GetErrorFromResponse(secretMapNode);
                if (!subresponseError.IsOK()) {
                    subresponses.push_back(subresponseError);
                    ++errorCount;
                    continue;
                }

                TSecretSubresponse subresponse;
                static const TString SecretsValueKey("value");
                auto valueNode = secretMapNode->GetChild(SecretsValueKey)->AsList();
                for (const auto& fieldNode : valueNode->GetChildren()) {
                    auto fieldMapNode = fieldNode->AsMap();
                    static const TString SecretKeyKey("key");
                    static const TString SecretValueKey("value");
                    subresponse.Payload.emplace(
                        fieldMapNode->GetChild(SecretKeyKey)->GetValue<TString>(),
                        fieldMapNode->GetChild(SecretsValueKey)->GetValue<TString>());
                }

                ++successCount;
                subresponses.push_back(subresponse);
            }

            LOG_DEBUG("Secrets retrieved from Vault (CallId: %v, SuccessCount: %v, ErrorCount: %v)",
                callId,
                successCount,
                errorCount);

            return subresponses;
        } catch (const std::exception& ex) {
            auto error = TError(
                ESecretVaultErrorCode::MalformedResponse,
                "Error parsing Vault response")
                << TErrorAttribute("call_id", callId)
                << ex;
            LOG_DEBUG(error);
            THROW_ERROR(error);
        }
    }

    static ESecretVaultErrorCode ParseErrorCode(TStringBuf codeString)
    {
        ESecretVaultErrorCode code;
        if (!TEnumTraits<ESecretVaultErrorCode>::FindValueByLiteral(codeString, &code)) {
            code = ESecretVaultErrorCode::UnknownError;
        }
        return code;
    }

    static TError GetErrorFromResponse(const IMapNodePtr& node)
    {
        static const TString StatusKey("status");
        static const TString OKValue("ok");
        auto statusString = node->GetChild(StatusKey)->GetValue<TString>();
        if (statusString == OKValue) {
            return {};
        }

        static const TString CodeKey("code");
        auto codeString = node->GetChild(CodeKey)->GetValue<TString>();
        auto code = ParseErrorCode(codeString);

        return TError(code, "Vault error")
            << TErrorAttribute("status", statusString);
    }

    TSharedRef MakeRequestBody(const TString& vaultTicket, const std::vector<TSecretSubrequest>& subrequests)
    {
        TString body;
        TStringOutput stream(body);
        auto jsonWriter = CreateJsonConsumer(&stream);
        BuildYsonFluently(jsonWriter.get())
            .BeginMap()
                .Item("tokenized_requests").DoListFor(subrequests, [&] (auto fluent, const auto& subrequest) {
                    fluent
                        .Item().BeginMap()
                            .Item("service_token").Value(vaultTicket)
                            .Item("token").Value(subrequest.DelegationToken)
                            .Item("signature").Value(subrequest.Signature)
                            .Item("secret_uuid").Value(subrequest.SecretId)
                            .Item("secret_version").Value(subrequest.SecretVersion)
                        .EndMap();
                })
            .EndMap();
        jsonWriter->Flush();
        return TSharedRef::FromString(std::move(body));
    }
};

ISecretVaultServicePtr CreateDefaultSecretVaultService(
    TDefaultSecretVaultServiceConfigPtr config,
    ITvmServicePtr tvmService,
    IPollerPtr poller)
{
    return New<TDefaultSecretVaultService>(
        std::move(config),
        std::move(tvmService),
        std::move(poller));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
