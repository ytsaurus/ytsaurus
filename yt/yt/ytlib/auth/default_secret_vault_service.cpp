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

namespace NYT::NAuth {

using namespace NConcurrency;
using namespace NJson;
using namespace NYTree;
using namespace NHttp;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESecretVaultResponseStatus,
    ((Unknown)  (0))
    ((OK)       (1))
    ((Warning)  (2))
    ((Error)    (3))
);

////////////////////////////////////////////////////////////////////////////////

class TDefaultSecretVaultService
    : public ISecretVaultService
{
public:
    TDefaultSecretVaultService(
        TDefaultSecretVaultServiceConfigPtr config,
        ITvmServicePtr tvmService,
        IPollerPtr poller,
        NProfiling::TRegistry profiler)
        : Config_(std::move(config))
        , TvmService_(std::move(tvmService))
        , HttpClient_(CreateHttpClient(std::move(poller)))
        , SubrequestsPerCallGauge_(profiler.Gauge("/subrequests_per_call"))
        , CallCountCounter_(profiler.Counter("/call_count"))
        , SubrequestCountCounter_(profiler.Counter("/subrequest_count"))
        , CallTimer_(profiler.Timer("/call_time"))
        , SuccessfulCallCountCounter_(profiler.Counter("/successful_call_count"))
        , FailedCallCountCounter_(profiler.Counter("/failed_call_count"))
        , SuccessfulSubrequestCountCounter_(profiler.Counter("/successful_subrequest_count"))
        , WarningSubrequestCountCounter_(profiler.Counter("/warning_subrequest_count"))
        , FailedSubrequestCountCounter_(profiler.Counter("/failed_subrequest_count"))
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

    NProfiling::TGauge SubrequestsPerCallGauge_;
    NProfiling::TCounter CallCountCounter_;
    NProfiling::TCounter SubrequestCountCounter_;
    NProfiling::TEventTimer CallTimer_;
    NProfiling::TCounter SuccessfulCallCountCounter_;
    NProfiling::TCounter FailedCallCountCounter_;
    NProfiling::TCounter SuccessfulSubrequestCountCounter_;
    NProfiling::TCounter WarningSubrequestCountCounter_;
    NProfiling::TCounter FailedSubrequestCountCounter_;

private:
    NHttp::IClientPtr CreateHttpClient(IPollerPtr poller) const
    {
        if (Config_->Secure) {
            return NHttps::CreateClient(Config_->HttpClient, std::move(poller));
        }
        return NHttp::CreateClient(Config_->HttpClient, std::move(poller));
    }

    TFuture<std::vector<TErrorOrSecretSubresponse>> OnTvmCallResult(const std::vector<TSecretSubrequest>& subrequests, const TString& vaultTicket)
    {
        auto callId = TGuid::Create();

        YT_LOG_DEBUG("Retrieving secrets from Vault (Count: %v, CallId: %v)",
            subrequests.size(),
            callId);

        CallCountCounter_.Increment();
        SubrequestCountCounter_.Increment(subrequests.size());
        SubrequestsPerCallGauge_.Update(subrequests.size());

        auto url = Format("%v://%v:%v/1/tokens/",
            Config_->Secure ? "https" : "http",
            Config_->Host,
            Config_->Port);
        if (!Config_->Consumer.empty()) {
            url += "?consumer=" + Config_->Consumer;
        }
        auto body = MakeRequestBody(vaultTicket, subrequests);
        static const auto Headers = MakeRequestHeaders();
        NProfiling::TWallTimer timer;
        return HttpClient_->Post(url, body, Headers)
            .WithTimeout(Config_->RequestTimeout)
            .Apply(BIND(
                &TDefaultSecretVaultService::OnVaultCallResult,
                MakeStrong(this),
                callId,
                timer));
    }

    std::vector<TErrorOrSecretSubresponse> OnVaultCallResult(
        TGuid callId,
        const NProfiling::TWallTimer& timer,
        const TErrorOr<NHttp::IResponsePtr>& rspOrError)
    {
        CallTimer_.Record(timer.GetElapsedTime());

        auto onError = [&] (TError error) {
            error.Attributes().Set("call_id", callId);
            FailedCallCountCounter_.Increment();
            YT_LOG_DEBUG(error);
            THROW_ERROR(error);
        };

        if (!rspOrError.IsOK()) {
            onError(TError("Vault call failed")
                << rspOrError);
        }

        const auto& rsp = rspOrError.Value();
        if (rsp->GetStatusCode() != EStatusCode::OK) {
            onError(TError("Vault call returned HTTP status code %v",
                static_cast<int>(rsp->GetStatusCode())));
        }

        IMapNodePtr rootNode;
        try {
            auto body = rsp->ReadAll();
            rootNode = ParseVaultResponse(body);
        } catch (const std::exception& ex) {
            onError(TError(
                ESecretVaultErrorCode::MalformedResponse,
                "Error parsing Vault response")
                << ex);
        }

        auto responseStatusString = GetStatusStringFromResponse(rootNode);
        auto responseStatus = ParseStatus(responseStatusString);
        if (responseStatus == ESecretVaultResponseStatus::Error) {
            onError(GetErrorFromResponse(rootNode, responseStatusString));
        }
        if (responseStatus != ESecretVaultResponseStatus::OK) {
            // NB! Vault API is not supposed to return other statuses (e.g. warning) at the top-level.
            onError(MakeUnexpectedStatusError(responseStatusString));
        }

        std::vector<TErrorOrSecretSubresponse> subresponses;
        try {
            auto secretsNode = rootNode->GetChildOrThrow("secrets")->AsList();

            int successCount = 0;
            int warningCount = 0;
            int errorCount = 0;
            auto secretNodes = secretsNode->GetChildren();
            for (size_t subresponseIndex = 0; subresponseIndex < secretNodes.size(); ++subresponseIndex) {
                auto secretMapNode = secretNodes[subresponseIndex]->AsMap();

                auto subresponseStatusString = GetStatusStringFromResponse(secretMapNode);
                auto subresponseStatus = ParseStatus(subresponseStatusString);
                if (subresponseStatus == ESecretVaultResponseStatus::OK) {
                    ++successCount;
                } else if (subresponseStatus == ESecretVaultResponseStatus::Warning) {
                    // NB! Warning status is supposed to contain valid data so we proceed parsing the response.
                    ++warningCount;
                    auto warningMessage = GetWarningMessageFromResponse(secretMapNode);
                    YT_LOG_DEBUG("Received warning status in subresponse from Vault (CallId: %v, SubresponseIndex: %v, WarningMessage: %Qv)",
                        callId,
                        subresponseIndex,
                        warningMessage);
                } else if (subresponseStatus == ESecretVaultResponseStatus::Error) {
                    subresponses.push_back(GetErrorFromResponse(
                        secretMapNode,
                        subresponseStatusString));
                    ++errorCount;
                    continue;
                } else {
                    subresponses.push_back(MakeUnexpectedStatusError(
                        subresponseStatusString));
                    ++errorCount;
                    continue;
                }

                TSecretSubresponse subresponse;
                auto valueNode = secretMapNode->GetChildOrThrow("value")->AsList();
                for (const auto& fieldNode : valueNode->GetChildren()) {
                    auto fieldMapNode = fieldNode->AsMap();
                    auto encodingNode = fieldMapNode->FindChild("encoding");
                    TString encoding = encodingNode ? encodingNode->GetValue<TString>() : "";
                    subresponse.Values.emplace_back(TSecretValue{
                        fieldMapNode->GetChildOrThrow("key")->GetValue<TString>(),
                        fieldMapNode->GetChildOrThrow("value")->GetValue<TString>(),
                        encoding});
                }

                subresponses.push_back(subresponse);
            }

            SuccessfulCallCountCounter_.Increment();
            SuccessfulSubrequestCountCounter_.Increment(successCount);
            WarningSubrequestCountCounter_.Increment(warningCount);
            FailedSubrequestCountCounter_.Increment(errorCount);

            YT_LOG_DEBUG("Secrets retrieved from Vault (CallId: %v, SuccessCount: %v, WarningCount: %v, ErrorCount: %v)",
                callId,
                successCount,
                warningCount,
                errorCount);
        } catch (const std::exception& ex) {
            onError(TError(
                ESecretVaultErrorCode::MalformedResponse,
                "Error parsing Vault response")
                << ex);
        }
        return subresponses;
    }

    IMapNodePtr ParseVaultResponse(const TSharedRef& body)
    {
        TMemoryInput stream(body.Begin(), body.Size());
        auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        auto jsonConfig = New<TJsonFormatConfig>();
        jsonConfig->EncodeUtf8 = false;
        ParseJson(&stream, builder.get(), jsonConfig);
        return builder->EndTree()->AsMap();
    }

    static ESecretVaultErrorCode ParseErrorCode(TStringBuf codeString)
    {
        // TODO(babenko): add link to doc
        if (codeString == "nonexistent_entity_error") {
            return ESecretVaultErrorCode::NonexistentEntityError;
        } else if (codeString == "delegation_access_error") {
            return ESecretVaultErrorCode::DelegationAccessError;
        } else if (codeString == "delegation_token_revoked") {
            return ESecretVaultErrorCode::DelegationTokenRevoked;
        } else {
            return ESecretVaultErrorCode::UnknownError;
        }
    }

    static TString GetStatusStringFromResponse(const IMapNodePtr& node)
    {
        static const TString StatusKey("status");
        return node->GetChildOrThrow(StatusKey)->GetValue<TString>();
    }

    static TError MakeUnexpectedStatusError(const TString& statusString)
    {
        return TError(
            ESecretVaultErrorCode::UnexpectedStatus,
            "Received unexpected status from Vault")
            << TErrorAttribute("status", statusString);
    }

    static ESecretVaultResponseStatus ParseStatus(const TString& statusString)
    {
        if (statusString == "ok") {
            return ESecretVaultResponseStatus::OK;
        } else if (statusString == "warning") {
            return ESecretVaultResponseStatus::Warning;
        } else if (statusString == "error") {
            return ESecretVaultResponseStatus::Error;
        } else {
            return ESecretVaultResponseStatus::Unknown;
        }
    }

    static TString GetWarningMessageFromResponse(const IMapNodePtr& node)
    {
        static const TString WarningMessageKey("warning_message");
        auto warningMessageNode = node->FindChild(WarningMessageKey);
        return warningMessageNode ? warningMessageNode->GetValue<TString>() : "Vault warning";
    }

    static TError GetErrorFromResponse(const IMapNodePtr& node, const TString& statusString)
    {
        static const TString CodeKey("code");
        auto codeString = node->GetChildOrThrow(CodeKey)->GetValue<TString>();
        auto code = ParseErrorCode(codeString);

        static const TString MessageKey("message");
        auto messageNode = node->FindChild(MessageKey);
        return TError(
            code,
            messageNode ? messageNode->GetValue<TString>() : "Vault error")
            << TErrorAttribute("status", statusString)
            << TErrorAttribute("code", codeString);
    }

    static THeadersPtr MakeRequestHeaders()
    {
        auto headers = New<THeaders>();
        headers->Add("Content-Type", "application/json");
        return headers;
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
                            .Item("service_ticket").Value(vaultTicket)
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
}; // TDefaultSecretVaultService

ISecretVaultServicePtr CreateDefaultSecretVaultService(
    TDefaultSecretVaultServiceConfigPtr config,
    ITvmServicePtr tvmService,
    IPollerPtr poller,
    NProfiling::TRegistry profiler)
{
    return New<TDefaultSecretVaultService>(
        std::move(config),
        std::move(tvmService),
        std::move(poller),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
