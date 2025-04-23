#include "default_secret_vault_service.h"
#include "secret_vault_service.h"

#include "config.h"
#include "private.h"

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>

#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>

#include <yt/yt/core/json/json_parser.h>
#include <yt/yt/core/json/json_writer.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/tree_builder.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <library/cpp/uri/encode.h>

namespace NYT::NAuth {

using namespace NConcurrency;
using namespace NHttp;
using namespace NJson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = AuthLogger;

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
        std::vector<ITvmServicePtr> tvmServices,
        IPollerPtr poller,
        NProfiling::TProfiler profiler)
        : Config_(std::move(config))
        , TvmServices_(BuildTvmServiceMap(std::move(tvmServices)))
        , DefaultTvmIdForNewTokens_(DeduceDefaultTvmId(Config_->DefaultTvmIdForNewTokens))
        , DefaultTvmIdForExistingTokens_(DeduceDefaultTvmId(Config_->DefaultTvmIdForExistingTokens))
        , HttpClient_(Config_->Secure
                      ? NHttps::CreateClient(Config_->HttpClient, std::move(poller))
                      : NHttp::CreateClient(Config_->HttpClient, std::move(poller)))
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

    TFuture<std::vector<TErrorOrSecretSubresponse>> GetSecrets(
        const std::vector<TSecretSubrequest>& subrequests) override
    {
        return BIND(&TDefaultSecretVaultService::DoGetSecrets, MakeStrong(this), subrequests)
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
    }

    TFuture<TDelegationTokenResponse> GetDelegationToken(TDelegationTokenRequest request) override
    {
        if (request.Signature.empty() || request.SecretId.empty() || request.UserTicket.empty()) {
            return MakeFuture<TDelegationTokenResponse>(TError(
                "Invalid call for delegation token with signature %Qv, secret id %Qv "
                "and user ticket length %v",
                request.Signature,
                request.SecretId,
                request.UserTicket.size()));
        }

        return BIND(&TDefaultSecretVaultService::DoGetDelegationToken,
            MakeStrong(this),
            std::move(request))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
    }

    void RevokeDelegationToken(TRevokeDelegationTokenRequest request) override
    {
        if (request.DelegationToken.empty()
            || request.Signature.empty()
            || request.SecretId.empty())
        {
            YT_LOG_WARNING(
                "Invalid call to revoke delegation token with signature %Qv, secret id %Qv",
                request.Signature,
                request.SecretId);
            return;
        }

        const auto callId = TGuid::Create();

        YT_LOG_DEBUG(
            "Revoking delegation token from Vault "
            "(SecretId: %v, Signature: %v, CallId: %v)",
            request.SecretId,
            request.Signature,
            callId);

        CallCountCounter_.Increment();

        if (!Config_->EnableRevocation) {
            return;
        }

        try {
            const auto url = MakeRequestUrl("/1/tokens/revoke", false);
            const auto headers = New<THeaders>();
            headers->Add("Content-Type", "application/json");
            const auto body = MakeRevokeDelegationTokenRequestBody(request);

            const auto responseBody = HttpPost(url, body, headers);
            const auto response = ParseVaultResponse(responseBody);

            auto responseStatusString = GetStatusStringFromResponse(response);
            auto responseStatus = ParseStatus(responseStatusString);
            if (responseStatus == ESecretVaultResponseStatus::Error) {
                THROW_ERROR GetErrorFromResponse(response, responseStatusString);
            }
            if (responseStatus == ESecretVaultResponseStatus::Unknown) {
                THROW_ERROR MakeUnexpectedStatusError(responseStatusString);
            }
            if (responseStatus == ESecretVaultResponseStatus::Warning) {
                WarningSubrequestCountCounter_.Increment();
                YT_LOG_WARNING("Received warning message from Vault: %v",
                    GetWarningMessageFromResponse(response));
            }

            auto resultsNode = response->GetChildOrThrow("result")->AsList();
            auto resultNodes = resultsNode->GetChildren();
            THROW_ERROR_EXCEPTION_UNLESS(resultNodes.size() == 1,
                "Unexpected number of results from Vault: %v", resultNodes.size());
            auto resultNode = resultNodes[0]->AsMap();

            responseStatusString = GetStatusStringFromResponse(resultNode);
            responseStatus = ParseStatus(responseStatusString);
            if (responseStatus == ESecretVaultResponseStatus::Error) {
                THROW_ERROR GetErrorFromResponse(response, responseStatusString);
            }
            if (responseStatus == ESecretVaultResponseStatus::Unknown) {
                THROW_ERROR MakeUnexpectedStatusError(responseStatusString);
            }
            if (responseStatus == ESecretVaultResponseStatus::Warning) {
                WarningSubrequestCountCounter_.Increment();
                YT_LOG_WARNING("Received warning message from Vault: %v",
                    GetWarningMessageFromResponse(response));
            }
            YT_LOG_DEBUG(
                "Revoked delegation token from Vault "
                "(SecretId: %v, Signature: %v, CallId: %v)",
                request.SecretId,
                request.Signature,
                callId);
        } catch (const std::exception& ex) {
            FailedCallCountCounter_.Increment();
            YT_LOG_WARNING(ex,
                "Failed to revoke delegation token from Vault "
                "(SecretId: %v, Signature: %v, CallId: %v)",
                request.SecretId,
                request.Signature,
                callId);
        }
    }

private:
    const TDefaultSecretVaultServiceConfigPtr Config_;

    using TTvmServiceMap = THashMap<TTvmId, ITvmServicePtr>;
    const TTvmServiceMap TvmServices_;

    TTvmId DefaultTvmIdForNewTokens_;
    TTvmId DefaultTvmIdForExistingTokens_;

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
    std::vector<TErrorOrSecretSubresponse> DoGetSecrets(
        const std::vector<TSecretSubrequest>& subrequests)
    {
        const auto callId = TGuid::Create();

        YT_LOG_DEBUG("Retrieving secrets from Vault (Count: %v, CallId: %v)",
            subrequests.size(),
            callId);

        CallCountCounter_.Increment();
        SubrequestCountCounter_.Increment(subrequests.size());
        SubrequestsPerCallGauge_.Update(subrequests.size());

        try {
            const auto url = MakeRequestUrl("/1/tokens/", true);
            const auto headers = New<THeaders>();
            headers->Add("Content-Type", "application/json");

            const auto body = MakeGetSecretsRequestBody(subrequests);

            const auto responseBody = HttpPost(url, body, headers);
            const auto response = ParseVaultResponse(responseBody);

            auto responseStatusString = GetStatusStringFromResponse(response);
            auto responseStatus = ParseStatus(responseStatusString);
            if (responseStatus == ESecretVaultResponseStatus::Error) {
                THROW_ERROR GetErrorFromResponse(response, responseStatusString);
            }
            if (responseStatus != ESecretVaultResponseStatus::OK) {
                // NB! Vault API is not supposed to return other statuses (e.g. warning) at the top-level.
                THROW_ERROR MakeUnexpectedStatusError(responseStatusString);
            }

            std::vector<TErrorOrSecretSubresponse> subresponses;

            auto secretsNode = response->GetChildOrThrow("secrets")->AsList();

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
                    YT_LOG_DEBUG(
                        "Received warning status in subresponse from Vault "
                        "(CallId: %v, SubresponseIndex: %v, WarningMessage: %v)",
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
                    subresponses.push_back(MakeUnexpectedStatusError(subresponseStatusString));
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
                        fieldMapNode->GetChildValueOrThrow<TString>("key"),
                        fieldMapNode->GetChildValueOrThrow<TString>("value"),
                        encoding});
                }

                subresponses.push_back(subresponse);
            }

            SuccessfulCallCountCounter_.Increment();
            SuccessfulSubrequestCountCounter_.Increment(successCount);
            WarningSubrequestCountCounter_.Increment(warningCount);
            FailedSubrequestCountCounter_.Increment(errorCount);

            YT_LOG_DEBUG(
                "Secrets retrieved from Vault "
                "(CallId: %v, SuccessCount: %v, WarningCount: %v, ErrorCount: %v)",
                callId,
                successCount,
                warningCount,
                errorCount);
            return subresponses;
        } catch (const std::exception& ex) {
            FailedCallCountCounter_.Increment();
            auto error = TError("Failed to get secrets from Vault")
                << ex
                << TErrorAttribute("call_id", callId);
            YT_LOG_DEBUG(error);
            THROW_ERROR error;
        }
    }

    TDelegationTokenResponse DoGetDelegationToken(TDelegationTokenRequest request)
    {
        const auto callId = TGuid::Create();

        YT_LOG_DEBUG(
            "Retrieving delegation token from Vault "
            "(SecretId: %v, Signature: %v, UserTicket: %v, CallId: %v)",
            request.SecretId,
            request.Signature, // signatures are not secret; tokens are
            RemoveTicketSignature(request.UserTicket),
            callId);

        CallCountCounter_.Increment();

        try {
            const auto url = MakeRequestUrl(Format("/1/secrets/%v/tokens/", request.SecretId), false);
            const auto headers = New<THeaders>();
            const auto& tvmService = GetTvmService(request.TvmId, DefaultTvmIdForNewTokens_);
            const auto vaultTicket = tvmService->GetServiceTicket(Config_->VaultServiceId);
            headers->Add("Content-Type", "application/json");
            // TODO(babenko): migrate to std::string
            headers->Add(NHeaders::UserTicketHeaderName, TString(request.UserTicket));
            // TODO(babenko): migrate to std::string
            headers->Add(NHeaders::ServiceTicketHeaderName, TString(vaultTicket));
            const auto body = MakeGetDelegationTokenRequestBody(request);

            const auto responseBody = HttpPost(url, body, headers);
            const auto response = ParseVaultResponse(responseBody);

            auto responseStatusString = GetStatusStringFromResponse(response);
            auto responseStatus = ParseStatus(responseStatusString);
            if (responseStatus == ESecretVaultResponseStatus::Error) {
                THROW_ERROR GetErrorFromResponse(response, responseStatusString);
            }
            if (responseStatus == ESecretVaultResponseStatus::Unknown) {
                THROW_ERROR MakeUnexpectedStatusError(responseStatusString);
            }
            if (responseStatus == ESecretVaultResponseStatus::Warning) {
                WarningSubrequestCountCounter_.Increment();
                YT_LOG_WARNING("Received warning message from Vault: %v",
                    GetWarningMessageFromResponse(response));
            }

            return TDelegationTokenResponse{
                .Token = response->GetChildValueOrThrow<TString>("token"),
                .TvmId = tvmService->GetSelfTvmId(),
            };
        } catch (const std::exception& ex) {
            FailedCallCountCounter_.Increment();
            auto error = TError("Failed to get delegation token from Vault")
                << ex
                << TErrorAttribute("call_id", callId);
            YT_LOG_DEBUG(error);
            THROW_ERROR error;
        }
    }

    TString MakeRequestUrl(TStringBuf path, bool addConsumer) const
    {
        auto url = Format("%v://%v:%v%v",
            Config_->Secure ? "https" : "http",
            Config_->Host,
            Config_->Port,
            path);
        if (addConsumer && !Config_->Consumer.empty()) {
            url = Format("%v?consumer=%v", url, Config_->Consumer);
        }
        return url;
    }

    TSharedRef MakeGetSecretsRequestBody(
        const std::vector<TSecretSubrequest>& subrequests)
    {
        TString body;
        TStringOutput stream(body);
        auto jsonWriter = CreateJsonConsumer(&stream);
        BuildYsonFluently(jsonWriter.get())
            .BeginMap()
                .Item("tokenized_requests").DoListFor(subrequests,
                    [&] (auto fluent, const auto& subrequest) {
                        auto map = fluent.Item().BeginMap();
                        const auto& tvmService = GetTvmService(
                            subrequest.TvmId,
                            DefaultTvmIdForExistingTokens_);
                        const auto vaultTicket =
                            tvmService->GetServiceTicket(Config_->VaultServiceId);
                        map.Item("service_ticket").Value(vaultTicket);
                        if (!subrequest.DelegationToken.empty()) {
                            map.Item("token").Value(subrequest.DelegationToken);
                        }
                        if (!subrequest.Signature.empty()) {
                            map.Item("signature").Value(subrequest.Signature);
                        }
                        if (!subrequest.SecretId.empty()) {
                            map.Item("secret_uuid").Value(subrequest.SecretId);
                        }
                        if (!subrequest.SecretVersion.empty()) {
                            map.Item("secret_version").Value(subrequest.SecretVersion);
                        }
                        map.EndMap();
                    })
            .EndMap();
        jsonWriter->Flush();
        return TSharedRef::FromString(std::move(body));
    }

    TSharedRef MakeGetDelegationTokenRequestBody(const TDelegationTokenRequest& request)
    {
        TString body;
        TStringOutput stream(body);
        auto jsonWriter = CreateJsonConsumer(&stream);
        BuildYsonFluently(jsonWriter.get())
            .BeginMap()
                .Item("signature").Value(request.Signature)
                .Item("tvm_client_id").Value(request.TvmId.value_or(DefaultTvmIdForNewTokens_))
                .DoIf(!request.Comment.empty(),
                    [&] (auto fluent) {
                        fluent.Item("comment").Value(request.Comment);
                    })
            .EndMap();
        jsonWriter->Flush();
        return TSharedRef::FromString(std::move(body));
    }

    TSharedRef MakeRevokeDelegationTokenRequestBody(const TRevokeDelegationTokenRequest& request)
    {
        TString body;
        TStringOutput stream(body);
        auto jsonWriter = CreateJsonConsumer(&stream);
        const auto& tvmService = GetTvmService(request.TvmId, DefaultTvmIdForExistingTokens_);
        const auto vaultTicket = tvmService->GetServiceTicket(Config_->VaultServiceId);
        BuildYsonFluently(jsonWriter.get())
            .BeginMap()
                .Item("tokenized_requests").BeginList()
                    .Item().BeginMap()
                        .Item("token").Value(request.DelegationToken)
                        .Item("signature").Value(request.Signature)
                        .Item("secret_uuid").Value(request.SecretId)
                        .Item("service_ticket").Value(vaultTicket)
                    .EndMap()
                .EndList()
            .EndMap();
        jsonWriter->Flush();
        return TSharedRef::FromString(std::move(body));
    }

    TSharedRef HttpPost(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers)
    {
        NProfiling::TWallTimer timer;
        auto rspOrError = WaitFor(HttpClient_->Post(url, body, headers)
            .WithTimeout(Config_->RequestTimeout));
        CallTimer_.Record(timer.GetElapsedTime());

        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Vault call failed");

        const auto& rsp = rspOrError.Value();
        if (rsp->GetStatusCode() != EStatusCode::OK) {
            THROW_ERROR_EXCEPTION("Vault call returned HTTP status code %v, response %v",
                static_cast<int>(rsp->GetStatusCode()),
                rsp->ReadAll());
        }

        return rsp->ReadAll();
    }

    static IMapNodePtr ParseVaultResponse(const TSharedRef& body)
    {
        try {
            TMemoryInput stream(body.Begin(), body.Size());
            auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
            auto jsonConfig = New<TJsonFormatConfig>();
            jsonConfig->EncodeUtf8 = false;
            ParseJson(&stream, builder.get(), jsonConfig);
            return builder->EndTree()->AsMap();
        } catch (const std::exception& ex) {
            THROW_ERROR TError(ESecretVaultErrorCode::MalformedResponse,
                "Error parsing Vault response");
        }
    }

    static TString GetStatusStringFromResponse(const IMapNodePtr& node)
    {
        return node->GetChildValueOrThrow<TString>("status");
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

    static TError GetErrorFromResponse(const IMapNodePtr& node, const TString& statusString)
    {
        auto codeString = node->GetChildValueOrThrow<TString>("code");
        auto code = ParseErrorCode(codeString);

        auto messageNode = node->FindChild("message");
        return TError(
            code,
            messageNode
                ? messageNode->GetValue<TString>()
                : "Vault error",
            TError::DisableFormat)
            << TErrorAttribute("status", statusString)
            << TErrorAttribute("code", codeString);
    }

    static ESecretVaultErrorCode ParseErrorCode(TStringBuf codeString)
    {
        // https://vault-api.passport.yandex.net/docs/#api
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

    static TError MakeUnexpectedStatusError(const TString& statusString)
    {
        return TError(
            ESecretVaultErrorCode::UnexpectedStatus,
            "Received unexpected status from Vault")
            << TErrorAttribute("status", statusString);
    }

    static TString GetWarningMessageFromResponse(const IMapNodePtr& node)
    {
        auto warningMessageNode = node->FindChild("warning_message");
        return warningMessageNode ? warningMessageNode->GetValue<TString>() : "Vault warning";
    }

    static TTvmServiceMap BuildTvmServiceMap(std::vector<ITvmServicePtr> tvmServices)
    {
        TTvmServiceMap result;
        for (auto& tvmService : tvmServices) {
            TTvmId id = tvmService->GetSelfTvmId();
            auto [_, inserted] = result.try_emplace(id, std::move(tvmService));
            THROW_ERROR_EXCEPTION_UNLESS(inserted,
                "Multiple TVM services binding to same id %v",
                id);
        }
        return result;
    }

    TTvmId DeduceDefaultTvmId(std::optional<TTvmId> configuredTvmId) const
    {
        if (configuredTvmId.has_value()) {
            THROW_ERROR_EXCEPTION_UNLESS(TvmServices_.contains(configuredTvmId.value()),
                "Requested TVM id %v not provided",
                configuredTvmId.value());
            return configuredTvmId.value();
        } else {
            THROW_ERROR_EXCEPTION_UNLESS(TvmServices_.size() == 1,
                "Cannot deduce which TVM service to use out of %v",
                TvmServices_.size());
            return TvmServices_.begin()->first;
        }
    }

    ITvmServicePtr GetTvmService(std::optional<TTvmId> requestedTvmId, TTvmId defaultTvmId) const
    {
        TTvmId effectiveTvmId = requestedTvmId.value_or(defaultTvmId);
        auto it = TvmServices_.find(effectiveTvmId);
        THROW_ERROR_EXCEPTION_IF(it == TvmServices_.end(),
                "Requested TVM id %v not provided",
                effectiveTvmId);
        return it->second;
    }
}; // TDefaultSecretVaultService

ISecretVaultServicePtr CreateDefaultSecretVaultService(
    TDefaultSecretVaultServiceConfigPtr config,
    std::vector<ITvmServicePtr> tvmServices,
    IPollerPtr poller,
    NProfiling::TProfiler profiler)
{
    return New<TDefaultSecretVaultService>(
        std::move(config),
        std::move(tvmServices),
        std::move(poller),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
