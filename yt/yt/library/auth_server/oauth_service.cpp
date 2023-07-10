#include "oauth_service.h"

#include "config.h"
#include "library/cpp/yt/logging/logger.h"
#include "private.h"
#include "helpers.h"
#include "yt/yt/core/ytree/public.h"

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/public.h>
#include <yt/yt/core/http/retriable_client.h>
#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/json/json_parser.h>
#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NAuth {

using namespace NConcurrency;
using namespace NHttp;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TOAuthService
    : public IOAuthService
{
public:
    TOAuthService(
        TOAuthServiceConfigPtr config,
        IPollerPtr poller,
        NProfiling::TProfiler profiler)
        : Config_(std::move(config))
        , HttpClient_(
            CreateRetrieableClient(
                Config_->RetriableClient,
                Config_->Secure
                    ? NHttps::CreateClient(Config_->HttpClient, poller)
                    : NHttp::CreateClient(Config_->HttpClient, poller),
                poller->GetInvoker()))
        , OAuthCalls_(profiler.Counter("/oauth_calls"))
        , OAuthCallErrors_(profiler.Counter("/oauth_call_errors"))
        , OAuthCallTime_(profiler.Timer("/oauth_call_time"))
    { }

    TFuture<TOAuthUserInfoResult> GetUserInfo(const TString& accessToken) override
    {
        return BIND(&TOAuthService::DoGetUserInfo, MakeStrong(this), accessToken)
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
    }

private:
    const TOAuthServiceConfigPtr Config_;
    const NHttp::IRetriableClientPtr HttpClient_;

    NProfiling::TCounter OAuthCalls_;
    NProfiling::TCounter OAuthCallErrors_;
    NProfiling::TEventTimer OAuthCallTime_;

private:
    static NJson::TJsonFormatConfigPtr MakeJsonFormatConfig()
    {
        auto config = New<NJson::TJsonFormatConfig>();
        config->EncodeUtf8 = false; // Additional string conversion is not necessary in this case
        return config;
    }

    TOAuthUserInfoResult DoGetUserInfo(const TString& accessToken)
    {
        OAuthCalls_.Increment();
        NProfiling::TWallTimer timer;
        
        auto callId = TGuid::Create();
        auto httpHeaders = New<THeaders>();
        httpHeaders->Add("Authorization", "Bearer " + accessToken);

        auto jsonResponseChecker = CreateJsonResponseChecker(
            BIND(&TOAuthService::DoCheckUserInfoResponse, MakeStrong(this)),
            MakeJsonFormatConfig());

        const auto url = Format("%v://%v:%v/%v",
            Config_->Secure ? "https" : "http",
            Config_->Host,
            Config_->Port,
            Config_->UserInfoEndpoint);
        
        YT_LOG_DEBUG("Calling OAuth get user info (Url: %v, CallId: %v)", NHttp::SanitizeUrl(url), callId);

        auto result = WaitFor(HttpClient_->Get(jsonResponseChecker, url, httpHeaders));
        OAuthCallTime_.Record(timer.GetElapsedTime());
        if (!result.IsOK()) {
            OAuthCallErrors_.Increment();
            auto error = TError("OAuth call failed")
                << result
                << TErrorAttribute("call_id", callId);
            YT_LOG_WARNING(error);
            THROW_ERROR(error);
        }

        const auto& formattedResponose = jsonResponseChecker->GetFormattedResponse()->AsMap();
        auto userInfo = TOAuthUserInfoResult{
            .Login = formattedResponose->GetChildValueOrThrow<TString>(Config_->UserInfoLoginField)
        };

        if (Config_->UserInfoSubjectField) {
            userInfo.Subject = formattedResponose->GetChildValueOrThrow<TString>(*Config_->UserInfoSubjectField);
        }

        return userInfo;
    }

    TError DoCheckUserInfoResponse(const IResponsePtr& rsp, const NYTree::INodePtr& rspNode) const
    {
        if (rsp->GetStatusCode() != EStatusCode::OK) {
            auto error = TError("OAuth response has non-ok status code: %v", static_cast<int>(rsp->GetStatusCode()));

            if (rspNode->GetType() == ENodeType::Map && Config_->UserInfoErrorField) {
                auto errorNode = rspNode->AsMap()->FindChild(*Config_->UserInfoErrorField);
                if (errorNode && errorNode->GetType() == ENodeType::String) {
                    error = error << TError(errorNode->AsString()->GetValue());
                }
                YT_LOG_WARNING("OAuth response has non-ok status code, but no error message found (Status Code: %v, Error field: %v)",
                    static_cast<int>(rsp->GetStatusCode()),
                    *Config_->UserInfoErrorField);
            }

            return error;
        }

        if (rspNode->GetType() != ENodeType::Map) {
            return TError("OAuth response content has unexpected type")
                << TErrorAttribute("expected_result_type", ENodeType::Map)
                << TErrorAttribute("actual_result_type", rspNode->GetType());
        }

        auto loginNode = rspNode->AsMap()->FindChild(Config_->UserInfoLoginField);
        if (!loginNode || loginNode->GetType() != ENodeType::String) {
            return TError("OAuth response content has no login field")
                << TErrorAttribute("login_field", Config_->UserInfoLoginField);
        }

        if (Config_->UserInfoSubjectField) {
            auto subjectNode = rspNode->AsMap()->FindChild(*Config_->UserInfoSubjectField);
            if (!subjectNode || subjectNode->GetType() != ENodeType::String) {
                return TError("OAuth response content has no subject field")
                    << TErrorAttribute("subject_field", Config_->UserInfoSubjectField);
            }
        }

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

IOAuthServicePtr CreateOAuthService(
    TOAuthServiceConfigPtr config,
    NConcurrency::IPollerPtr poller,
    NProfiling::TProfiler profiler)
{
    return New<TOAuthService>(
        std::move(config),
        std::move(poller),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

}
