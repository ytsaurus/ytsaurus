#include "oauth_service.h"

#include "config.h"
#include "private.h"
#include "helpers.h"

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

    TFuture<TOAuthUserInfoResult> GetUserInfo(const TString& accessToken) override {
        return BIND(&TOAuthService::DoGetUserInfo, MakeStrong(this), accessToken)
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
    }

private:
    const TOAuthServiceConfigPtr Config_;
    const NHttp::IRetriableClientPtr HttpClient_;

    NProfiling::TCounter OAuthCalls_;
    NProfiling::TCounter OAuthCallErrors_;
    NProfiling::TCounter OAuthCallFatalErrors_;
    NProfiling::TEventTimer OAuthCallTime_;

private:
    static NJson::TJsonFormatConfigPtr MakeJsonFormatConfig()
    {
        auto config = New<NJson::TJsonFormatConfig>();
        config->EncodeUtf8 = false; // Hipsters use real Utf8.
        return config;
    }

    TOAuthUserInfoResult DoGetUserInfo(const TString& accessToken) {
        OAuthCalls_.Increment();
        NProfiling::TWallTimer timer;
        
        auto callId = TGuid::Create();
        auto httpHeaders = New<THeaders>();
        httpHeaders->Add("Authorization", "Bearer " + accessToken);

        auto jsonResponseChecker = CreateJsonResponseChecker(BIND([this] (const IResponsePtr& rsp, const NYTree::INodePtr& json) -> TError {
            if (rsp->GetStatusCode() != EStatusCode::OK) {
                return TError("OAuth call returned HTTP status code %v", static_cast<int>(rsp->GetStatusCode()));
            }

            if (json->GetType() != ENodeType::Map) {
                return TError("OAuth call has returned an improper result")
                    << TErrorAttribute("expected_result_type", ENodeType::Map)
                    << TErrorAttribute("actual_result_type", json->GetType());
            }

            auto loginNode = json->AsMap()->FindChild(Config_->UserInfoLoginField);
            if (!loginNode || loginNode->GetType() != ENodeType::String) {
                return TError("OAuth call didn't return login");
            }

            return {};
        }), MakeJsonFormatConfig());

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

        return TOAuthUserInfoResult{
            .Login = jsonResponseChecker->GetFormattedResponse()->AsMap()->FindChild(Config_->UserInfoLoginField)->AsString()->GetValue()
        };
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
