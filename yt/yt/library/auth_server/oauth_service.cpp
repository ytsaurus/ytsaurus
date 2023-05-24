#include "oauth_service.h"

#include "config.h"
#include "library/cpp/yt/string/string_builder.h"
#include "private.h"
#include "helpers.h"

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>
#include <yt/yt/core/json/json_parser.h>
#include <yt/yt/core/json/public.h>
#include <yt/yt/core/profiling/timing.h>

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
        , HttpClient_(Config_->Secure
            ? NHttps::CreateClient(Config_->HttpClient, std::move(poller))
            : NHttp::CreateClient(Config_->HttpClient, std::move(poller)))
        , OAuthCalls_(profiler.Counter("/oauth_calls"))
        , OAuthCallErrors_(profiler.Counter("/oauth_call_errors"))
        , OAuthCallFatalErrors_(profiler.Counter("/oauth_call_fatal_errors"))
        , OAuthCallTime_(profiler.Timer("/oauth_call_time"))
    { }



private:
    const TOAuthServiceConfigPtr Config_;
    const NHttp::IClientPtr HttpClient_;

    NProfiling::TCounter OAuthCalls_;
    NProfiling::TCounter OAuthCallErrors_;
    NProfiling::TCounter OAuthCallFatalErrors_;
    NProfiling::TEventTimer OAuthCallTime_;

private:
    TFuture<TOAuthUserInfoResult> GetUserInfo(const TString& accessToken) {
        auto deadline = TInstant::Now() + Config_->RequestTimeout;
        auto callId = TGuid::Create();

        // TSafeUrlBuilder builder;
        const auto url = Format("%v://%v:%v/%v?",
            Config_->Secure ? "https" : "http",
            Config_->Host,
            Config_->Port,
            Config_->UserInfoEndpoint);

        auto httpHeaders = New<THeaders>();
        httpHeaders->Add("Authorization", "Bearer " + accessToken);

        std::vector<TError> accumulatedErrors;
        for (int attempt = 1; TInstant::Now() < deadline || attempt == 1; ++attempt) {
            INodePtr result;
            try {
                OAuthCalls_.Increment();
                result = DoGetCall(
                    callId,
                    attempt,
                    url,
                    httpHeaders,
                    deadline);
            } catch (const std::exception& ex) {
                OAuthCallErrors_.Increment();
                YT_LOG_WARNING(
                    ex,
                    "OAuth call attempt failed, backing off (CallId: %v, Attempt: %v)",
                    callId,
                    attempt);
                auto error = TError("OAuth call attempt %v failed", attempt)
                    << ex
                    << TErrorAttribute("call_id", callId)
                    << TErrorAttribute("attempt", attempt);
                accumulatedErrors.push_back(std::move(error));
            }

            // Check for known exceptions to retry.
            if (result) {
                auto loginNode = result->AsMap()->FindChild(Config_->UserInfoLoginField);
                if (!loginNode || loginNode->GetType() != ENodeType::String) {
                    auto userInfoResult = TOAuthUserInfoResult{
                        .Login = loginNode->AsString()->GetValue()
                    };
                    return MakeFuture(userInfoResult);
                }

                YT_LOG_WARNING("OAuth didn't return login, backing off (CallId: %v, Attempt: %v)",
                    callId,
                    attempt);
            }

            auto now = TInstant::Now();
            if (now > deadline) {
                break;
            }

            TDelayedExecutor::WaitForDuration(std::min(Config_->BackoffTimeout, deadline - now));
        }

        OAuthCallFatalErrors_.Increment();
        THROW_ERROR_EXCEPTION("OAuth call failed")
            << std::move(accumulatedErrors)
            << TErrorAttribute("call_id", callId);
    }

    static NJson::TJsonFormatConfigPtr MakeJsonFormatConfig()
    {
        auto config = New<NJson::TJsonFormatConfig>();
        config->EncodeUtf8 = false; // Hipsters use real Utf8.
        return config;
    }

    INodePtr DoGetCall(
        TGuid callId,
        int attempt,
        const TString& url,
        const THeadersPtr& headers,
        TInstant deadline)
    {
        auto onError = [&] (TError error) {
            error.MutableAttributes()->Set("call_id", callId);
            YT_LOG_DEBUG(error);
            THROW_ERROR(error);
        };

        NProfiling::TWallTimer timer;
        auto timeout = std::min(deadline - TInstant::Now(), Config_->AttemptTimeout);

        YT_LOG_DEBUG("Calling OAuth (Url: %v, CallId: %v, Attempt: %v, Timeout: %v)",
            url,
            callId,
            attempt,
            timeout);

        auto rspOrError = WaitFor(HttpClient_->Get(url, headers));
        if (!rspOrError.IsOK()) {
            onError(TError("OAuth call failed")
                << rspOrError);
        }

        const auto& rsp = rspOrError.Value();
        if (rsp->GetStatusCode() != EStatusCode::OK) {
            onError(TError("OAuth call returned HTTP status code %v",
                static_cast<int>(rsp->GetStatusCode())));
        }

        INodePtr rootNode;
        try {

            YT_LOG_DEBUG("Started reading response body from OAuth (CallId: %v, Attempt: %v)",
                callId,
                attempt);

            auto body = rsp->ReadAll();

            YT_LOG_DEBUG("Finished reading response body from OAuth (CallId: %v, Attempt: %v)\n%v",
                callId,
                attempt,
                body);

            TMemoryInput stream(body.Begin(), body.Size());
            auto factory = NYTree::CreateEphemeralNodeFactory();
            auto builder = NYTree::CreateBuilderFromFactory(factory.get());
            static const auto Config = MakeJsonFormatConfig();
            NJson::ParseJson(&stream, builder.get(), Config);
            rootNode = builder->EndTree();

            OAuthCallTime_.Record(timer.GetElapsedTime());
            YT_LOG_DEBUG("Parsed OAuth daemon reply (CallId: %v, Attempt: %v)",
                callId,
                attempt);
        } catch (const std::exception& ex) {
            onError(TError(
                "Error parsing OAuth response")
                << ex);
        }

        if (rootNode->GetType() != ENodeType::Map) {
            THROW_ERROR_EXCEPTION("OAuth has returned an improper result")
                << TErrorAttribute("expected_result_type", ENodeType::Map)
                << TErrorAttribute("actual_result_type", rootNode->GetType());
        }

        return rootNode;
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
