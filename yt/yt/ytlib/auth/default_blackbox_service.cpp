#include "default_blackbox_service.h"
#include "blackbox_service.h"
#include "config.h"
#include "helpers.h"
#include "private.h"
#include "tvm_service.h"

#include <yt/core/json/json_parser.h>

#include <yt/core/https/client.h>

#include <yt/core/http/client.h>
#include <yt/core/http/http.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/core/concurrency/delayed_executor.h>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NHttp;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TDefaultBlackboxService
    : public IBlackboxService
{
public:
    TDefaultBlackboxService(
        TDefaultBlackboxServiceConfigPtr config,
        ITvmServicePtr tvmService,
        IPollerPtr poller,
        NProfiling::TProfiler profiler)
        : Config_(std::move(config))
        , TvmService_(Config_->UseTvm ? std::move(tvmService) : nullptr)
        , Profiler_(std::move(profiler))
        , HttpClient_(Config_->Secure
            ? NHttps::CreateClient(Config_->HttpClient, std::move(poller))
            : NHttp::CreateClient(Config_->HttpClient, std::move(poller)))
    { }

    virtual TFuture<INodePtr> Call(
        const TString& method,
        const THashMap<TString, TString>& params) override
    {
        return BIND(&TDefaultBlackboxService::DoCall, MakeStrong(this), method, params)
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
    }

    virtual TErrorOr<TString> GetLogin(const NYTree::INodePtr& reply) const override
    {
        if (Config_->UseLowercaseLogin) {
            return GetByYPath<TString>(reply, "/attributes/1008");
        } else {
            return GetByYPath<TString>(reply, "/login");
        }
    }

private:
    const TDefaultBlackboxServiceConfigPtr Config_;
    const ITvmServicePtr TvmService_;
    const NProfiling::TProfiler Profiler_;

    const NHttp::IClientPtr HttpClient_;

    NProfiling::TMonotonicCounter BlackboxCalls_{"/blackbox_calls"};
    NProfiling::TMonotonicCounter BlackboxCallErrors_{"/blackbox_call_errors"};
    NProfiling::TMonotonicCounter BlackboxCallFatalErrors_{"/blackbox_call_fatal_errors"};
    NProfiling::TAggregateGauge BlackboxCallTime_{"/blackbox_call_time", {}, NProfiling::EAggregateMode::All};

private:
    INodePtr DoCall(
        const TString& method,
        const THashMap<TString, TString>& params)
    {
        auto deadline = TInstant::Now() + Config_->RequestTimeout;

        TString blackboxTicket;
        if (TvmService_) {
            auto rspOrError = WaitFor(TvmService_->GetTicket(Config_->BlackboxServiceId));
            if (!rspOrError.IsOK()) {
                AuthProfiler.Increment(BlackboxCallFatalErrors_);
                YT_LOG_ERROR(rspOrError);
                THROW_ERROR_EXCEPTION("TVM call failed") << rspOrError;
            }
            blackboxTicket = rspOrError.Value();
        }

        TSafeUrlBuilder builder;
        builder.AppendString(Format("%v://%v:%v/blackbox?",
            Config_->Secure ? "https" : "http",
            Config_->Host,
            Config_->Port));
        builder.AppendParam(AsStringBuf("method"), method);
        for (const auto& param : params) {
            builder.AppendChar('&');
            builder.AppendParam(param.first, param.second);
        }
        builder.AppendChar('&');
        builder.AppendParam("attributes", "1008");
        builder.AppendChar('&');
        builder.AppendParam("format", "json");

        auto realUrl = builder.FlushRealUrl();
        auto safeUrl = builder.FlushSafeUrl();

        auto httpHeaders = New<THeaders>();
        if (!blackboxTicket.empty()) {
            httpHeaders->Add("X-Ya-Service-Ticket", blackboxTicket);
        }

        auto callId = TGuid::Create();

        std::vector<TError> accumulatedErrors;

        for (int attempt = 1; TInstant::Now() < deadline || attempt == 1; ++attempt) {
            INodePtr result;
            try {
                AuthProfiler.Increment(BlackboxCalls_);
                result = DoCallOnce(
                    callId,
                    attempt,
                    realUrl,
                    safeUrl,
                    httpHeaders,
                    deadline);
            } catch (const std::exception& ex) {
                AuthProfiler.Increment(BlackboxCallErrors_);
                YT_LOG_WARNING(
                    ex,
                    "Blackbox call attempt failed, backing off (CallId: %v, Attempt: %v)",
                    callId,
                    attempt);
                auto error = TError("Blackbox call attempt %v failed", attempt)
                    << ex
                    << TErrorAttribute("call_id", callId)
                    << TErrorAttribute("attempt", attempt);
                accumulatedErrors.push_back(std::move(error));
            }

            // Check for known exceptions to retry.
            if (result) {
                auto exceptionNode = result->AsMap()->FindChild("exception");
                if (!exceptionNode || exceptionNode->GetType() != ENodeType::Map) {
                    // No exception information, go as-is.
                    return result;
                }

                auto exceptionIdNode = exceptionNode->AsMap()->FindChild("id");
                if (!exceptionIdNode || exceptionIdNode->GetType() != ENodeType::Int64) {
                    // No exception information, go as-is.
                    return result;
                }

                auto errorNode = result->AsMap()->FindChild("error");
                auto blackboxError =
                    errorNode && errorNode->GetType() == ENodeType::String
                    ? TError(errorNode->GetValue<TString>())
                    : TError("Blackbox did not provide any human-readable error details");

                switch (static_cast<EBlackboxException>(exceptionIdNode->GetValue<i64>())) {
                    case EBlackboxException::Ok:
                        return result;
                    case EBlackboxException::DBFetchFailed:
                    case EBlackboxException::DBException:
                        YT_LOG_WARNING(blackboxError,
                            "Blackbox has raised an exception, backing off (CallId: %v, Attempt: %v)",
                            callId,
                            attempt);
                        break;
                    default:
                        YT_LOG_WARNING(blackboxError,
                            "Blackbox has raised an exception (CallId: %v, Attempt: %v)",
                            callId,
                            attempt);
                        AuthProfiler.Increment(BlackboxCallFatalErrors_);
                        THROW_ERROR_EXCEPTION("Blackbox has raised an exception")
                            << TErrorAttribute("call_id", callId)
                            << TErrorAttribute("attempt", attempt)
                            << blackboxError;
                }
            }

            auto now = TInstant::Now();
            if (now > deadline) {
                break;
            }

            TDelayedExecutor::WaitForDuration(std::min(Config_->BackoffTimeout, deadline - now));
        }

        AuthProfiler.Increment(BlackboxCallFatalErrors_);
        THROW_ERROR_EXCEPTION("Blackbox call failed")
            << std::move(accumulatedErrors)
            << TErrorAttribute("call_id", callId);
    }

    static NJson::TJsonFormatConfigPtr MakeJsonFormatConfig()
    {
        auto config = New<NJson::TJsonFormatConfig>();
        config->EncodeUtf8 = false; // Hipsters use real Utf8.
        return config;
    }

    INodePtr DoCallOnce(
        TGuid callId,
        int attempt,
        const TString& realUrl,
        const TString& safeUrl,
        const THeadersPtr& headers,
        TInstant deadline)
    {
        auto onError = [&] (TError error) {
            error.Attributes().Set("call_id", callId);
            YT_LOG_DEBUG(error);
            THROW_ERROR(error);
        };

        NProfiling::TWallTimer timer;
        auto timeout = std::min(deadline - TInstant::Now(), Config_->AttemptTimeout);

        YT_LOG_DEBUG("Calling Blackbox (Url: %v, CallId: %v, Attempt: %v, Timeout: %v)",
            safeUrl,
            callId,
            attempt,
            timeout);

        auto rspOrError = WaitFor(HttpClient_->Get(realUrl, headers));
        if (!rspOrError.IsOK()) {
            onError(TError("Blackbox call failed")
                << rspOrError);
        }

        const auto& rsp = rspOrError.Value();
        if (rsp->GetStatusCode() != EStatusCode::OK) {
            onError(TError("Blackbox call returned HTTP status code %v",
                static_cast<int>(rsp->GetStatusCode())));
        }

        INodePtr rootNode;
        try {

            YT_LOG_DEBUG("Started reading response body from Blackbox (CallId: %v, Attempt: %v)",
                callId,
                attempt);

            auto body = rsp->ReadAll();

            YT_LOG_DEBUG("Finished reading response body from Blackbox (CallId: %v, Attempt: %v)\n%v",
                callId,
                attempt,
                body);

            TMemoryInput stream(body.Begin(), body.Size());
            auto factory = NYTree::CreateEphemeralNodeFactory();
            auto builder = NYTree::CreateBuilderFromFactory(factory.get());
            static const auto Config = MakeJsonFormatConfig();
            NJson::ParseJson(&stream, builder.get(), Config);
            rootNode = builder->EndTree();

            Profiler_.Update(BlackboxCallTime_, timer.GetElapsedValue());
            YT_LOG_DEBUG("Parsed Blackbox daemon reply (CallId: %v, Attempt: %v)",
                callId,
                attempt);
        } catch (const std::exception& ex) {
            onError(TError(
                "Error parsing Blackbox response")
                << ex);
        }

        if (rootNode->GetType() != ENodeType::Map) {
            THROW_ERROR_EXCEPTION("Blackbox has returned an improper result")
                << TErrorAttribute("expected_result_type", ENodeType::Map)
                << TErrorAttribute("actual_result_type", rootNode->GetType());
        }

        return rootNode;
    }
};

IBlackboxServicePtr CreateDefaultBlackboxService(
    TDefaultBlackboxServiceConfigPtr config,
    ITvmServicePtr tvmService,
    IPollerPtr poller,
    NProfiling::TProfiler profiler)
{
    return New<TDefaultBlackboxService>(
        std::move(config),
        std::move(tvmService),
        std::move(poller),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
