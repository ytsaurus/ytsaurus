#include "default_blackbox_service.h"
#include "blackbox_service.h"
#include "config.h"
#include "helpers.h"
#include "private.h"

#include <yt/core/json/json_parser.h>

#include <library/http/simple/http_client.h>

#include <util/string/quote.h>
#include <util/string/url.h>

namespace NYT {
namespace NAuth {

using namespace NYTree;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

static constexpr auto TimeoutSlack = TDuration::MilliSeconds(1);

////////////////////////////////////////////////////////////////////////////////

class TDefaultBlackboxService
    : public IBlackboxService
{
public:
    TDefaultBlackboxService(
        TDefaultBlackboxServiceConfigPtr config,
        IInvokerPtr invoker)
        : Config_(std::move(config))
        , Invoker_(std::move(invoker))
    { }

    virtual TFuture<INodePtr> Call(
        const TString& method,
        const THashMap<TString, TString>& params,
        const THashMap<TString, TString>& headers) override
    {
        auto deadline = TInstant::Now() + Config_->RequestTimeout;
        return BIND(&TDefaultBlackboxService::DoCall, MakeStrong(this), method, params, headers, deadline)
            .AsyncVia(Invoker_)
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
    const IInvokerPtr Invoker_;

    TMonotonicCounter BlackboxCalls_{"/blackbox_calls"};
    TMonotonicCounter BlackboxCallErrors_{"/blackbox_call_errors"};
    TMonotonicCounter BlackboxCallFatalErrors_{"/blackbox_call_fatal_errors"};

private:
    INodePtr DoCall(
        const TString& method,
        const THashMap<TString, TString>& params,
        const THashMap<TString, TString>& headers,
        TInstant deadline)
    {
        auto host = AddSchemePrefix(TString(GetHost(Config_->Host)), Config_->Secure ? "https" : "http");
        auto port = Config_->Port;

        TSafeUrlBuilder builder;
        builder.AppendString("/blackbox?");
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

        auto callId = TGuid::Create();

        std::vector<TError> accumulatedErrors;

        for (int attempt = 1; deadline - TInstant::Now() > TimeoutSlack; ++attempt) {
            INodePtr result;
            try {
                AuthProfiler.Increment(BlackboxCalls_);
                result = DoCallOnce(
                    callId,
                    attempt,
                    host,
                    port,
                    realUrl,
                    safeUrl,
                    headers,
                    deadline);
            } catch (const std::exception& ex) {
                AuthProfiler.Increment(BlackboxCallErrors_);
            
                LOG_WARNING(
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

                // See https://doc.yandex-team.ru/blackbox/concepts/blackboxErrors.xml
                switch (EBlackboxException(exceptionIdNode->GetValue<i64>())) {
                    case EBlackboxException::Ok:
                        return result;
                    case EBlackboxException::DBFetchFailed:
                    case EBlackboxException::DBException:
                        LOG_WARNING(blackboxError,
                            "Blackbox has raised an exception, backing off (CallId: %v, Attempt: %v)",
                            callId,
                            attempt);
                        break;
                    default:
                        LOG_WARNING(blackboxError,
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

            Sleep(std::min(Config_->BackoffTimeout, deadline - now));
        }

        AuthProfiler.Increment(BlackboxCallFatalErrors_);
        THROW_ERROR_EXCEPTION("Blackbox call failed")
            << std::move(accumulatedErrors)
            << TErrorAttribute("call_id", callId);
    }

    INodePtr DoCallOnce(
        TGuid callId,
        int attempt,
        const TString& host,
        ui16 port,
        const TString& realUrl,
        const TString& safeUrl,
        const THashMap<TString, TString>& headers,
        TInstant deadline)
    {
        auto timeout = std::min(deadline - TInstant::Now(), Config_->AttemptTimeout);

        TString buffer;
        INodePtr result;

        LOG_DEBUG("Calling Blackbox (Url: %v, CallId: %v, Attempt: %v, Host: %v, Port: %v, Timeout: %v)",
            safeUrl,
            callId,
            attempt,
            host,
            port,
            timeout);

        {
            TSimpleHttpClient httpClient(host, port, timeout, timeout);
            TStringOutput outputStream(buffer);
            httpClient.DoGet(realUrl, &outputStream, headers);
        }

        LOG_DEBUG("Received Blackbox reply (CallId: %v, Attempt: %v)\n%v",
            callId,
            attempt,
            buffer);

        {
            TStringInput inputStream(buffer);
            auto factory = NYTree::CreateEphemeralNodeFactory();
            auto builder = NYTree::CreateBuilderFromFactory(factory.get());
            auto config = New<NJson::TJsonFormatConfig>();
            config->EncodeUtf8 = false; // Hipsters use real Utf8.
            NJson::ParseJson(&inputStream, builder.get(), std::move(config));
            result = builder->EndTree();
        }

        if (!result || result->GetType() != ENodeType::Map) {
            THROW_ERROR_EXCEPTION("Blackbox has returned an improper result")
                << TErrorAttribute("expected_result_type", ENodeType::Map)
                << TErrorAttribute("actual_result_type", result->GetType());
        }

        LOG_DEBUG("Parsed Blackbox reply (CallId: %v, Attempt: %v)",
            callId,
            attempt);

        return result;
    }
};

IBlackboxServicePtr CreateDefaultBlackboxService(
    TDefaultBlackboxServiceConfigPtr config,
    IInvokerPtr invoker)
{
    return New<TDefaultBlackboxService>(
        std::move(config),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
