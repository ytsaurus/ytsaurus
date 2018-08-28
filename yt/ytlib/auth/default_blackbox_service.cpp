#include "default_blackbox_service.h"
#include "blackbox_service.h"
#include "config.h"
#include "private.h"
#include "helpers.h"

#include <yt/core/json/json_parser.h>

#include <library/http/simple/http_client.h>

#include <util/string/quote.h>
#include <util/string/url.h>

namespace NYT {
namespace NAuth {

using namespace NYTree;

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

    virtual TFuture<INodePtr> Call(const TString& method, const THashMap<TString, TString>& params) override
    {
        auto deadline = TInstant::Now() + Config_->RequestTimeout;
        return BIND(&TDefaultBlackboxService::DoCall, MakeStrong(this), method, params, deadline)
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

    static const THashSet<TString> PrivateUrlParams_;

private:
    static std::pair<TString, TString> BuildUrl(const TString& method, const THashMap<TString, TString>& params)
    {
        TStringBuilder realUrl;
        TStringBuilder safeUrl;

        auto appendString = [&] (const char* string) {
            realUrl.AppendString(string);
            safeUrl.AppendString(string);
        };

        auto appendChar = [&] (const char ch) {
            realUrl.AppendChar(ch);
            safeUrl.AppendChar(ch);
        };

        auto appendParam = [&] (const TString& key, const TString& value) {
            auto size = key.length() + 4 + CgiEscapeBufLen(value.length());

            char* realBegin = realUrl.Preallocate(size);
            char* realIt = realBegin;
            memcpy(realIt, key.c_str(), key.length());
            realIt += key.length();
            *realIt = '=';
            realIt += 1;
            auto realEnd = CGIEscape(realIt, value.c_str(), value.length());
            realUrl.Advance(realEnd - realBegin);

            char* safeBegin = safeUrl.Preallocate(size);
            char* safeEnd = safeBegin;
            if (PrivateUrlParams_.has(key)) {
                memcpy(safeEnd, realBegin, realIt - realBegin);
                safeEnd += realIt - realBegin;
                memcpy(safeEnd, "***", 3);
                safeEnd += 3;
            } else {
                memcpy(safeEnd, realBegin, realEnd - realBegin);
                safeEnd += realEnd - realBegin;
            }
            safeUrl.Advance(safeEnd - safeBegin);
        };

        appendString("/blackbox?");
        appendParam("method", method);
        for (const auto& param : params) {
            appendChar('&');
            appendParam(param.first, param.second);
        }
        appendChar('&');
        appendParam("attributes", "1008");
        appendChar('&');
        appendParam("format", "json");

        return std::make_pair(realUrl.Flush(), safeUrl.Flush());
    }

    INodePtr DoCall(const TString& method, const THashMap<TString, TString>& params, TInstant deadline)
    {
        auto host = AddSchemePrefix(TString(GetHost(Config_->Host)), Config_->Secure ? "https" : "http");
        auto port = Config_->Port;

        TString realUrl, safeUrl;
        std::tie(realUrl, safeUrl) = BuildUrl(method, params);

        ui64 callId = RandomNumber<ui64>();

        std::vector<TError> accumulatedErrors;

        for (int attempt = 1; deadline - TInstant::Now() > TimeoutSlack; ++attempt) {
            INodePtr result;
            try {
                result = DoCallOnce(callId, attempt, host, port, realUrl, safeUrl, deadline);
            } catch (const std::exception& ex) {
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
                auto exception = result->AsMap()->FindChild("exception");
                if (exception && exception->GetType() == ENodeType::Map) {
                    auto exceptionId = exception->AsMap()->FindChild("id");
                    if (exceptionId) {
                        auto value = ConvertTo<int>(exceptionId);
                        // See https://doc.yandex-team.ru/blackbox/concepts/blackboxErrors.xml
                        switch (EBlackboxException(value)) {
                            case EBlackboxException::Ok:
                                return result;
                            case EBlackboxException::DBFetchFailed:
                            case EBlackboxException::DBException:
                                LOG_WARNING(
                                    "Blackbox has raised an exception, backing off (CallId: %v, Attempt: %v)",
                                    callId,
                                    attempt);
                                break;
                            default:
                                LOG_WARNING(
                                    "Blackbox has raised an exception (CallId: %v, Attempt: %v)",
                                    callId,
                                    attempt);
                                THROW_ERROR_EXCEPTION("Blackbox has raised an exception")
                                    << TErrorAttribute("call_id", callId)
                                    << TErrorAttribute("attempt", attempt);
                        }
                    }
                } else {
                    // No exception information, go as-is.
                    return result;
                }
            }

            auto now = TInstant::Now();
            if (now > deadline) {
                break;
            }
            Sleep(std::min(Config_->BackoffTimeout, deadline - now));
        }

        THROW_ERROR_EXCEPTION("Blackbox call failed")
            << std::move(accumulatedErrors)
            << TErrorAttribute("call_id", callId);
    }

    INodePtr DoCallOnce(
        ui64 callId,
        int attempt,
        const TString& host,
        ui16 port,
        const TString& realUrl,
        const TString& safeUrl,
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
            httpClient.DoGet(realUrl, &outputStream);
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

const THashSet<TString> TDefaultBlackboxService::PrivateUrlParams_ = {
    "userip",
    "oauth_token",
    "sessionid",
    "sslsessionid"
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
