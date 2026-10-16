#include "spec.h"

#include <yt/yt/core/http/http.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/convert.h>

#include <algorithm>

namespace NYT::NFlow {
namespace {

bool IsHttpHeaderNameCharacter(unsigned char character)
{
    return (character >= 'a' && character <= 'z') ||
        (character >= 'A' && character <= 'Z') ||
        (character >= '0' && character <= '9') ||
        TStringBuf("!#$%&'*+-.^_`|~").Contains(character);
}

bool IsValidHttpHeaderName(TStringBuf name)
{
    return !name.empty() && std::all_of(name.begin(), name.end(), IsHttpHeaderNameCharacter);
}

bool IsAllowedHttpHeader(TStringBuf name)
{
    static const NHttp::THeaders::THeaderNames ForbiddenHeaders = {
        "connection",
        "content-length",
        "host",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    };
    return !ForbiddenHeaders.contains(name);
}

bool IsValidHttpHeaderValue(TStringBuf value)
{
    return value.find('\r') == TStringBuf::npos && value.find('\n') == TStringBuf::npos;
}

} // namespace

void TAsyncHttpSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("url", &TThis::Url).NonEmpty();
    registrar.Parameter("payload_column", &TThis::PayloadColumn).NonEmpty();
    registrar.Parameter("headers", &TThis::Headers).Default();
    registrar.Parameter("idempotency_header", &TThis::IdempotencyHeader)
        .Default("Idempotency-Key");
    registrar.Parameter("keep_alive", &TThis::KeepAlive).Default(true);
    registrar.Parameter("max_redirect_count", &TThis::MaxRedirectCount)
        .Default(0)
        .GreaterThanOrEqual(0);
    registrar.Parameter("max_idle_connections", &TThis::MaxIdleConnections)
        .Default(8)
        .GreaterThanOrEqual(0);
    registrar.Postprocessor([] (TThis* parameters) {
        if (parameters->AtMostOnceStrategy && parameters->AtMostOnceStrategy->Enabled) {
            THROW_ERROR_EXCEPTION("Async HTTP sink does not support at_most_once_strategy");
        }

        auto parsedUrl = NHttp::ParseUrl(parameters->Url);
        if ((parsedUrl.Protocol != "http" && parsedUrl.Protocol != "https") || parsedUrl.Host.empty()) {
            THROW_ERROR_EXCEPTION("Async HTTP sink URL must use http or https and contain a host");
        }

        NHttp::THeaders::THeaderNames headerNames;
        for (const auto& [name, value] : parameters->Headers) {
            if (!IsValidHttpHeaderName(name)) {
                THROW_ERROR_EXCEPTION("Async HTTP sink header names must be valid HTTP tokens");
            }
            if (!IsValidHttpHeaderValue(value)) {
                THROW_ERROR_EXCEPTION("Async HTTP sink header values must not contain carriage returns or newlines");
            }
            if (!IsAllowedHttpHeader(name)) {
                THROW_ERROR_EXCEPTION("Async HTTP sink headers must not contain hop-by-hop or transport-managed names");
            }
            if (!headerNames.insert(name).second) {
                THROW_ERROR_EXCEPTION("Async HTTP sink header names must be unique case-insensitively");
            }
        }
        if (!parameters->IdempotencyHeader.empty() &&
            !IsValidHttpHeaderName(parameters->IdempotencyHeader))
        {
            THROW_ERROR_EXCEPTION(
                "Async HTTP sink idempotency_header must be empty or a valid HTTP header name");
        }
        if (!parameters->IdempotencyHeader.empty() &&
            !IsAllowedHttpHeader(parameters->IdempotencyHeader))
        {
            THROW_ERROR_EXCEPTION(
                "Async HTTP sink idempotency_header must not be hop-by-hop or transport-managed");
        }
        if (!parameters->IdempotencyHeader.empty() &&
            headerNames.contains(parameters->IdempotencyHeader))
        {
            THROW_ERROR_EXCEPTION("Async HTTP sink headers must not override idempotency_header");
        }
    });
}

void TDynamicAsyncHttpSinkParameters::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TThis* parameters) {
        if (parameters->AtMostOnceStrategy) {
            parameters->AtMostOnceStrategy->SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::Keep);
        }
    });

    registrar.Parameter("request_timeout", &TThis::RequestTimeout)
        .Default(TDuration::Minutes(1))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("attempt_timeout", &TThis::AttemptTimeout)
        .Default(TDuration::Seconds(10))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("retry_initial_delay", &TThis::RetryInitialDelay)
        .Default(TDuration::Seconds(1))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("retry_minimum_delay", &TThis::RetryMinimumDelay)
        .Default(TDuration::MilliSeconds(100))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("retry_multiplier", &TThis::RetryMultiplier)
        .Default(2.0)
        .InRange(1.01, 100.0);
    registrar.Parameter("retry_maximum_delay", &TThis::RetryMaximumDelay)
        .Default(TDuration::Seconds(30))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("retry_jitter_ratio", &TThis::RetryJitterRatio)
        .Default(0.2)
        .InRange(0.0, 1.0);
    registrar.Parameter("max_attempt_count", &TThis::MaxAttemptCount)
        .Default(5)
        .InRange(1, 100);
    registrar.Postprocessor([] (TThis* parameters) {
        if (parameters->AtMostOnceStrategy) {
            const auto& unrecognized = parameters->AtMostOnceStrategy->GetLocalUnrecognized();
            const auto& enabled = unrecognized ? unrecognized->FindChild("enabled") : nullptr;
            if (enabled && NYTree::ConvertTo<bool>(enabled)) {
                THROW_ERROR_EXCEPTION("Async HTTP sink does not support at_most_once_strategy");
            }
        }

        if (parameters->AttemptTimeout > parameters->RequestTimeout) {
            THROW_ERROR_EXCEPTION("attempt_timeout must not exceed request_timeout");
        }
        if (parameters->RetryMinimumDelay > parameters->RetryInitialDelay) {
            THROW_ERROR_EXCEPTION("retry_minimum_delay must not exceed retry_initial_delay");
        }
        if (parameters->RetryInitialDelay > parameters->RetryMaximumDelay) {
            THROW_ERROR_EXCEPTION("retry_initial_delay must not exceed retry_maximum_delay");
        }
    });
}

} // namespace NYT::NFlow
