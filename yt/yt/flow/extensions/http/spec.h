#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/connectors/common/delegating_async_sink_base.h>

#include <util/generic/hash.h>

namespace NYT::NFlow {

struct TAsyncHttpSinkParameters
    : public TDelegatingAsyncSinkBase::TParameters
{
    std::string Url;
    std::string PayloadColumn;
    THashMap<std::string, std::string> Headers;
    std::string IdempotencyHeader = "Idempotency-Key";
    bool KeepAlive = true;
    int MaxRedirectCount = 0;
    int MaxIdleConnections = 8;

    REGISTER_YSON_STRUCT(TAsyncHttpSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAsyncHttpSinkParameters);

struct TDynamicAsyncHttpSinkParameters
    : public TDelegatingAsyncSinkBase::TDynamicParameters
{
    TDuration RequestTimeout;
    TDuration AttemptTimeout;
    TDuration RetryInitialDelay;
    TDuration RetryMinimumDelay;
    double RetryMultiplier = 2.0;
    TDuration RetryMaximumDelay;
    double RetryJitterRatio = 0.2;
    int MaxAttemptCount = 3;

    REGISTER_YSON_STRUCT(TDynamicAsyncHttpSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicAsyncHttpSinkParameters);

} // namespace NYT::NFlow
