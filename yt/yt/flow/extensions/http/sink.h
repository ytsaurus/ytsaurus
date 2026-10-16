#pragma once

#include "public.h"
#include "spec.h"

#include <yt/yt/flow/library/cpp/connectors/common/delegating_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/http/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <map>

namespace NYT::NFlow {

DECLARE_REFCOUNTED_CLASS(TOrderedDeliveryQueue);

using TAsyncHttpDelayExecutor = TCallback<TFuture<void>(TDuration)>;
using TAsyncHttpRandomGenerator = TCallback<double()>;
using TAsyncHttpNow = TCallback<TInstant()>;

TDuration CalculateAsyncHttpRetryDelay(
    const TDynamicAsyncHttpSinkParameters& parameters,
    int retryIndex,
    double randomSample);

struct TAsyncHttpClients
{
    NHttp::IClientPtr Http;
    NHttp::IClientPtr Https;
};

TAsyncHttpClients CreateAsyncHttpClients(
    const TAsyncHttpSinkParameters& parameters,
    const NConcurrency::IPollerPtr& poller);

TFuture<void> DrainAsyncHttpResponse(const NHttp::IResponsePtr& response);

class TAsyncHttpRequestExecutor
    : public TRefCounted
{
public:
    TAsyncHttpRequestExecutor(
        NHttp::IClientPtr client,
        std::string url,
        NHttp::THeadersPtr headers,
        TDynamicAsyncHttpSinkParametersPtr dynamicParameters,
        IInvokerPtr invoker,
        NProfiling::TProfiler profiler,
        IStatusErrorStatePtr errorState,
        TAsyncHttpDelayExecutor delayExecutor,
        TAsyncHttpRandomGenerator randomGenerator,
        TAsyncHttpNow now,
        std::string idempotencyHeader = {},
        int maxRedirectCount = 0);

    TAsyncHttpRequestExecutor(
        TAsyncHttpClients clients,
        std::string url,
        NHttp::THeadersPtr headers,
        TDynamicAsyncHttpSinkParametersPtr dynamicParameters,
        IInvokerPtr invoker,
        NProfiling::TProfiler profiler,
        IStatusErrorStatePtr errorState,
        TAsyncHttpDelayExecutor delayExecutor,
        TAsyncHttpRandomGenerator randomGenerator,
        TAsyncHttpNow now,
        std::string idempotencyHeader = {},
        int maxRedirectCount = 0);

    void Reconfigure(TDynamicAsyncHttpSinkParametersPtr parameters);
    TFuture<void> Post(TSharedRef body, std::string idempotencyKey = {});

private:
    friend class TAsyncHttpRequest;

    const TAsyncHttpClients Clients_;
    const std::string Url_;
    const NHttp::THeadersPtr Headers_;
    const std::string IdempotencyHeader_;
    const int MaxRedirectCount_;
    TAtomicIntrusivePtr<TDynamicAsyncHttpSinkParameters> DynamicParameters_;
    const IInvokerPtr Invoker_;
    const NProfiling::TProfiler Profiler_;
    const IStatusErrorStatePtr ErrorState_;
    const TAsyncHttpDelayExecutor DelayExecutor_;
    const TAsyncHttpRandomGenerator RandomGenerator_;
    const TAsyncHttpNow Now_;
    const NProfiling::TCounter AttemptFailureCounter_;
    THashMap<int, NProfiling::TCounter> ResponseCounters_;
    ui64 NextDeliveryId_ = 0;
    std::map<ui64, TError> ActiveDeliveryErrors_;

    TFuture<void> DoPost(TSharedRef body, std::string idempotencyKey);
    TFuture<NHttp::IResponsePtr> Post(const std::string& url, const TSharedRef& body, const NHttp::THeadersPtr& headers);
    void CountResponse(int statusCode);
    void UpdateDeliveryError(ui64 deliveryId, TError error);
    void ResolveDelivery(ui64 deliveryId);
};

DEFINE_REFCOUNTED_TYPE(TAsyncHttpRequestExecutor);

class TAsyncHttpSinkController
    : public TSinkControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TAsyncHttpSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicAsyncHttpSinkParameters);

    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override;
};

class TAsyncHttpSink
    : public TDelegatingAsyncSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TAsyncHttpSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicAsyncHttpSinkParameters);

    using TSinkController = TAsyncHttpSinkController;

    TAsyncHttpSink(TSinkContextPtr context, TDynamicSinkContextPtr dynamicContext);

private:
    void DoInit(const std::string& producerId) override;
    std::pair<TFuture<void>, ui64> DoDistribute(
        const TOutputMessageConstPtr& message,
        i64 seqNo) override;

    std::string PayloadColumn_;
    TAsyncHttpRequestExecutorPtr Executor_;
    TOrderedDeliveryQueuePtr OrderedDeliveryQueue_;
};

DEFINE_REFCOUNTED_TYPE(TAsyncHttpSinkController);
DEFINE_REFCOUNTED_TYPE(TAsyncHttpSink);

} // namespace NYT::NFlow
