#include "sink.h"

#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>
#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/random/random.h>
#include <util/string/hex.h>

#include <algorithm>
#include <cctype>
#include <deque>
#include <optional>
#include <string_view>
#include <vector>

namespace NYT::NFlow {
namespace {

bool IsUrlScheme(std::string_view value)
{
    if (value.empty() || !std::isalpha(static_cast<unsigned char>(value.front()))) {
        return false;
    }

    return std::all_of(value.begin() + 1, value.end(), [] (unsigned char character) {
        return std::isalnum(character) || character == '+' || character == '-' || character == '.';
    });
}

std::string NormalizeRedirectPath(std::string path)
{
    const bool absolute = !path.empty() && path.front() == '/';
    const bool trailingSlash = !path.empty() && path.back() == '/';
    std::vector<std::string> segments;
    size_t position = 0;
    while (position <= path.size()) {
        const auto next = path.find('/', position);
        const auto segment = path.substr(position, next - position);
        if (segment == "..") {
            if (!segments.empty()) {
                segments.pop_back();
            }
        } else if (!segment.empty() && segment != ".") {
            segments.push_back(segment);
        }
        if (next == std::string::npos) {
            break;
        }
        position = next + 1;
    }

    std::string result = absolute ? "/" : "";
    for (const auto& segment : segments) {
        if (!result.empty() && result.back() != '/') {
            result += '/';
        }
        result += segment;
    }
    if (trailingSlash && !result.empty() && result.back() != '/') {
        result += '/';
    }
    return result;
}

std::optional<std::string> ResolveRedirectLocation(
    const std::string& currentUrl,
    const std::string& location)
{
    if (location.empty()) {
        return std::nullopt;
    }

    const auto schemeEnd = currentUrl.find("://");
    if (schemeEnd == std::string::npos) {
        return std::nullopt;
    }
    const auto authorityEnd = currentUrl.find_first_of("/?#", schemeEnd + 3);
    const auto origin = currentUrl.substr(0, authorityEnd);
    const auto currentPathEnd = currentUrl.find_first_of("?#", authorityEnd);
    const auto currentPath = authorityEnd == std::string::npos
        ? std::string()
        : currentUrl.substr(
            authorityEnd,
            currentPathEnd == std::string::npos ? std::string::npos : currentPathEnd - authorityEnd);

    const auto locationPathEnd = location.find_first_of("?#");
    const auto locationPath = location.substr(0, locationPathEnd);
    const auto locationSuffix = locationPathEnd == std::string::npos
        ? std::string()
        : location.substr(locationPathEnd);
    const auto locationSchemeEnd = locationPath.find(':');
    if (locationSchemeEnd != std::string::npos && IsUrlScheme(locationPath.substr(0, locationSchemeEnd))) {
        return location;
    }
    if (location.rfind("//", 0) == 0) {
        return currentUrl.substr(0, schemeEnd + 1) + location;
    }
    if (locationPath.empty()) {
        return origin + (currentPath.empty() ? "/" : currentPath) + locationSuffix;
    }
    if (locationPath.front() == '/') {
        return origin + NormalizeRedirectPath(locationPath) + locationSuffix;
    }

    auto basePath = currentPath.empty() ? "/" : currentPath;
    const auto lastSlash = basePath.rfind('/');
    auto directory = lastSlash == std::string::npos ? "/" : basePath.substr(0, lastSlash + 1);
    return origin + NormalizeRedirectPath(directory + locationPath) + locationSuffix;
}

} // namespace

class TOrderedDeliveryQueue
    : public TRefCounted
{
public:
    TOrderedDeliveryQueue() = default;

    TFuture<void> Enqueue(TFuture<void> deliveryFuture)
    {
        auto promise = NewPromise<void>();
        ui64 deliveryId;
        bool subscribe = false;
        bool processCompletedDeliveries = false;
        {
            auto guard = Guard(Lock_);
            deliveryId = NextDeliveryId_++;
            if (TerminalError_) {
                CompletedDeliveries_.emplace_back(promise, *TerminalError_);
                if (!ProcessingCompletedDeliveries_) {
                    ProcessingCompletedDeliveries_ = true;
                    processCompletedDeliveries = true;
                }
            } else {
                PendingDeliveries_.push_back({
                    .Id = deliveryId,
                    .Promise = promise,
                });
                subscribe = true;
            }
        }
        if (subscribe) {
            deliveryFuture.Subscribe(BIND(&TOrderedDeliveryQueue::OnDeliveryFinished, MakeStrong(this), deliveryId));
        }
        if (processCompletedDeliveries) {
            ProcessCompletedDeliveries();
        }
        return promise.ToFuture();
    }

    std::optional<TError> GetTerminalError()
    {
        auto guard = Guard(Lock_);
        return TerminalError_;
    }

private:
    struct TPendingDelivery
    {
        ui64 Id;
        TPromise<void> Promise;
        std::optional<TError> Result;
    };

    void OnDeliveryFinished(ui64 deliveryId, const TError& error)
    {
        bool processCompletedDeliveries = false;
        {
            auto guard = Guard(Lock_);
            if (PendingDeliveries_.empty() || deliveryId < PendingDeliveries_.front().Id) {
                return;
            }

            auto index = deliveryId - PendingDeliveries_.front().Id;
            if (index >= PendingDeliveries_.size()) {
                return;
            }
            PendingDeliveries_[index].Result = error;

            while (!PendingDeliveries_.empty() && PendingDeliveries_.front().Result) {
                auto delivery = std::move(PendingDeliveries_.front());
                PendingDeliveries_.pop_front();
                auto result = *delivery.Result;
                CompletedDeliveries_.emplace_back(std::move(delivery.Promise), result);
                if (!result.IsOK()) {
                    TerminalError_ = result;
                    for (auto& pendingDelivery : PendingDeliveries_) {
                        CompletedDeliveries_.emplace_back(std::move(pendingDelivery.Promise), result);
                    }
                    PendingDeliveries_.clear();
                    break;
                }
            }

            if (!CompletedDeliveries_.empty() && !ProcessingCompletedDeliveries_) {
                ProcessingCompletedDeliveries_ = true;
                processCompletedDeliveries = true;
            }
        }

        if (processCompletedDeliveries) {
            ProcessCompletedDeliveries();
        }
    }

    void ProcessCompletedDeliveries()
    {
        while (true) {
            std::deque<std::pair<TPromise<void>, TError>> completedDeliveries;
            {
                auto guard = Guard(Lock_);
                if (CompletedDeliveries_.empty()) {
                    ProcessingCompletedDeliveries_ = false;
                    return;
                }
                std::swap(completedDeliveries, CompletedDeliveries_);
            }

            for (auto& [promise, result] : completedDeliveries) {
                promise.Set(std::move(result));
            }
        }
    }

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    ui64 NextDeliveryId_ = 0;
    std::optional<TError> TerminalError_;
    std::deque<TPendingDelivery> PendingDeliveries_;
    std::deque<std::pair<TPromise<void>, TError>> CompletedDeliveries_;
    bool ProcessingCompletedDeliveries_ = false;
};

DEFINE_REFCOUNTED_TYPE(TOrderedDeliveryQueue);

class TAsyncHttpRequest
    : public TRefCounted
{
public:
    TAsyncHttpRequest(
        TAsyncHttpRequestExecutorPtr executor,
        ui64 deliveryId,
        TSharedRef body,
        NHttp::THeadersPtr headers)
        : Executor_(std::move(executor))
        , DeliveryId_(deliveryId)
        , Body_(std::move(body))
        , Headers_(std::move(headers))
    { }

    TFuture<void> Run()
    {
        StartTime_ = Executor_->Now_();
        return StartAttempt();
    }

private:
    TFuture<void> StartAttempt()
    {
        auto parameters = Executor_->DynamicParameters_.Acquire();
        auto remaining = parameters->RequestTimeout - (Executor_->Now_() - StartTime_);
        if (AttemptCount_ >= parameters->MaxAttemptCount || remaining <= TDuration::Zero()) {
            return Fail(TError("Async HTTP POST retry policy exhausted")
                    .With(TErrorAttribute("attempt_count", AttemptCount_))
                    .With(LastError_));
        }

        ++AttemptCount_;
        auto attemptTimeout = std::min(parameters->AttemptTimeout, remaining);
        CurrentUrl_ = Executor_->Url_;
        return StartRequest()
            .WithTimeout(attemptTimeout)
            .Apply(BIND(&TAsyncHttpRequest::HandleAttempt, MakeStrong(this))
                    .AsyncVia(Executor_->Invoker_));
    }

    TFuture<int> StartRequest()
    {
        return Executor_->Post(CurrentUrl_, Body_, Headers_)
            .Apply(BIND(&TAsyncHttpRequest::DrainResponse, MakeStrong(this))
                    .AsyncVia(Executor_->Invoker_));
    }

    TFuture<int> DrainResponse(const TErrorOr<NHttp::IResponsePtr>& result)
    {
        if (!result.IsOK()) {
            return MakeFuture<int>(TError(result));
        }

        const auto& response = result.Value();
        auto statusCode = response->GetStatusCode();
        Executor_->CountResponse(static_cast<int>(statusCode));
        if (IsRedirect(statusCode) && RedirectCount_ < Executor_->MaxRedirectCount_) {
            const auto* location = response->GetHeaders()->Find("Location");
            if (location) {
                return DrainAsyncHttpResponse(response).Apply(BIND([
                    this,
                    this_ = MakeStrong(this),
                    location = *location] (const TError& error) -> TFuture<int> {
                    if (!error.IsOK()) {
                        return MakeFuture<int>(TError("Failed to drain async HTTP redirect response").With(error));
                    }
                    return FollowRedirect(location);
                }).AsyncVia(Executor_->Invoker_));
            }
        }

        return DrainAsyncHttpResponse(response).Apply(BIND([statusCode] (const TError& error) -> TErrorOr<int> {
            if (!error.IsOK()) {
                return TError("Failed to drain async HTTP response").With(error);
            }
            return static_cast<int>(statusCode);
        }));
    }

    TFuture<int> FollowRedirect(const std::string& location)
    {
        auto url = ResolveRedirectLocation(CurrentUrl_, location);
        if (!url) {
            return MakeFuture<int>(TError("Async HTTP sink could not resolve redirect URL"));
        }
        NHttp::TUrlRef currentUrl;
        NHttp::TUrlRef nextUrl;
        try {
            currentUrl = NHttp::ParseUrl(CurrentUrl_);
            nextUrl = NHttp::ParseUrl(*url);
        } catch (const std::exception&) {
            return MakeFuture<int>(TError("Async HTTP sink could not resolve redirect URL"));
        }
        if (currentUrl.Protocol == "https" && nextUrl.Protocol == "http") {
            return MakeFuture<int>(TError("Async HTTP sink does not follow HTTPS to HTTP redirects"));
        }
        if ((nextUrl.Protocol != "http" && nextUrl.Protocol != "https") || nextUrl.Host.empty()) {
            return MakeFuture<int>(TError("Async HTTP sink redirect URL must use http or https and contain a host"));
        }
        CurrentUrl_ = std::move(*url);
        ++RedirectCount_;
        return StartRequest();
    }

    static bool IsRedirect(NHttp::EStatusCode statusCode)
    {
        return statusCode == NHttp::EStatusCode::MovedPermanently ||
            statusCode == NHttp::EStatusCode::Found ||
            statusCode == NHttp::EStatusCode::SeeOther ||
            statusCode == NHttp::EStatusCode::UseProxy ||
            statusCode == NHttp::EStatusCode::TemporaryRedirect ||
            statusCode == NHttp::EStatusCode::PermanentRedirect;
    }

    TFuture<void> HandleAttempt(const TErrorOr<int>& result)
    {
        if (!result.IsOK()) {
            Executor_->AttemptFailureCounter_.Increment();
            return Retry(TError(result));
        }

        int statusCode = result.Value();
        if (statusCode >= 200 && statusCode < 300) {
            Executor_->ResolveDelivery(DeliveryId_);
            return OKFuture;
        }

        return Retry(TError("Async HTTP POST returned a non-success status")
                .With(TErrorAttribute("status_code", statusCode)));
    }

    TFuture<void> Retry(TError error)
    {
        LastError_ = error;
        Executor_->UpdateDeliveryError(DeliveryId_, error);

        auto parameters = Executor_->DynamicParameters_.Acquire();
        auto remaining = parameters->RequestTimeout - (Executor_->Now_() - StartTime_);
        if (AttemptCount_ >= parameters->MaxAttemptCount || remaining <= TDuration::Zero()) {
            return Fail(TError("Async HTTP POST retry policy exhausted")
                    .With(TErrorAttribute("attempt_count", AttemptCount_))
                    .With(LastError_));
        }

        auto delay = CalculateAsyncHttpRetryDelay(
            *parameters,
            AttemptCount_ - 1,
            Executor_->RandomGenerator_());
        if (delay >= remaining) {
            return Fail(TError("Async HTTP POST retry deadline exhausted")
                    .With(TErrorAttribute("attempt_count", AttemptCount_))
                    .With(LastError_));
        }

        return Executor_->DelayExecutor_(delay).Apply(
            BIND(&TAsyncHttpRequest::HandleDelay, MakeStrong(this))
                .AsyncVia(Executor_->Invoker_));
    }

    TFuture<void> HandleDelay(const TError& error)
    {
        if (!error.IsOK()) {
            return Fail(TError("Async HTTP POST retry delay failed").With(error));
        }
        return StartAttempt();
    }

    TFuture<void> Fail(TError error)
    {
        Executor_->UpdateDeliveryError(DeliveryId_, error);
        return MakeFuture<void>(std::move(error));
    }

    const TAsyncHttpRequestExecutorPtr Executor_;
    const ui64 DeliveryId_;
    const TSharedRef Body_;
    const NHttp::THeadersPtr Headers_;
    TInstant StartTime_;
    int AttemptCount_ = 0;
    int RedirectCount_ = 0;
    std::string CurrentUrl_;
    TError LastError_;
};

DEFINE_REFCOUNTED_TYPE(TAsyncHttpRequest);

TDuration CalculateAsyncHttpRetryDelay(
    const TDynamicAsyncHttpSinkParameters& parameters,
    int retryIndex,
    double randomSample)
{
    YT_VERIFY(retryIndex >= 0);
    YT_VERIFY(randomSample >= -1.0 && randomSample <= 1.0);

    i64 delay = std::clamp(
        parameters.RetryInitialDelay.MicroSeconds(),
        parameters.RetryMinimumDelay.MicroSeconds(),
        parameters.RetryMaximumDelay.MicroSeconds());
    const i64 maximum = parameters.RetryMaximumDelay.MicroSeconds();
    for (int index = 0; index < retryIndex; ++index) {
        if (delay >= static_cast<i64>(maximum / parameters.RetryMultiplier)) {
            delay = maximum;
            break;
        }
        delay = static_cast<i64>(delay * parameters.RetryMultiplier);
    }

    const auto jittered = static_cast<i64>(
        delay * (1.0 + parameters.RetryJitterRatio * randomSample));
    return TDuration::MicroSeconds(std::clamp(
        jittered,
        static_cast<i64>(parameters.RetryMinimumDelay.MicroSeconds()),
        maximum));
}

TAsyncHttpClients CreateAsyncHttpClients(
    const TAsyncHttpSinkParameters& parameters,
    const NConcurrency::IPollerPtr& poller)
{
    auto httpConfig = New<NHttp::TClientConfig>();
    httpConfig->MaxRedirectCount = 0;
    httpConfig->MaxIdleConnections = parameters.KeepAlive ? parameters.MaxIdleConnections : 0;
    httpConfig->OmitQuestionMarkForEmptyQuery = true;

    auto httpsConfig = New<NHttps::TClientConfig>();
    httpsConfig->MaxRedirectCount = 0;
    httpsConfig->MaxIdleConnections = parameters.KeepAlive ? parameters.MaxIdleConnections : 0;
    httpsConfig->OmitQuestionMarkForEmptyQuery = true;

    return {
        .Http = NHttp::CreateClient(std::move(httpConfig), poller),
        .Https = NHttps::CreateClient(httpsConfig, poller),
    };
}

TFuture<void> DrainAsyncHttpResponse(const NHttp::IResponsePtr& response)
{
    return response->Read().Apply(BIND([response] (TSharedRef chunk) {
        return chunk.Empty() ? OKFuture : DrainAsyncHttpResponse(response);
    }));
}

TAsyncHttpRequestExecutor::TAsyncHttpRequestExecutor(
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
    std::string idempotencyHeader,
    int maxRedirectCount)
    : TAsyncHttpRequestExecutor(
        TAsyncHttpClients{
            .Http = client,
            .Https = std::move(client),
        },
        std::move(url),
        std::move(headers),
        std::move(dynamicParameters),
        std::move(invoker),
        std::move(profiler),
        std::move(errorState),
        std::move(delayExecutor),
        std::move(randomGenerator),
        std::move(now),
        std::move(idempotencyHeader),
        maxRedirectCount)
{ }

TAsyncHttpRequestExecutor::TAsyncHttpRequestExecutor(
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
    std::string idempotencyHeader,
    int maxRedirectCount)
    : Clients_(std::move(clients))
    , Url_(std::move(url))
    , Headers_(std::move(headers))
    , IdempotencyHeader_(std::move(idempotencyHeader))
    , MaxRedirectCount_(maxRedirectCount)
    , DynamicParameters_(std::move(dynamicParameters))
    , Invoker_(std::move(invoker))
    , Profiler_(std::move(profiler))
    , ErrorState_(std::move(errorState))
    , DelayExecutor_(std::move(delayExecutor))
    , RandomGenerator_(std::move(randomGenerator))
    , Now_(std::move(now))
    , AttemptFailureCounter_(Profiler_.Counter("/attempt_failures"))
{ }

void TAsyncHttpRequestExecutor::Reconfigure(TDynamicAsyncHttpSinkParametersPtr parameters)
{
    DynamicParameters_.Store(std::move(parameters));
}

TFuture<void> TAsyncHttpRequestExecutor::Post(TSharedRef body, std::string idempotencyKey)
{
    return BIND(
        &TAsyncHttpRequestExecutor::DoPost,
        MakeStrong(this),
        std::move(body),
        std::move(idempotencyKey))
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TAsyncHttpRequestExecutor::DoPost(TSharedRef body, std::string idempotencyKey)
{
    const auto deliveryId = NextDeliveryId_++;
    auto headers = Headers_->Duplicate();
    if (!IdempotencyHeader_.empty()) {
        headers->Set(IdempotencyHeader_, std::move(idempotencyKey));
    }
    return New<TAsyncHttpRequest>(
        MakeStrong(this),
        deliveryId,
        std::move(body),
        std::move(headers))
        ->Run();
}

TFuture<NHttp::IResponsePtr> TAsyncHttpRequestExecutor::Post(
    const std::string& url,
    const TSharedRef& body,
    const NHttp::THeadersPtr& headers)
{
    auto protocol = NHttp::ParseUrl(url).Protocol;
    if (protocol == "http") {
        return Clients_.Http->Post(url, body, headers);
    }
    if (protocol == "https") {
        return Clients_.Https->Post(url, body, headers);
    }
    return MakeFuture<NHttp::IResponsePtr>(TError("Async HTTP sink redirect URL must use http or https"));
}

void TAsyncHttpRequestExecutor::CountResponse(int statusCode)
{
    auto [iterator, inserted] = ResponseCounters_.try_emplace(statusCode);
    if (inserted) {
        iterator->second = Profiler_
            .WithTag("status_code", ToString(statusCode))
            .Counter("/responses");
    }
    iterator->second.Increment();
}

void TAsyncHttpRequestExecutor::UpdateDeliveryError(ui64 deliveryId, TError error)
{
    ActiveDeliveryErrors_[deliveryId] = std::move(error);
    ErrorState_->SetError(ActiveDeliveryErrors_.begin()->second);
}

void TAsyncHttpRequestExecutor::ResolveDelivery(ui64 deliveryId)
{
    ActiveDeliveryErrors_.erase(deliveryId);
    if (ActiveDeliveryErrors_.empty()) {
        ErrorState_->ClearError();
    } else {
        ErrorState_->SetError(ActiveDeliveryErrors_.begin()->second);
    }
}

std::optional<i64> TAsyncHttpSinkController::GetReceiverChannelCount()
{
    return std::nullopt;
}

TAsyncHttpSink::TAsyncHttpSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TDelegatingAsyncSinkBase(std::move(context), std::move(dynamicContext))
    , PayloadColumn_(GetParameters()->PayloadColumn)
{
    if (GetSpec()->InputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Async HTTP sink expects exactly one input stream, got %v",
            GetSpec()->InputStreamIds.size());
    }

    auto streamId = *GetSpec()->InputStreamIds.begin();
    auto schema = GetContext()->StreamSpecStorage->GetSchema(streamId);
    if (schema->Columns().size() != 1) {
        THROW_ERROR_EXCEPTION("Async HTTP sink expects exactly one payload column, got %v",
            schema->Columns().size());
    }

    const auto* column = schema->FindColumn(PayloadColumn_);
    if (!column || column->GetWireType() != NTableClient::EValueType::String) {
        THROW_ERROR_EXCEPTION("Async HTTP sink payload column %Qv must be the only String column",
            PayloadColumn_);
    }

    auto headers = New<NHttp::THeaders>();
    for (const auto& [name, value] : GetParameters()->Headers) {
        headers->Set(name, value);
    }

    Executor_ = New<TAsyncHttpRequestExecutor>(
        CreateAsyncHttpClients(*GetParameters(), GetContext()->Poller),
        GetParameters()->Url,
        std::move(headers),
        GetDynamicParameters(),
        GetContext()->SerializedInvoker,
        GetContext()->Profiler.WithPrefix("/async_http_sink"),
        GetContext()->StatusProfiler->ErrorState("/async_http_sink"),
        BIND([invoker = GetContext()->SerializedInvoker] (TDuration delay) {
            return NConcurrency::TDelayedExecutor::MakeDelayed(delay, invoker);
        }),
        BIND([] {
            return 2.0 * RandomNumber<double>() - 1.0;
        }),
        BIND([] {
            return TInstant::Now();
        }),
        GetParameters()->IdempotencyHeader,
        GetParameters()->MaxRedirectCount);
    OrderedDeliveryQueue_ = New<TOrderedDeliveryQueue>();

    SubscribeReconfigured(BIND([this] (const TDynamicSinkContextPtr&) {
        Executor_->Reconfigure(GetDynamicParameters());
    }));
}

void TAsyncHttpSink::DoInit(const std::string& producerId)
{
    Y_UNUSED(producerId);
}

std::pair<TFuture<void>, ui64> TAsyncHttpSink::DoDistribute(
    const TOutputMessageConstPtr& message,
    i64 seqNo)
{
    Y_UNUSED(seqNo);

    if (auto terminalError = OrderedDeliveryQueue_->GetTerminalError()) {
        return {MakeFuture<void>(std::move(*terminalError)), static_cast<ui64>(message->ByteSize)};
    }

    auto value = GetColumn(*message, PayloadColumn_);
    TFuture<void> deliveryFuture;
    if (value.Type == NTableClient::EValueType::String) {
        const auto messageId = message->MessageId.Underlying();
        deliveryFuture = Executor_->Post(
            TSharedRef::FromString(TString(value.AsStringBuf())),
            HexEncode(messageId.data(), messageId.size()));
    } else if (value.Type == NTableClient::EValueType::Null) {
        deliveryFuture = OKFuture;
    } else {
        deliveryFuture = MakeFuture<void>(
            TError("Async HTTP payload column must contain a String or null value"));
    }
    return {OrderedDeliveryQueue_->Enqueue(std::move(deliveryFuture)), static_cast<ui64>(message->ByteSize)};
}

} // namespace NYT::NFlow
