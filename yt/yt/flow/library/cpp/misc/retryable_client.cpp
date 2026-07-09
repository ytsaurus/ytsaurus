
#include "retryable_client.h"

#include "retryable_client_spec.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rpc_proxy/helpers.h>
#include <yt/yt/client/object_client/public.h>
#include <yt/yt/client/tablet_client/public.h>
#include <yt/yt/client/transaction_client/public.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/misc/backoff_strategy.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

template <typename TResult>
TFuture<TResult> ExecuteWithRetriesSyncImpl(
    const TCallback<TFuture<TResult>(TDuration)>& callback,
    const TDynamicRetryableClientSpecPtr& dynamicSpec,
    const IStatusErrorStatePtr& errorState,
    const NLogging::TLogger& Logger)
{
    TError lastError;

    auto startTime = TInstant::Now();
    auto backoffStrategy = TBackoffStrategy(dynamicSpec->Backoff);
    auto innerTimeout = dynamicSpec->MinInnerTimeout;

    auto makeError = [&] (TStringBuf message) -> TError {
        return TError("%v", message)
            << TErrorAttribute("total_time", TInstant::Now() - startTime)
            << TErrorAttribute("total_time_limit", dynamicSpec->Timeout)
            << TErrorAttribute("failed_attempts", backoffStrategy.GetInvocationIndex())
            << TErrorAttribute("attempts_count_limit", backoffStrategy.GetInvocationCount());
    };

    auto makeNoMoreRetriesErrorFuture = [&] () -> TFuture<TResult> {
        auto resultError = makeError("Common attempts timeout exceeded or attempts count limit exceeded");
        if (!lastError.IsOK()) {
            resultError <<= lastError;
        }
        return MakeFuture<TResult>(resultError);
    };

    try {
        while (true) {
            auto future = callback(innerTimeout);
            auto resultOrError = WaitFor(future);
            if (resultOrError.IsOK()) {
                return future;
            }
            errorState->SetError(resultOrError);
            if (resultOrError.GetCode() == NYT::EErrorCode::Canceled) {
                return makeNoMoreRetriesErrorFuture();
            }
            lastError = resultOrError;
            if (!IsFlowRetriableError(lastError)) {
                return MakeFuture<TResult>(makeError("Request attempt failed, error is not retryable") << lastError);
            }

            if (!backoffStrategy.Next()) {
                return makeNoMoreRetriesErrorFuture();
            }

            auto backoff = backoffStrategy.GetBackoff();
            YT_LOG_WARNING(lastError, "Request attempt failed with retryable error (SleepDuration: %v, NextInnerTimeout: %v)", backoff, innerTimeout);
            innerTimeout = std::max(dynamicSpec->MinInnerTimeout, backoff);
            TDelayedExecutor::WaitForDuration(backoff);
        }
    } catch (const TFiberCanceledException&) {
        return makeNoMoreRetriesErrorFuture();
    }
    YT_LOG_FATAL("Unreachable point reached");
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCommonState)

struct TCommonState
    : public TRefCounted
{
    TAtomicIntrusivePtr<TDynamicRetryableClientSpec> DynamicSpec;
    IStatusProfilerPtr StatusProfiler;
    IInvokerPtr Invoker;
    NApi::IClientPtr UnderlyingClient;

    //! Runs |callback| through the retry loop, reporting under the |component| error state.
    template <typename TResult>
    TFuture<TResult> RunWithRetries(
        const TCallback<TFuture<TResult>(TDuration)>& callback,
        const std::string& component,
        const NLogging::TLogger& logger)
    {
        auto spec = DynamicSpec.Acquire();
        return BIND(&ExecuteWithRetriesSyncImpl<TResult>, callback, spec, StatusProfiler->ErrorState(component), logger)
            .AsyncVia(Invoker)
            .Run()
            .WithTimeout(
                spec->Timeout,
                TFutureTimeoutOptions{
                    .Error = TError(NYT::EErrorCode::Canceled, "Canceled because global timeout exceeded"),
                });
    }
};

DEFINE_REFCOUNTED_TYPE(TCommonState);

////////////////////////////////////////////////////////////////////////////////

class TRetryableTimestampProvider
    : public NTransactionClient::ITimestampProvider
{
public:
    TRetryableTimestampProvider(
        TCommonStatePtr state,
        std::string component,
        NLogging::TLogger logger)
        : State_(std::move(state))
        , Component_(std::move(component))
        , Logger(std::move(logger))
    { }

    TFuture<NTransactionClient::TTimestamp> GenerateTimestamps(
        int count,
        NObjectClient::TCellTag clockClusterTag) override
    {
        return State_->RunWithRetries<NTransactionClient::TTimestamp>(
            BIND([state = State_, count, clockClusterTag] (TDuration /*innerTimeout*/) {
                return state->UnderlyingClient->GetTimestampProvider()->GenerateTimestamps(count, clockClusterTag);
            }),
            Component_,
            Logger);
    }

    NTransactionClient::TTimestamp GetLatestTimestamp(NObjectClient::TCellTag clockClusterTag) override
    {
        return State_->UnderlyingClient->GetTimestampProvider()->GetLatestTimestamp(clockClusterTag);
    }

    void Reconfigure(const NTransactionClient::TRemoteTimestampProviderConfigPtr& config) override
    {
        State_->UnderlyingClient->GetTimestampProvider()->Reconfigure(config);
    }

private:
    const TCommonStatePtr State_;
    const std::string Component_;
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TRetryableClient)

class TRetryableClient
    : public IRetryableClient
{
public:
    TRetryableClient(
        const NLogging::TLogger& logger,
        const TCommonStatePtr& state,
        const std::string& component = "default")
        : IRetryableClient(state->UnderlyingClient)
        , Logger(logger)
        , Component_(component)
        , State_(state)
        , RetryableTimestampProvider_(New<TRetryableTimestampProvider>(
            state,
            component + "/timestamp",
            logger))
    { }

    void Reconfigure(const TDynamicRetryableClientSpecPtr& dynamicSpec) override
    {
        State_->DynamicSpec.Store(dynamicSpec);
    }

    IRetryableClientPtr WithErrorComponent(const std::string& component) override
    {
        return New<TRetryableClient>(
            Logger,
            State_,
            component);
    }

    const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override
    {
        return RetryableTimestampProvider_;
    }

    template <typename TRequestOptions>
    TRequestOptions MakePatchedOptions(const TRequestOptions& options, TDuration timeout)
    {
        auto patchedOptions = options;
        patchedOptions.Timeout = timeout;
        return patchedOptions;
    }

    template <typename TResult>
    TFuture<TResult> ExecuteWithRetries(const TCallback<TFuture<TResult>(TDuration)>& callback)
    {
        return State_->RunWithRetries<TResult>(callback, Component_, Logger);
    }

#define RETRYING_METHOD(returnType, method, signature, args)                                      \
    returnType method signature override                                                          \
    {                                                                                             \
        return ExecuteWithRetries(                                                                \
            BIND([=, this, this_ = MakeStrong(this), optionsCopy = options] (TDuration timeout) { \
                auto options = MakePatchedOptions(optionsCopy, timeout);                          \
                return Underlying_->method args;                                                  \
            }));                                                                                  \
    }

    RETRYING_METHOD(TFuture<TUnversionedLookupRowsResult>, LookupRows,
        (
            const NYPath::TYPath& path,
            NTableClient::TNameTablePtr nameTable,
            const TSharedRange<NTableClient::TLegacyKey>& keys,
            const TLookupRowsOptions& options),
        (path, nameTable, keys, options))

    RETRYING_METHOD(TFuture<TVersionedLookupRowsResult>, VersionedLookupRows,
        (
            const NYPath::TYPath& path,
            NTableClient::TNameTablePtr nameTable,
            const TSharedRange<NTableClient::TLegacyKey>& keys,
            const TVersionedLookupRowsOptions& options),
        (path, nameTable, keys, options))

    RETRYING_METHOD(TFuture<std::vector<TUnversionedLookupRowsResult>>, MultiLookupRows,
        (
            const std::vector<TMultiLookupSubrequest>& subrequests,
            const TMultiLookupOptions& options),
        (subrequests, options))

    RETRYING_METHOD(TFuture<TSelectRowsResult>, SelectRows,
        (
            const std::string& query,
            const TSelectRowsOptions& options),
        (query, options))

    RETRYING_METHOD(TFuture<TPullRowsResult>, PullRows,
        (
            const NYPath::TYPath& path,
            const TPullRowsOptions& options),
        (path, options))

    RETRYING_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullQueue,
        (
            const NYPath::TRichYPath& queuePath,
            i64 offset,
            int partitionIndex,
            const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
            const TPullQueueOptions& options),
        (queuePath, offset, partitionIndex, rowBatchReadOptions, options))

    RETRYING_METHOD(TFuture<NQueueClient::IQueueRowsetPtr>, PullQueueConsumer,
        (
            const NYPath::TRichYPath& consumerPath,
            const NYPath::TRichYPath& queuePath,
            std::optional<i64> offset,
            int partitionIndex,
            const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
            const TPullQueueConsumerOptions& options),
        (consumerPath, queuePath, offset, partitionIndex, rowBatchReadOptions, options))

#undef RETRYING_METHOD

protected:
    const NLogging::TLogger Logger;

private:
    const std::string Component_;
    TCommonStatePtr State_;
    const NTransactionClient::ITimestampProviderPtr RetryableTimestampProvider_;
};

DEFINE_REFCOUNTED_TYPE(TRetryableClient)

////////////////////////////////////////////////////////////////////////////////

IRetryableClientPtr CreateRetryableClient(
    const NApi::IClientPtr& underlyingClient,
    const IInvokerPtr& invoker,
    const IStatusProfilerPtr& statusProfiler,
    const NLogging::TLogger& logger)
{
    YT_VERIFY(underlyingClient);
    YT_VERIFY(invoker);
    YT_VERIFY(statusProfiler);
    auto state = New<TCommonState>();
    state->Invoker = invoker;
    state->DynamicSpec.Store(New<TDynamicRetryableClientSpec>());
    state->StatusProfiler = statusProfiler;
    state->UnderlyingClient = underlyingClient;
    return New<TRetryableClient>(logger, state);
}

////////////////////////////////////////////////////////////////////////////////

bool IsFlowRetriableError(const TError& error, bool retryProxyBanned)
{
    return NApi::NRpcProxy::IsRetriableError(error, retryProxyBanned) ||
        error.FindMatching(NTabletClient::EErrorCode::AllWritesDisabled) ||
        error.FindMatching(NTabletClient::EErrorCode::InvalidMountRevision) ||
        error.FindMatching(NTabletClient::EErrorCode::SyncReplicaIsNotInSyncMode) ||
        error.FindMatching(NTabletClient::EErrorCode::SyncReplicaIsNotKnown) ||
        error.FindMatching(NTabletClient::EErrorCode::SyncReplicaIsNotWritten) ||
        error.FindMatching(NChunkClient::EErrorCode::ReaderRetryCountLimitExceeded) ||
        error.FindMatching(NChunkClient::EErrorCode::ReaderTimeout) ||
        // A participant tablet cell can transiently fail to prepare a transaction (e.g. cell restart or leader switch),
        // wrapping an inner NoSuchTransaction; this aborts the 2PC, so the commit can be safely retried.
        error.FindMatching(NTransactionClient::EErrorCode::ParticipantFailedToPrepare) ||
        // COMPAT(pechatnov): Temporary fallback for old binaries that don't use the error codes yet
        // (NChunkClient::EErrorCode::ReaderRetryCountLimitExceeded + NChunkClient::EErrorCode::ReaderTimeout).
        error.FindMatching([] (const TError& error) {
            return error.GetMessage().find("Chunk fragment reader has exceeded") != std::string::npos;
        }) ||
        // COMPAT(pechatnov): Temporary fallback for old binaries that don't use the error codes yet.
        // Codes will be added in YT-20959.
        error.FindMatching([] (const TError& error) {
            return error.GetMessage().find("Cannot write into tablet since it is a smooth movement") != std::string::npos;
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
