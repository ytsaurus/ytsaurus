#include "helpers.h"

#include "client.h"
#include "payload.h"

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NOrm::NClient::NNative {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NConcurrency;
using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

void ValidateBatchSelectObjectOptions(const TSelectObjectsOptions& options)
{
    if (options.Offset) {
        THROW_ERROR_EXCEPTION("Offset is forbidden");
    }
    if (options.Limit && *options.Limit < 1) {
        THROW_ERROR_EXCEPTION("Limit must be at least 1");
    }
}

template <class TOptions, class TReadResult>
void ValidateReadResult(const TOptions& options, const TReadResult& readResult)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        options.Timestamp == readResult.Timestamp,
        "Unexpected timestamp: expected %v, but got %v",
        options.Timestamp,
        readResult.Timestamp);

    if constexpr (requires { readResult.ContinuationToken; }) {
        THROW_ERROR_EXCEPTION_UNLESS(
            readResult.ContinuationToken.has_value(),
            "Received null continuation token");
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TSubrequest, class TResult, class TOptions>
TFuture<std::vector<TResult>> BatchApplyUpdatesImpl(
    IClientPtr client,
    TFuture<TResult> (IClient::*method) (std::vector<TSubrequest>, const TOptions&),
    TStringBuf methodName,
    std::vector<TSubrequest> subrequests,
    NObjects::TTimestamp startTransactionTimestamp,
    int batchSize,
    NLogging::TLogger Logger,
    IInvokerPtr invoker,
    TOptions options)
{
    YT_LOG_INFO(
        "Applying updates (Method: %v, UpdateCount %v)",
        methodName,
        subrequests.size());

    if (!invoker) {
        invoker = GetCurrentInvoker();
    }

    auto asyncApplyBatch = [&] (TSharedRange<TSubrequest> subrequestBatch) {
        return BIND([
            client,
            method,
            options,
            startTransactionTimestamp,
            subrequestBatch = std::move(subrequestBatch)
        ] () mutable
        {
            auto doApplyUpdatesBatch = [&] {
                return WaitFor((client.Get()->*method)(subrequestBatch.ToVector(), options))
                    .ValueOrThrow();
            };

            if (startTransactionTimestamp == NTransactionClient::NullTimestamp) {
                return doApplyUpdatesBatch();
            }

            options.TransactionId = WaitFor(client->StartTransaction(TStartTransactionOptions{
                .StartTimestamp = startTransactionTimestamp,
            }))
                .ValueOrThrow()
                .TransactionId;

            auto result = doApplyUpdatesBatch();

            WaitFor(client->CommitTransaction(options.TransactionId))
                .ThrowOnError();

            return result;
        })
            .AsyncVia(invoker)
            .Run();
    };

    std::vector<TFuture<TResult>> asyncResults;
    auto subrequestsRange = MakeSharedRange(std::move(subrequests));
    while (!subrequestsRange.Empty()) {
        batchSize = std::min<int>(subrequestsRange.size(), batchSize);
        asyncResults.push_back(asyncApplyBatch(subrequestsRange.Slice(0, batchSize)));
        subrequestsRange = subrequestsRange.Slice(batchSize, subrequestsRange.size());
    }

    auto combineResults = [methodName, Logger = std::move(Logger)] (
        std::vector<TErrorOr<TResult>>&& maybeResults)
    {
        std::vector<TError> innerErrors;
        std::vector<TResult> results;
        for (auto& maybeResult : maybeResults) {
            if (!maybeResult.IsOK()) {
                innerErrors.push_back(TError(std::move(maybeResult)));
            } else {
                results.push_back(std::move(maybeResult.Value()));
            }
        }
        if (!innerErrors.empty()) {
            THROW_ERROR_EXCEPTION("Failed to apply all updates for method %v", methodName)
                << innerErrors;
        }

        YT_LOG_INFO("Successfully applied updates (MethodName: %v)", methodName);
        return results;
    };

    return AllSet(std::move(asyncResults)).ApplyUnique(BIND(combineResults));
}

////////////////////////////////////////////////////////////////////////////////

template <class TReadResult, CInvocable<TFuture<TReadResult>(std::optional<int>)> TTryReadBatch>
auto ReadBatchWithRetries(
    TTryReadBatch&& tryReadBatch,
    const NLogging::TLogger& Logger,
    const std::optional<TAdaptiveBatchSizeOptions>& adaptiveBatchOptions,
    std::optional<int> batchSize)
{
    YT_VERIFY(!adaptiveBatchOptions.has_value() || batchSize.has_value());

    struct TResult
    {
        TReadResult ReadResult;
        std::optional<int> SuccessfulBatchSize;
        std::optional<int> NewBatchSize;
    };

    for (int attemptNumber = 0; ; ++attemptNumber) {
        auto batchResultOrError = WaitFor(tryReadBatch(batchSize));
        if (batchResultOrError.IsOK()) {
            TResult result{
                .ReadResult = std::move(batchResultOrError).Value(),
                .SuccessfulBatchSize = batchSize,
            };

            if (adaptiveBatchOptions.has_value()) {
                *batchSize += adaptiveBatchOptions->IncreasingAdditive;
                *batchSize = std::min(*batchSize, adaptiveBatchOptions->MaxBatchSize);
            }
            result.NewBatchSize = batchSize;

            return result;
        }

        if (!adaptiveBatchOptions.has_value()) {
            batchResultOrError.ThrowOnError();
        }

        int oldBatchSize = *batchSize;
        *batchSize /= adaptiveBatchOptions->DecreasingDivisor;

        if (*batchSize < adaptiveBatchOptions->MinBatchSize) {
            THROW_ERROR_EXCEPTION("Batch size is lower than minimum, not retrying")
                << TErrorAttribute("batch_size", *batchSize)
                << TErrorAttribute("min_batch_size", adaptiveBatchOptions->MinBatchSize)
                << batchResultOrError;
        }

        auto maxRetryCount = adaptiveBatchOptions->MaxConsecutiveRetryCount;
        if (maxRetryCount.has_value() && attemptNumber >= *maxRetryCount) {
            THROW_ERROR_EXCEPTION("Max retry count reached, not retrying")
                << TErrorAttribute("max_retry_count", *maxRetryCount)
                << batchResultOrError;
        }

        YT_LOG_DEBUG(batchResultOrError,
            "Failed to read objects, trying smaller limit (OldBatchSize: %v, NewBatchSize: %v)",
            oldBatchSize,
            *batchSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TSelectObjectsResult BatchSelectObjects(
    const IClientPtr& client,
    NObjects::TObjectTypeValue objectType,
    const TAttributeSelector& selector,
    TSelectObjectsOptions options,
    const NLogging::TLogger& logger,
    const std::optional<TAdaptiveBatchSizeOptions>& adaptiveBatchOptions)
{
    if (adaptiveBatchOptions.has_value()) {
        adaptiveBatchOptions->Validate();
    }
    ValidateBatchSelectObjectOptions(options);

    if (options.Timestamp == NObjects::NullTimestamp) {
        options.Timestamp = WaitFor(client->GenerateTimestamp())
            .ValueOrThrow()
            .Timestamp;
    }

    auto batchSize = options.Limit;
    if (adaptiveBatchOptions.has_value()) {
        if (batchSize.has_value()) {
            batchSize = std::min(*batchSize, adaptiveBatchOptions->MaxBatchSize);
        } else {
            batchSize = adaptiveBatchOptions->MaxBatchSize;
        }
    }

    TSelectObjectsResult result;
    while (!options.Limit.has_value() || *options.Limit > 0) {
        auto batchResult = ReadBatchWithRetries<TSelectObjectsResult>(
            [&] (std::optional<int> batchSize) {
                auto batchOptions = options;
                batchOptions.Limit = batchSize;
                return client->SelectObjects(objectType, selector, batchOptions);
            },
            logger,
            adaptiveBatchOptions,
            batchSize);

        auto successfulBatchSize = batchResult.SuccessfulBatchSize;
        if (options.Limit.has_value()) {
            YT_VERIFY(successfulBatchSize.has_value());
            YT_VERIFY(*successfulBatchSize <= *options.Limit);
            *options.Limit -= *successfulBatchSize;
        }
        if (batchSize.has_value()) {
            YT_VERIFY(batchResult.NewBatchSize.has_value());
            *batchSize = *batchResult.NewBatchSize;
            if (options.Limit.has_value()) {
                *batchSize = std::min(*batchSize, *options.Limit);
            }
        }

        auto& readResult = batchResult.ReadResult;
        ValidateReadResult(options, readResult);
        auto resultsSize = std::ssize(readResult.Results);

        result.Timestamp = readResult.Timestamp;
        result.Results.insert(
            result.Results.end(),
            std::make_move_iterator(readResult.Results.begin()),
            std::make_move_iterator(readResult.Results.end()));
        result.ContinuationToken = std::move(readResult.ContinuationToken);

        if (!successfulBatchSize.has_value() || resultsSize < *successfulBatchSize) {
            break;
        }
        options.ContinuationToken = result.ContinuationToken;
    }

    return result;
}

TGetObjectsResult BatchGetObjects(
    const IClientPtr& client,
    const std::vector<TObjectIdentity>& objectIdentities,
    NObjects::TObjectTypeValue objectType,
    const TAttributeSelector& selector,
    TGetObjectOptions options,
    const NLogging::TLogger& logger,
    const std::optional<TAdaptiveBatchSizeOptions>& adaptiveBatchOptions)
{
    if (adaptiveBatchOptions.has_value()) {
        adaptiveBatchOptions->Validate();
    }

    if (options.Timestamp == NObjects::NullTimestamp) {
        options.Timestamp = WaitFor(client->GenerateTimestamp())
            .ValueOrThrow()
            .Timestamp;
    }

    int batchSize = std::ssize(objectIdentities);
    if (adaptiveBatchOptions.has_value()) {
        batchSize = std::min(batchSize, adaptiveBatchOptions->MaxBatchSize);
    }

    TGetObjectsResult result;
    int objectsRemained = std::ssize(objectIdentities);
    while (objectsRemained > 0) {
        auto batchResult = ReadBatchWithRetries<TGetObjectsResult>(
            [&] (std::optional<int> batchSize) {
                YT_VERIFY(batchSize.has_value());
                return client->GetObjects(
                    {
                        objectIdentities.end() - objectsRemained,
                        objectIdentities.end() - objectsRemained + *batchSize,
                    },
                    objectType,
                    selector,
                    options);
            },
            logger,
            adaptiveBatchOptions,
            batchSize);

        YT_VERIFY(batchResult.SuccessfulBatchSize.has_value());
        YT_VERIFY(batchResult.NewBatchSize.has_value());

        objectsRemained -= *batchResult.SuccessfulBatchSize;
        batchSize = std::min(*batchResult.NewBatchSize, objectsRemained);

        auto& readResult = batchResult.ReadResult;
        ValidateReadResult(options, readResult);

        result.Timestamp = readResult.Timestamp;
        result.Subresults.insert(
            result.Subresults.end(),
            std::make_move_iterator(readResult.Subresults.begin()),
            std::make_move_iterator(readResult.Subresults.end()));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_BATCH_APPLY_UPDATES_SPECIALIZATION(methodType) \
template <> \
TFuture<std::vector<T##methodType##sResult>> BatchApplyUpdates< \
    T##methodType##sSubrequest, \
    T##methodType##sResult, \
    T##methodType##Options>( \
    IClientPtr client, \
    TFuture<T##methodType##sResult> (IClient::*method) ( \
        std::vector<T##methodType##sSubrequest>, \
        const T##methodType##Options&), \
    std::vector<T##methodType##sSubrequest> subrequests, \
    NObjects::TTimestamp startTransactionTimestamp, \
    int batchSize, \
    NLogging::TLogger logger, \
    IInvokerPtr invoker, \
    T##methodType##Options options) \
{ \
    return BatchApplyUpdatesImpl( \
        std::move(client), \
        method, \
        #methodType"s", \
        std::move(subrequests), \
        startTransactionTimestamp, \
        batchSize, \
        std::move(logger), \
        std::move(invoker), \
        std::move(options)); \
}

DEFINE_BATCH_APPLY_UPDATES_SPECIALIZATION(CreateObject);
DEFINE_BATCH_APPLY_UPDATES_SPECIALIZATION(UpdateObject);
DEFINE_BATCH_APPLY_UPDATES_SPECIALIZATION(RemoveObject);

#undef DEFINE_BATCH_APPLY_UPDATES_SPECIALIZATION

////////////////////////////////////////////////////////////////////////////////

void BatchTouchIndex(
    const IClientPtr& client,
    const std::vector<TObjectIdentity>& objectIdentities,
    NObjects::TObjectTypeValue objectType,
    const TString& indexName,
    bool ignoreNonexistent)
{
    std::vector<TUpdateObjectsSubrequest> subrequests;
    subrequests.reserve(objectIdentities.size());
    std::vector<TUpdate> updates{TSetUpdate{
        "/control/touch_index",
        BuildYsonPayload(BIND([&] (NYT::NYson::IYsonConsumer* consumer) {
            NYT::NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("index_names")
                    .BeginList()
                        .Item().Value(indexName)
                    .EndList()
                .EndMap();
        }))}};
    for (const auto& objectIdentity : objectIdentities) {
        subrequests.push_back({
            .ObjectIdentity = objectIdentity,
            .ObjectType = objectType,
            .Updates = updates});
    }
    WaitFor(client->UpdateObjects(
        subrequests,
        TUpdateObjectOptions{.IgnoreNonexistent=ignoreNonexistent}))
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

TYsonPayload BuildYsonPayload(TYsonCallback producerCallback)
{
    TYsonProducer producer(std::move(producerCallback));
    return TYsonPayload{ConvertToYsonString(producer, EYsonFormat::Binary)};
}

////////////////////////////////////////////////////////////////////////////////

void ValidateAttributeList(
    const TAttributeList& attributeList,
    EPayloadFormat format,
    size_t selectorSize,
    bool fetchTimestamps,
    bool ignoreNonexistent,
    bool fetchRootObject)
{
    static TStringBuf valuePayloadsName("value payloads");
    static TStringBuf timestampsName("timestamps");

    auto validateRepeatedFieldSize = [] (
        const auto& repeatedField,
        size_t expectedSize,
        bool ignoreNonexistent,
        TStringBuf name)
    {
        if (expectedSize != repeatedField.size()) {
            if (!(ignoreNonexistent && repeatedField.empty())) {
                THROW_ERROR_EXCEPTION("Incorrect number of %v: expected %v, but got %v",
                    name,
                    ignoreNonexistent && expectedSize
                        ? Format("%v or 0", expectedSize)
                        : Format("%v", expectedSize),
                    repeatedField.size());
            }
        }
    };

    validateRepeatedFieldSize(
        attributeList.ValuePayloads,
        /*expectedSize*/ fetchRootObject ? 1ul : selectorSize,
        ignoreNonexistent,
        valuePayloadsName);

    for (const auto& valuePayload : attributeList.ValuePayloads) {
        ValidatePayloadFormat(format, valuePayload);
    }

    validateRepeatedFieldSize(
        attributeList.Timestamps,
        /*expectedSize*/ fetchTimestamps ? selectorSize : 0,
        ignoreNonexistent,
        timestampsName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
