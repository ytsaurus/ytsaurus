#include "helpers.h"

#include "client.h"
#include "payload.h"
#include "private.h"

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/yson/writer.h>
#include <yt/core/ytree/fluent.h>

namespace NYP::NClient::NApi::NNative {

using namespace NYT::NConcurrency;
using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TSelectObjectsResult BatchSelectObjects(
    const IClientPtr& client,
    EObjectType objectType,
    const TAttributeSelector& selector,
    TSelectObjectsOptions options)
{
    if (options.Offset) {
        THROW_ERROR_EXCEPTION("Offset is forbidden");
    }
    if (!options.Limit) {
        THROW_ERROR_EXCEPTION("Limit is required");
    }
    if (*options.Limit <= 0) {
        THROW_ERROR_EXCEPTION("Limit must be positive");
    }

    TSelectObjectsResult result;
    auto moveBatchResult = [&result, &options] (TSelectObjectsResult& batchResult) {
        if (options.Timestamp != batchResult.Timestamp) {
            THROW_ERROR_EXCEPTION("Unexpected timestamp: expected %llx, but got %llx",
                options.Timestamp,
                batchResult.Timestamp);
        }
        result.Timestamp = batchResult.Timestamp;
        result.Results.insert(
            result.Results.end(),
            std::make_move_iterator(batchResult.Results.begin()),
            std::make_move_iterator(batchResult.Results.end()));
        result.ContinuationToken = batchResult.ContinuationToken;

        if (!result.ContinuationToken) {
            THROW_ERROR_EXCEPTION("Received null continuation token");
        }
    };

    if (options.Timestamp == NullTimestamp) {
        options.Timestamp = WaitFor(client->GenerateTimestamp())
            .ValueOrThrow()
            .Timestamp;
    }

    bool shouldContinue = true;
    while (shouldContinue) {
        options.ContinuationToken = result.ContinuationToken;

        auto batchResult = WaitFor(client->SelectObjects(
            objectType,
            selector,
            options))
            .ValueOrThrow();

        shouldContinue = (batchResult.Results.size() >= options.Limit);

        moveBatchResult(batchResult);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TYsonPayload BuildYsonPayload(TYsonCallback producerCallback)
{
    TYsonProducer producer(std::move(producerCallback));
    return TYsonPayload{ConvertToYsonString(producer, EYsonFormat::Binary)};
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TUpdateNodeHfsmStateResult> UpdateNodeHfsmState(
    const IClientPtr& client,
    TObjectId nodeId,
    EHfsmState state,
    TString message)
{
    TSetUpdate update{
        "/control/update_hfsm_state",
        BuildYsonPayload(BIND([&] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("state").Value(static_cast<NProto::EHfsmState>(state))
                    .Item("message").Value(std::move(message))
                .EndMap();
        }))};
    return client->UpdateObject(
        std::move(nodeId),
        EObjectType::Node,
        std::vector<TUpdate>{std::move(update)});
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TRequestPodEvictionResult> RequestPodEviction(
    const IClientPtr& client,
    TObjectId podId,
    TString message,
    TRequestPodEvictionOptions options,
    const TTransactionId& transactionId)
{
    TSetUpdate update{
        "/control/request_eviction",
        BuildYsonPayload(BIND([&] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("message").Value(std::move(message))
                    .Item("validate_disruption_budget").Value(options.ValidateDisruptionBudget)
                    .Item("reason").Value(options.Reason)
                .EndMap();
        }))};
    return client->UpdateObject(
        std::move(podId),
        EObjectType::Pod,
        std::vector<TUpdate>{std::move(update)},
        /*attributeTimestampPrerequisites = */ {},
        transactionId);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TAbortPodEvictionResult> AbortPodEviction(
    const IClientPtr& client,
    TObjectId podId,
    TString message,
    const TTransactionId& transactionId)
{
    TSetUpdate update{
        "/control/abort_eviction",
        BuildYsonPayload(BIND([&] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("message").Value(std::move(message))
                .EndMap();
        }))};
    return client->UpdateObject(
        std::move(podId),
        EObjectType::Pod,
        std::vector<TUpdate>{std::move(update)},
        /* attributeTimestampPrerequisites */ {},
        transactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
