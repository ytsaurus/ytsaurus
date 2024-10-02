#pragma once

#include "public.h"

#include "request.h"
#include "response.h"

#include <yt/yt/core/yson/producer.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

TSelectObjectsResult BatchSelectObjects(
    const IClientPtr& client,
    NObjects::TObjectTypeValue objectType,
    const TAttributeSelector& selector,
    TSelectObjectsOptions options,
    const NLogging::TLogger& logger,
    const std::optional<TAdaptiveBatchSizeOptions>& adaptiveBatchOptions = std::nullopt);

TGetObjectsResult BatchGetObjects(
    const IClientPtr& client,
    const std::vector<TObjectIdentity>& objectIdentities,
    NObjects::TObjectTypeValue objectType,
    const TAttributeSelector& selector,
    TGetObjectOptions options,
    const NLogging::TLogger& logger,
    const std::optional<TAdaptiveBatchSizeOptions>& adaptiveBatchOptions = std::nullopt);

template <class TSubrequest, class TResult, class TOptions>
TFuture<std::vector<TResult>> BatchApplyUpdates(
    IClientPtr client,
    TFuture<TResult> (IClient::*method) (std::vector<TSubrequest>, const TOptions&),
    std::vector<TSubrequest> subrequests,
    NObjects::TTimestamp startTransactionTimestamp,
    int batchSize,
    NLogging::TLogger logger,
    IInvokerPtr invoker = nullptr,
    TOptions options = {});

void BatchTouchIndex(
    const IClientPtr& client,
    const std::vector<TObjectIdentity>& objectIdentities,
    NObjects::TObjectTypeValue objectType,
    const TString& indexName,
    bool ignoreNonexistent);

////////////////////////////////////////////////////////////////////////////////

TYsonPayload BuildYsonPayload(NYT::NYson::TYsonCallback producerCallback);

////////////////////////////////////////////////////////////////////////////////

void ValidateAttributeList(
    const TAttributeList& attributeList,
    EPayloadFormat format,
    size_t selectorSize,
    bool fetchTimestamps,
    bool ignoreNonexistent,
    bool fetchRootObject);

////////////////////////////////////////////////////////////////////////////////

template <bool ForceYsonPayloadFormat, class T>
TSetUpdate BuildSetAttributeUpdate(NYPath::TYPath attributePath, const T& targetValue);

template <class T>
TSetUpdate BuildSetAttributeUpdate(NYPath::TYPath attributePath, const T& targetValue);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
