#pragma once

#include "public.h"

#include "request.h"
#include "response.h"

#include <yt/core/yson/producer.h>

namespace NYP::NClient::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

TSelectObjectsResult BatchSelectObjects(
    const IClientPtr& client,
    EObjectType objectType,
    const TAttributeSelector& selector,
    TSelectObjectsOptions options);

////////////////////////////////////////////////////////////////////////////////

TYsonPayload BuildYsonPayload(NYT::NYson::TYsonCallback producerCallback);

////////////////////////////////////////////////////////////////////////////////

TFuture<TUpdateNodeHfsmStateResult> UpdateNodeHfsmState(
    const IClientPtr& client,
    TObjectId nodeId,
    EHfsmState state,
    TString message);

////////////////////////////////////////////////////////////////////////////////

struct TRequestPodEvictionOptions
{
    bool ValidateDisruptionBudget = true;
    EEvictionReason Reason = EEvictionReason::Client;
};

TFuture<TRequestPodEvictionResult> RequestPodEviction(
    const IClientPtr& client,
    TObjectId podId,
    TString message,
    TRequestPodEvictionOptions options = TRequestPodEvictionOptions());

////////////////////////////////////////////////////////////////////////////////

TFuture<TAbortPodEvictionResult> AbortPodEviction(
    const IClientPtr& client,
    TObjectId podId,
    TString message,
    const TTransactionId& transactionId = TTransactionId());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
