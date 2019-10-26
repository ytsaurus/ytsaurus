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

TFuture<TRequestPodEvictionResult> RequestPodEviction(
    const IClientPtr& client,
    TObjectId podId,
    TString message,
    bool validateDisruptionBudget);

////////////////////////////////////////////////////////////////////////////////

TFuture<TAbortPodEvictionResult> AbortPodEviction(
    const IClientPtr& client,
    TObjectId podId,
    TString message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
