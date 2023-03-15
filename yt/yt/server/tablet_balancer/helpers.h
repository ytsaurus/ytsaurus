#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/table.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/tablet_client/master_tablet_service_proxy.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TCellTagRequest
{
    NTabletClient::TMasterTabletServiceProxy::TReqGetTableBalancingAttributesPtr Request;
    TFuture<NTabletClient::TMasterTabletServiceProxy::TRspGetTableBalancingAttributesPtr> Response;
};

struct TCellTagBatch
{
    NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr Request;
    TFuture<NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr> Response;
};

template <typename TRequest>
void ExecuteRequestsToCellTags(THashMap<NObjectClient::TCellTag, TRequest>* batchRequest);

THashMap<NObjectClient::TCellTag, TCellTagRequest> FetchTableAttributes(
    const NApi::NNative::IClientPtr& client,
    const THashSet<TTableId>& tableIds,
    const THashMap<TTableId, TTablePtr>& Tables,
    std::function<void(const NTabletClient::TMasterTabletServiceProxy::TReqGetTableBalancingAttributesPtr&)> prepareRequestProto);

//! Fetch attributes using CellTag from ObjectId.
THashMap<NObjectClient::TObjectId, NYTree::IAttributeDictionaryPtr> FetchAttributes(
    const NApi::NNative::IClientPtr& client,
    const THashSet<NObjectClient::TObjectId>& objectIds,
    const std::vector<TString>& attributeKeys);

TInstant TruncatedNow();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
