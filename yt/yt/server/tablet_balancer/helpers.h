#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/table.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/tablet_client/master_tablet_service_proxy.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TCellTagRequest
{
    NTabletClient::TMasterTabletServiceProxy::TReqGetTableBalancingAttributesPtr Request;
    TFuture<NTabletClient::TMasterTabletServiceProxy::TRspGetTableBalancingAttributesPtr> Response;

    i64 GetSize() const;
};

struct TCellTagBatch
{
    NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr Request;
    TFuture<NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr> Response;

    i64 GetSize() const;
};

template <typename TRequest>
void ExecuteRequestsToCellTags(THashMap<NObjectClient::TCellTag, TRequest>* batchRequest, const IMulticellThrottlerPtr& throttler);

THashMap<NObjectClient::TCellTag, TCellTagRequest> FetchTableAttributes(
    const NApi::NNative::IClientPtr& client,
    const THashSet<TTableId>& tableIds,
    const THashSet<TTableId>& tableIdsToFetchPivotKeys,
    const THashMap<TTableId, NObjectClient::TCellTag>& tableIdToCellTag,
    const IMulticellThrottlerPtr& throttler,
    std::function<void(const NTabletClient::TMasterTabletServiceProxy::TReqGetTableBalancingAttributesPtr&)> prepareRequestProto);

THashMap<NTabletClient::TTableReplicaId, NTabletClient::ETableReplicaMode> FetchChaosTableReplicaModes(
    const NApi::NNative::IClientPtr& client,
    const THashSet<NTabletClient::TTableReplicaId>& replicaIds);

//! Fetch attributes using CellTag from ObjectId.
THashMap<NObjectClient::TObjectId, NYTree::IAttributeDictionaryPtr> FetchAttributes(
    const NApi::NNative::IClientPtr& client,
    const THashSet<NObjectClient::TObjectId>& objectIds,
    const std::vector<std::string>& attributeKeys,
    const IMulticellThrottlerPtr& throttler);

TInstant TruncatedNow();

////////////////////////////////////////////////////////////////////////////////

NApi::NNative::IClientPtr GetClusterClient(
    const NHiveClient::TClientDirectoryPtr& clientDirectory,
    const TClusterName& clusterName);

using TTablePerformanceCountersMap = THashMap<TTableId, THashMap<TTabletId, NTableClient::TUnversionedOwningRow>>;
std::tuple<TTablePerformanceCountersMap, NQueryClient::TTableSchemaPtr> FetchPerformanceCountersAndSchemaFromTable(
    const NApi::NNative::IClientPtr& client,
    const THashSet<TTableId>& tableIds,
    const NYPath::TYPath& tablePath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
