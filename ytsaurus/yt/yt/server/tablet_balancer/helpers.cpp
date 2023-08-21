#include "helpers.h"
#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/table.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

THashMap<TObjectId, IAttributeDictionaryPtr> FetchAttributesByCellTags(
    const NApi::NNative::IClientPtr& client,
    const std::vector<std::pair<TObjectId, TCellTag>>& objectIdsWithCellTags,
    const std::vector<TString>& attributeKeys)
{
    // TODO(alexelex): Receive list of error codes to skip them.

    THashMap<TCellTag, TCellTagBatch> batchRequests;
    for (const auto& [objectId, cellTag] : objectIdsWithCellTags) {
        auto req = TTableYPathProxy::Get(FromObjectId(objectId) + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

        auto proxy = CreateObjectServiceReadProxy(
            client,
            EMasterChannelKind::Follower,
            cellTag);
        auto it = batchRequests.emplace(cellTag, TCellTagBatch{proxy.ExecuteBatch(), {}}).first;
        it->second.Request->AddRequest(req, ToString(objectId));
    }

    ExecuteRequestsToCellTags(&batchRequests);

    THashMap<TObjectId, IAttributeDictionaryPtr> responses;
    for (const auto& [objectId, cellTag] : objectIdsWithCellTags) {
        const auto& batchReq = batchRequests[cellTag].Response.Get().Value();
        auto rspOrError = batchReq->GetResponse<TTableYPathProxy::TRspGet>(ToString(objectId));
        if (!rspOrError.IsOK() && rspOrError.GetCode() == NYTree::EErrorCode::ResolveError) {
            continue;
        }

        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
        auto attributes = ConvertToAttributes(TYsonString(rspOrError.Value()->value()));
        EmplaceOrCrash(responses, objectId, std::move(attributes));
    }

    return responses;
}

TInstant TruncateToMinutes(TInstant t)
{
    auto timeval = t.TimeVal();
    timeval.tv_usec = 0;
    timeval.tv_sec /= 60;
    timeval.tv_sec *= 60;
    return TInstant(timeval);
}

} // namespace

THashMap<TObjectId, IAttributeDictionaryPtr> FetchAttributes(
    const NApi::NNative::IClientPtr& client,
    const THashSet<TObjectId>& objectIds,
    const std::vector<TString>& attributeKeys)
{
    std::vector<std::pair<TObjectId, TCellTag>> objectIdsWithCellTags;
    objectIdsWithCellTags.reserve(std::ssize(objectIds));
    for (auto objectId : objectIds) {
        objectIdsWithCellTags.emplace_back(objectId, CellTagFromId(objectId));
    }
    return FetchAttributesByCellTags(client, objectIdsWithCellTags, attributeKeys);
}

THashMap<TCellTag, TCellTagRequest> FetchTableAttributes(
    const NApi::NNative::IClientPtr& client,
    const THashSet<TTableId>& tableIds,
    const THashMap<TTableId, TTablePtr>& Tables,
    std::function<void(const TMasterTabletServiceProxy::TReqGetTableBalancingAttributesPtr&)> prepareRequestProto)
{
    THashMap<TCellTag, TCellTagRequest> batchRequests;
    for (auto tableId : tableIds) {
        auto cellTag = GetOrCrash(Tables, tableId)->ExternalCellTag;
        TMasterTabletServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Follower, cellTag));
        auto [it, isNew] = batchRequests.emplace(cellTag, TCellTagRequest{proxy.GetTableBalancingAttributes(), {}});
        if (isNew) {
            prepareRequestProto(it->second.Request);
        }
        ToProto(it->second.Request->add_table_ids(), tableId);
    }

    ExecuteRequestsToCellTags(&batchRequests);
    return batchRequests;
}

TInstant TruncatedNow()
{
    return TruncateToMinutes(Now());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
