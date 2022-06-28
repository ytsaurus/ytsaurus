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
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TCellTagBatch
{
    TObjectServiceProxy::TReqExecuteBatchPtr Request;
    TFuture<TObjectServiceProxy::TRspExecuteBatchPtr> Response;
};

void ExecuteBatchRequests(THashMap<TCellTag, TCellTagBatch>* batchRequest)
{
    std::vector<TFuture<void>> futures;
    for (auto& [cellTag, batchRequest] : *batchRequest) {
        batchRequest.Response = batchRequest.Request->Invoke();
        futures.push_back(batchRequest.Response.AsVoid());
    }

    WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
}

THashMap<TObjectId, IAttributeDictionaryPtr> FetchAttributesByCellTags(
    const NApi::NNative::IClientPtr& client,
    const std::vector<std::pair<TObjectId, TCellTag>>& objectIdsWithCellTags,
    const std::vector<TString>& attributeKeys)
{
    // TODO(alexelex): Receive list of error codes to skip them.

    THashMap<TCellTag, TCellTagBatch> batchRequest;
    for (const auto& [objectId, cellTag] : objectIdsWithCellTags) {
        auto req = TTableYPathProxy::Get(FromObjectId(objectId) + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

        TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(EMasterChannelKind::Follower, cellTag));
        auto it = batchRequest.emplace(cellTag, TCellTagBatch{proxy.ExecuteBatch(), {}}).first;
        it->second.Request->AddRequest(req, ToString(objectId));
    }

    ExecuteBatchRequests(&batchRequest);

    THashMap<TObjectId, IAttributeDictionaryPtr> responses;
    for (const auto& [objectId, cellTag] : objectIdsWithCellTags) {
        const auto& batchReq = batchRequest[cellTag].Response.Get().Value();
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

THashMap<TObjectId, IAttributeDictionaryPtr> FetchTableAttributes(
    const NApi::NNative::IClientPtr& client,
    const THashSet<TTableId>& tableIds,
    const std::vector<TString>& attributeKeys,
    const THashMap<TTableId, TTablePtr>& Tables)
{
    std::vector<std::pair<TObjectId, TCellTag>> objectIdsWithCellTags;
    objectIdsWithCellTags.reserve(std::ssize(tableIds));
    for (auto tableId : tableIds) {
        YT_VERIFY(IsTableType(TypeFromId(tableId)));
        objectIdsWithCellTags.emplace_back(tableId, GetOrCrash(Tables, tableId)->ExternalCellTag);
    }
    return FetchAttributesByCellTags(client, objectIdsWithCellTags, attributeKeys);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
