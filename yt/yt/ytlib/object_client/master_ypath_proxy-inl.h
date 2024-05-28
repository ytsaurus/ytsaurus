#ifndef MASTER_YPATH_PROXY_INL_H_
#error "Direct inclusion of this file is not allowed, include master_ypath_proxy.h"
// For the sake of sane code completion.
#include "master_ypath_proxy.h"
#endif

#include "object_service_proxy.h"

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

static const int VectorizedReadSubbatchSize = 100;

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class TResponse>
class TMasterYPathProxy::TVectorizedRequestBatcher
{
private:
    using TTypedRequest = NYTree::TTypedYPathRequest<TRequest, TResponse>;
    using TTypedResponse = NYTree::TTypedYPathResponse<TRequest, TResponse>;
    using TTypedRequestPtr = TIntrusivePtr<NYTree::TTypedYPathRequest<TRequest, TResponse>>;
    using TTypedResponsePtr = TIntrusivePtr<NYTree::TTypedYPathResponse<TRequest, TResponse>>;

public:
    TVectorizedRequestBatcher(
        const NApi::NNative::IClientPtr& client,
        const TTypedRequestPtr& templateRequest,
        TRange<TObjectId> objectIds)
        : ObjectCount_(objectIds.size())
        , SerializedTemplateRequest_(templateRequest->Serialize())
        , Client_(client)
    {
        for (auto objectId : objectIds) {
            auto cellTag = CellTagFromId(objectId);
            CellTagToObjectId_[cellTag].push_back(objectId);
        }

        for (auto& [cellTag, objectIds] : CellTagToObjectId_) {
            SortUnique(objectIds);
        }
    }

    TFuture<THashMap<TObjectId, TErrorOr<TTypedResponsePtr>>> Invoke()
    {
        for (const auto& [cellTag, objectIds] : CellTagToObjectId_) {
            PrepareRequests(cellTag, objectIds);
        }

        return AllSucceeded(AsyncResults_)
            .ApplyUnique(BIND([objectCount = ObjectCount_] (std::vector<TObjectServiceProxy::TRspExecuteBatchPtr>&& results) {
                THashMap<TObjectId, TErrorOr<TTypedResponsePtr>> objectIdToResponse;
                objectIdToResponse.reserve(objectCount);

                for (const auto& batchRsp : results) {
                    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRsp), "Error getting requested information from master");

                    for (const auto& rspOrError : batchRsp->GetResponses<TMasterYPathProxy::TRspVectorizedRead>()) {
                        const auto& rsp = rspOrError.Value();
                        const auto& attachments = rsp->Attachments();

                        int currentPartIndex = 0;
                        for (const auto& subresponse : rsp->subresponses()) {
                            auto partCount = subresponse.part_count();

                            TSharedRefArrayBuilder subrequestPartsBuilder(partCount);
                            for (int partIndex = 0; partIndex < partCount; ++partIndex) {
                                subrequestPartsBuilder.Add(attachments[currentPartIndex++]);
                            }
                            auto innerMessage = subrequestPartsBuilder.Finish();

                            auto objectId = FromProto<TObjectId>(subresponse.object_id());
                            try {
                                TTypedResponsePtr innerResponse = New<TTypedResponse>();
                                innerResponse->Deserialize(innerMessage);
                                EmplaceOrCrash(objectIdToResponse, objectId, std::move(innerResponse));
                            } catch (const std::exception& ex) {
                                EmplaceOrCrash(objectIdToResponse, objectId, ex);
                            }
                        }
                    }
                }
                return objectIdToResponse;
            }).AsyncVia(GetCurrentInvoker()));
    }

private:
    THashMap<TCellTag, std::vector<TObjectId>> CellTagToObjectId_;
    const int ObjectCount_;
    const TSharedRefArray SerializedTemplateRequest_;
    const NApi::NNative::IClientPtr Client_;

    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> AsyncResults_;

    void PrepareRequests(TCellTag cellTag, const std::vector<TObjectId>& objectIds)
    {
        auto proxy = CreateObjectServiceReadProxy(
            Client_,
            NApi::EMasterChannelKind::Follower,
            cellTag);

        // NB: batch request is still useful here because server will execute requests, packed in batch request in parallel.
        // Additionally it's better to send several smaller requests to make sure they don't occupy master for too long.
        auto batchReq = proxy.ExecuteBatch();
        auto objectIdsRange = TRange(objectIds);
        int startOffset = 0;
        while (startOffset < std::ssize(objectIds)) {
            auto endOffset = std::min<int>(startOffset + VectorizedReadSubbatchSize, std::ssize(objectIds));
            auto subrequest = FormVectorizedReadRequest(objectIdsRange.Slice(startOffset, endOffset));
            startOffset = endOffset;

            batchReq->AddRequest(std::move(subrequest));
        }

        AsyncResults_.push_back(batchReq->Invoke());
    }

    TMasterYPathProxy::TReqVectorizedReadPtr FormVectorizedReadRequest(TRange<TObjectId> objectIds)
    {
        auto request = TMasterYPathProxy::VectorizedRead();

        // Fill template request info.
        auto& attachments = request->Attachments();
        attachments.insert(
            attachments.end(),
            SerializedTemplateRequest_.Begin(),
            SerializedTemplateRequest_.End());
        request->set_template_request_part_count(SerializedTemplateRequest_.size());

        // Fill object IDs info.
        ToProto(request->mutable_object_ids(), objectIds);
        return request;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
