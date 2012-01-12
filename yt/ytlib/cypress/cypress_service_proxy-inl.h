#ifndef CYPRESS_SERVICE_PROXY_INL_H_
#error "Direct inclusion of this file is not allowed, include cypress_service_proxy.h"
#endif

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedRequest>
TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
TCypressServiceProxy::Execute(
    TTypedRequest* innerRequest,
    const NObjectServer::TObjectId& objectId,
    const TTransactionId& transactionId)
{
    return Execute(
        innerRequest,
        YPathFromObjectId(objectId),
        transactionId);
}

template <class TTypedRequest>
TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
TCypressServiceProxy::Execute(
    TTypedRequest* innerRequest,
    const NYTree::TYPath& path,
    const TTransactionId& transactionId)
{
    typedef typename TTypedRequest::TTypedResponse TTypedResponse;

    if (!path.empty()) {
        innerRequest->SetPath(path);
    }

    auto outerRequest = Execute();
    outerRequest->set_transaction_id(transactionId.ToProto());

    auto innerRequestMessage = innerRequest->Serialize();
    NYTree::WrapYPathRequest(~outerRequest, ~innerRequestMessage);

    return outerRequest->Invoke()->Apply(FromFunctor(
        [] (TRspExecute::TPtr outerResponse) -> TIntrusivePtr<TTypedResponse>
        {
            auto innerResponse = New<TTypedResponse>();
            auto error = outerResponse->GetError();
            if (error.IsOK()) {
                auto innerResponseMessage = NYTree::UnwrapYPathResponse(~outerResponse);
                innerResponse->Deserialize(~innerResponseMessage);
            } else if (NRpc::IsRpcError(error)) {
                innerResponse->SetError(error);
            } else {
                innerResponse->SetError(TError(
                    NYTree::EYPathErrorCode(NYTree::EYPathErrorCode::GenericError),
                    outerResponse->GetError().GetMessage()));
            }
            return innerResponse;
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
