#ifndef CYPRESS_SERVICE_RPC_INL_H_
#error "Direct inclusion of this file is not allowed, include cypress_service_rpc.h"
#endif

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedRequest>
TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
TCypressServiceProxy::Execute(
    NYTree::TYPath path,
    const NTransactionServer::TTransactionId& transactionId,
    TTypedRequest* innerRequest)
{
    innerRequest->SetPath(path);
    auto outerRequest = Execute();
    outerRequest->set_transactionid(transactionId.ToProto());
    return Execute<TTypedRequest, typename TTypedRequest::TTypedResponse>(
        ~outerRequest,
        innerRequest);
}

template <class TTypedRequest>
TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
TCypressServiceProxy::Execute(
    const TNodeId& rootNodeId,
    const NTransactionServer::TTransactionId& transactionId,
    TTypedRequest* innerRequest)
{
    innerRequest->SetPath("/");
    auto outerRequest = Execute();
    outerRequest->set_rootnodeid(rootNodeId.ToProto());
    outerRequest->set_transactionid(transactionId.ToProto());
    return Execute<TTypedRequest, typename TTypedRequest::TTypedResponse>(
        ~outerRequest,
        innerRequest);
}

template <class TTypedRequest, class TTypedResponse>
TIntrusivePtr< TFuture< TIntrusivePtr<TTypedResponse> > >
TCypressServiceProxy::Execute(
    TCypressServiceProxy::TReqExecute* outerRequest,
    TTypedRequest* innerRequest)
{
    auto innerRequestMessage = innerRequest->Serialize();
    NYTree::WrapYPathRequest(outerRequest, ~innerRequestMessage);
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
