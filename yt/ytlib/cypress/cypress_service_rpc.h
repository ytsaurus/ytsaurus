#pragma once

#include "common.h"
#include "cypress_service_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"
#include "../ytree/ypath_client.h"
#include "../ytree/ypath_detail.h"
#include "../transaction_server/common.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

// TODO: move impl to inl
class TCypressServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TCypressServiceProxy> TPtr;

    RPC_DECLARE_PROXY(CypressService,
        ((NoSuchTransaction)(1))
        ((NoSuchRootNode)(2))
    );

    TCypressServiceProxy(NRpc::IChannel* channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NProto, Execute);

    template <class TTypedRequest>
    TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
    Execute(
        NYTree::TYPath path,
        const NTransaction::TTransactionId& transactionId,
        TTypedRequest* innerRequest)
    {
        innerRequest->SetPath(path);
        auto outerRequest = Execute();
        outerRequest->SetTransactionId(transactionId.ToProto());
        return Execute<TTypedRequest, typename TTypedRequest::TTypedResponse>(
            ~outerRequest,
            innerRequest);
    }

    template <class TTypedRequest>
    TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
    Execute(
        const TNodeId& rootNodeId,
        const NTransaction::TTransactionId& transactionId,
        TTypedRequest* innerRequest)
    {
        innerRequest->SetPath("/");
        auto outerRequest = Execute();
        outerRequest->SetRootNodeId(rootNodeId.ToProto());
        outerRequest->SetTransactionId(transactionId.ToProto());
        return Execute<TTypedRequest, typename TTypedRequest::TTypedResponse>(
            ~outerRequest,
            innerRequest);
    }

private:
    template <class TTypedRequest, class TTypedResponse>
    TIntrusivePtr< TFuture< TIntrusivePtr<TTypedResponse> > >
    Execute(
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
                } else if (error.IsRpcError()) {
                    innerResponse->SetError(error);    
                } else {
                    innerResponse->SetError(NRpc::TError(
                        NYTree::EYPathErrorCode(NYTree::EYPathErrorCode::GenericError),
                        outerResponse->GetError().GetMessage()));
                }
                return innerResponse;
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
