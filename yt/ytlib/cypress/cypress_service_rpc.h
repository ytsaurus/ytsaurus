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

class TCypressServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TCypressServiceProxy> TPtr;

    RPC_DECLARE_PROXY(CypressService,
        ((NoSuchTransaction)(1))
        ((NavigationError)(2))
    );

    TCypressServiceProxy(NRpc::IChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NProto, Execute);
    RPC_PROXY_METHOD(NProto, GetNodeId);

    template <class TTypedRequest>
    TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
    Execute(
        const NTransaction::TTransactionId& transactionId,
        TIntrusivePtr<TTypedRequest> innerRequest)
    {
        auto outerRequest = Execute();
        outerRequest->SetTransactionId(transactionId.ToProto());
        return DoExecute<TTypedRequest, typename TTypedRequest::TTypedResponse>(outerRequest, innerRequest);
    }

private:
    template <class TTypedRequest, class TTypedResponse>
    TIntrusivePtr< TFuture< TIntrusivePtr<TTypedResponse> > >
    DoExecute(
        TReqExecute::TPtr outerRequest,
        TIntrusivePtr<TTypedRequest> innerRequest)
    {
        WrapYPathRequest(~outerRequest, ~innerRequest);
        return outerRequest->Invoke()->Apply(FromFunctor(
            [] (TRspExecute::TPtr outerResponse) -> TIntrusivePtr<TTypedResponse>
            {
                auto innerResponse = New<TTypedResponse>();
                auto error = outerResponse->GetError();
                if (error.IsOK()) {
                    UnwrapYPathResponse(~outerResponse, ~innerResponse);
                } else if (error.IsRpcError()) {
                    SetYPathErrorResponse(error, ~innerResponse);    
                } else {
                    SetYPathErrorResponse(
                        NRpc::TError(
                            NYTree::EYPathErrorCode(NYTree::EYPathErrorCode::GenericError),
                            outerResponse->GetError().GetMessage()),
                        ~innerResponse);
                }
                return innerResponse;
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
