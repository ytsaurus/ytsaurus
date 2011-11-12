#pragma once

#include "common.h"
#include "cypress_service_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"
#include "../ytree/ypath_client.h"
#include "../ytree/ypath_service.h"
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
        ((VerbError)(3))
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
        NYTree::TYPath path,
        TIntrusivePtr<TTypedRequest> innerRequest)
    {
        auto outerRequest = Execute();
        outerRequest->SetTransactionId(transactionId.ToProto());
        outerRequest->SetPath(path);
        outerRequest->SetVerb(innerRequest->GetVerb());
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
                auto errorCode = outerResponse->GetErrorCode();
                if (errorCode == NRpc::EErrorCode::OK ||
                    errorCode == TCypressServiceProxy::EErrorCode::VerbError)
                {
                    UnwrapYPathResponse(~outerResponse, ~innerResponse);
                } else {
                    TError error(
                        NYTree::EYPathErrorCode::GenericError,
                        outerResponse->GetError().GetMessage());
                    SetYPathErrorResponse(error, ~innerResponse);    
                }
                return innerResponse;
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
