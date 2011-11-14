#pragma once

#include "common.h"
#include "orchid_service_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

class TOrchidServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TOrchidServiceProxy> TPtr;

    RPC_DECLARE_PROXY(OrchidService,
        ((NavigationError)(1))
    );

    TOrchidServiceProxy(NRpc::IChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NProto, Execute);


    template <class TTypedRequest>
    TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
    Execute(TTypedRequest* innerRequest)
    {
        auto outerRequest = Execute();
        return DoExecute<TTypedRequest, typename TTypedRequest::TTypedResponse>(
            ~outerRequest,
            innerRequest);
    }

private:
    // TODO: copypaste
    template <class TTypedRequest, class TTypedResponse>
    TIntrusivePtr< TFuture< TIntrusivePtr<TTypedResponse> > >
    DoExecute(TReqExecute* outerRequest, TTypedRequest* innerRequest)
    {
        WrapYPathRequest(outerRequest, innerRequest);
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

} // namespace NOrchid
} // namespace NYT
