#ifndef CYPRESS_SERVICE_PROXY_INL_H_
#error "Direct inclusion of this file is not allowed, include cypress_service_proxy.h"
#endif

#include "cypress_ypath_proxy.h"

#include <ytlib/rpc/service.h>
#include <ytlib/rpc/client.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedResponse>
TIntrusivePtr<TTypedResponse> TCypressServiceProxy::TRspExecuteBatch::GetResponse(int index) const
{
    YASSERT(index >= 0 && index < Size());
    auto innerResponse = New<TTypedResponse>();
    int beginIndex = BeginPartIndexes[index];
    int endIndex = beginIndex + Body.part_counts(index);
    yvector<TSharedRef> innerParts(
        Attachments_.begin() + beginIndex,
        Attachments_.begin() + endIndex);
    auto innerMessage = NBus::CreateMessageFromParts(MoveRV(innerParts));
    innerResponse->Deserialize(~innerMessage);
    return innerResponse;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedRequest>
TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
TCypressServiceProxy::Execute(TTypedRequest* innerRequest)
{
    typedef typename TTypedRequest::TTypedResponse TTypedResponse;

    auto innerRequestMessage = innerRequest->Serialize();

    auto outerRequest = Execute();
    outerRequest->add_part_counts(innerRequestMessage->GetParts().ysize());
    outerRequest->Attachments() = innerRequestMessage->GetParts();

    return outerRequest->Invoke()->Apply(FromFunctor(
        [] (TRspExecute::TPtr outerResponse) -> TIntrusivePtr<TTypedResponse>
        {
            auto innerResponse = New<TTypedResponse>();
            auto error = outerResponse->GetError();
            if (error.IsOK()) {
                auto innerResponseMessage = NBus::CreateMessageFromParts(outerResponse->Attachments());
                innerResponse->Deserialize(~innerResponseMessage);
            } else if (NRpc::IsRpcError(error)) {
                innerResponse->SetError(error);
            } else {
                // TODO(babenko): should we be erasing the error code here?
                innerResponse->SetError(TError(outerResponse->GetError().GetMessage()));
            }
            return innerResponse;
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
