#ifndef OBJECT_SERVICE_PROXY_INL_H_
#error "Direct inclusion of this file is not allowed, include object_service_proxy.h"
#endif

//#include <ytlib/cypress_ypath_proxy.h"

#include <ytlib/rpc/service.h>
#include <ytlib/rpc/client.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedResponse>
TIntrusivePtr<TTypedResponse> TObjectServiceProxy::TRspExecuteBatch::GetResponse(int index) const
{
    YASSERT(index >= 0 && index < GetSize());
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

template <class TTypedResponse>
TIntrusivePtr<TTypedResponse> TObjectServiceProxy::TRspExecuteBatch::GetResponse(const Stroka& key) const
{
    YASSERT(!key.empty());
    auto range = KeyToIndexes.equal_range(key);
    auto it = range.first;
    // Failure here means that no responses with the given key are found.
    YASSERT(range.first != range.second);
    int index = it->second;
    // Failure here means that more than one response with the given key is found.
    YASSERT(++it == range.second);
    return GetResponse<TTypedResponse>(index);
}

template <class TTypedResponse>
std::vector< TIntrusivePtr<TTypedResponse> > TObjectServiceProxy::TRspExecuteBatch::GetResponses(const Stroka& key)    const
{
    std::vector< TIntrusivePtr<TTypedResponse> > responses;
    if (key.empty()) {
        responses.reserve(GetSize());
        for (int index = 0; index < GetSize(); ++index) {
            responses.push_back(GetResponse<TTypedResponse>(index));
        }
    } else {
        auto range = KeyToIndexes.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) {
            responses.push_back(GetResponse<TTypedResponse>(it->second));
        }
    }
    return responses;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedRequest>
TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> >
TObjectServiceProxy::Execute(TIntrusivePtr<TTypedRequest> innerRequest)
{
    typedef typename TTypedRequest::TTypedResponse TTypedResponse;

    auto innerRequestMessage = innerRequest->Serialize();

    auto outerRequest = Execute();
    outerRequest->add_part_counts(static_cast<int>(innerRequestMessage->GetParts().size()));
    outerRequest->Attachments() = innerRequestMessage->GetParts();

    return outerRequest->Invoke().Apply(BIND(
        [] (TRspExecutePtr outerResponse) -> TIntrusivePtr<TTypedResponse> {
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

} // namespace NObjectServer
} // namespace NYT
