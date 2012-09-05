#ifndef OBJECT_SERVICE_PROXY_INL_H_
#error "Direct inclusion of this file is not allowed, include object_service_proxy.h"
#endif

#include <ytlib/rpc/service.h>
#include <ytlib/rpc/client.h>

namespace NYT {
namespace NObjectClient {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedResponse>
TIntrusivePtr<TTypedResponse> TObjectServiceProxy::TRspExecuteBatch::GetResponse(int index) const
{
    YCHECK(index >= 0 && index < GetSize());
    int beginIndex = BeginPartIndexes[index];
    int endIndex = beginIndex + Body.part_counts(index);
    if (beginIndex == endIndex) {
        // This is an empty response.
        return NULL;
    }

    std::vector<TSharedRef> innerParts(
        Attachments_.begin() + beginIndex,
        Attachments_.begin() + endIndex);
    auto innerMessage = NBus::CreateMessageFromParts(MoveRV(innerParts));
    auto innerResponse = New<TTypedResponse>();
    innerResponse->Deserialize(innerMessage);
    return innerResponse;
}

template <class TTypedResponse>
TIntrusivePtr<TTypedResponse> TObjectServiceProxy::TRspExecuteBatch::FindResponse(const Stroka& key) const
{
    YCHECK(!key.empty());
    auto range = KeyToIndexes.equal_range(key);
    if (range.first == range.second) {
        return NULL;
    }
    auto it = range.first;
    int index = it->second;
    // Failure here means that more than one response with the given key is found.
    YCHECK(++it == range.second);
    return GetResponse<TTypedResponse>(index);
}

template <class TTypedResponse>
TIntrusivePtr<TTypedResponse> TObjectServiceProxy::TRspExecuteBatch::GetResponse(const Stroka& key) const
{
    auto result = FindResponse<TTypedResponse>(key);
    YCHECK(result);
    return result;
}

template <class TTypedResponse>
std::vector< TIntrusivePtr<TTypedResponse> > TObjectServiceProxy::TRspExecuteBatch::GetResponses(const Stroka& key) const
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

    auto outerRequest = ExecuteBatch();
    outerRequest->AddRequest(innerRequest);
    return outerRequest->Invoke().Apply(BIND([] (TRspExecuteBatchPtr outerResponse) -> TIntrusivePtr<TTypedResponse> {
        if (outerResponse->IsOK()) {
            return outerResponse->GetResponse<TTypedResponse>(0);
        } else {
            auto innerResponse = New<TTypedResponse>();
            innerResponse->SetError(outerResponse->GetError());
            return innerResponse;
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT
