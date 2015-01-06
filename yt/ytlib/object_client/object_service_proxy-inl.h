#ifndef OBJECT_SERVICE_PROXY_INL_H_
#error "Direct inclusion of this file is not allowed, include object_service_proxy.h"
#endif

#include <core/rpc/service.h>
#include <core/rpc/client.h>

namespace NYT {
namespace NObjectClient {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedResponse>
TErrorOr<TIntrusivePtr<TTypedResponse>> TObjectServiceProxy::TRspExecuteBatch::GetResponse(int index) const
{
    auto innerResponseMessage = GetResponseMessage(index);
    if (!innerResponseMessage) {
        return TIntrusivePtr<TTypedResponse>();
    }
    auto innerResponse = New<TTypedResponse>();
    try {
        innerResponse->Deserialize(innerResponseMessage);
    } catch (const std::exception& ex) {
        return ex;
    }
    return innerResponse;
}

template <class TTypedResponse>
TNullable<TErrorOr<TIntrusivePtr<TTypedResponse>>> TObjectServiceProxy::TRspExecuteBatch::FindResponse(const Stroka& key) const
{
    YCHECK(!key.empty());
    auto range = KeyToIndexes.equal_range(key);
    if (range.first == range.second) {
        return Null;
    }
    auto it = range.first;
    int index = it->second;
    // Failure here means that more than one response with the given key is found.
    YCHECK(++it == range.second);
    return GetResponse<TTypedResponse>(index);
}

template <class TTypedResponse>
TErrorOr<TIntrusivePtr<TTypedResponse>> TObjectServiceProxy::TRspExecuteBatch::GetResponse(const Stroka& key) const
{
    auto result = FindResponse<TTypedResponse>(key);
    YCHECK(result);
    return *result;
}

template <class TTypedResponse>
std::vector<TErrorOr<TIntrusivePtr<TTypedResponse>>> TObjectServiceProxy::TRspExecuteBatch::GetResponses(const Stroka& key) const
{
    std::vector<TErrorOr<TIntrusivePtr<TTypedResponse>>> responses;
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
TFuture<TIntrusivePtr<typename TTypedRequest::TTypedResponse> >
TObjectServiceProxy::Execute(TIntrusivePtr<TTypedRequest> innerRequest)
{
    typedef typename TTypedRequest::TTypedResponse TTypedResponse;

    auto outerRequest = ExecuteBatch();
    outerRequest->AddRequest(innerRequest);
    return outerRequest->Invoke().Apply(BIND([] (TRspExecuteBatchPtr outerResponse) {
        return outerResponse->GetResponse<TTypedResponse>(0).ValueOrThrow();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT
