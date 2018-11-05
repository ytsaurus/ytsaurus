#pragma once
#ifndef OBJECT_SERVICE_PROXY_INL_H_
#error "Direct inclusion of this file is not allowed, include object_service_proxy.h"
// For the sake of sane code completion.
#include "object_service_proxy.h"
#endif

#include <yt/core/rpc/service.h>
#include <yt/core/rpc/client.h>

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
TNullable<TErrorOr<TIntrusivePtr<TTypedResponse>>> TObjectServiceProxy::TRspExecuteBatch::FindResponse(const TString& key) const
{
    YCHECK(!key.empty());
    auto range = KeyToIndexes_.equal_range(key);
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
TErrorOr<TIntrusivePtr<TTypedResponse>> TObjectServiceProxy::TRspExecuteBatch::GetResponse(const TString& key) const
{
    auto result = FindResponse<TTypedResponse>(key);
    YCHECK(result);
    return *result;
}

template <class TTypedResponse>
std::vector<TErrorOr<TIntrusivePtr<TTypedResponse>>> TObjectServiceProxy::TRspExecuteBatch::GetResponses(const TString& key) const
{
    std::vector<TErrorOr<TIntrusivePtr<TTypedResponse>>> responses;
    if (key.empty()) {
        responses.reserve(GetSize());
        for (int index = 0; index < GetSize(); ++index) {
            responses.push_back(GetResponse<TTypedResponse>(index));
        }
    } else {
        auto range = KeyToIndexes_.equal_range(key);
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
    return outerRequest->Invoke().Apply(BIND([] (const TRspExecuteBatchPtr& outerResponse) {
        return outerResponse->GetResponse<TTypedResponse>(0)
            .ValueOrThrow();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT
