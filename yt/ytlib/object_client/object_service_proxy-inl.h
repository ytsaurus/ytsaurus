#pragma once
#ifndef OBJECT_SERVICE_PROXY_INL_H_
#error "Direct inclusion of this file is not allowed, include object_service_proxy.h"
// For the sake of sane code completion.
#include "object_service_proxy.h"
#endif

#include <yt/core/rpc/service.h>
#include <yt/core/rpc/client.h>

namespace NYT::NObjectClient {

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
        innerResponse->Tag() = InnerRequestDescriptors_[index].Tag;
    } catch (const std::exception& ex) {
        return ex;
    }
    return innerResponse;
}

template <class TTypedResponse>
std::optional<TErrorOr<TIntrusivePtr<TTypedResponse>>> TObjectServiceProxy::TRspExecuteBatch::FindResponse(const TString& key) const
{
    std::optional<TErrorOr<TIntrusivePtr<TTypedResponse>>> result;
    for (int index = 0; index < static_cast<int>(InnerRequestDescriptors_.size()); ++index) {
        if (key == InnerRequestDescriptors_[index].Key) {
            if (result) {
                THROW_ERROR_EXCEPTION("Found multiple responses with key %Qv", key);
            }
            result = GetResponse<TTypedResponse>(index);
        }
    }
    return result;
}

template <class TTypedResponse>
TErrorOr<TIntrusivePtr<TTypedResponse>> TObjectServiceProxy::TRspExecuteBatch::GetResponse(const TString& key) const
{
    auto result = FindResponse<TTypedResponse>(key);
    YT_VERIFY(result);
    return std::move(*result);
}

template <class TTypedResponse>
std::vector<TErrorOr<TIntrusivePtr<TTypedResponse>>> TObjectServiceProxy::TRspExecuteBatch::GetResponses(const std::optional<TString>& key) const
{
    std::vector<TErrorOr<TIntrusivePtr<TTypedResponse>>> responses;
    responses.reserve(InnerRequestDescriptors_.size());
    for (int index = 0; index < static_cast<int>(InnerRequestDescriptors_.size()); ++index) {
        if (!key || *key == InnerRequestDescriptors_[index].Key) {
            responses.push_back(GetResponse<TTypedResponse>(index));
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

} // namespace NYT::NObjectClient
