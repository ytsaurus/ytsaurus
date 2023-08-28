#ifndef OBJECT_SERVICE_PROXY_INL_H_
#error "Direct inclusion of this file is not allowed, include object_service_proxy.h"
// For the sake of sane code completion.
#include "object_service_proxy.h"
#endif

#include <yt/yt/core/rpc/service.h>
#include <yt/yt/core/rpc/client.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedResponse>
TErrorOr<TIntrusivePtr<TTypedResponse>> TObjectServiceProxy::TRspExecuteBatch::GetResponse(int index) const
{
    auto innerResponseMessage = GetResponseMessage(index);
    if (!innerResponseMessage) {
        return TIntrusivePtr<TTypedResponse>();
    }

    try {
        auto innerResponse = New<TTypedResponse>();
        innerResponse->Deserialize(innerResponseMessage);
        innerResponse->Tag() = InnerRequestDescriptors_[index].Tag;
        return innerResponse;
    } catch (const std::exception& ex) {
        return ex;
    }
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

template <class TTypedResponse>
std::vector<std::pair<std::any, TErrorOr<TIntrusivePtr<TTypedResponse>>>> TObjectServiceProxy::TRspExecuteBatch::GetTaggedResponses(const std::optional<TString>& key) const
{
    std::vector<std::pair<std::any, TErrorOr<TIntrusivePtr<TTypedResponse>>>> taggedResponses;
    taggedResponses.reserve(InnerRequestDescriptors_.size());
    for (int index = 0; index < static_cast<int>(InnerRequestDescriptors_.size()); ++index) {
        if (!key || *key == InnerRequestDescriptors_[index].Key) {
            taggedResponses.emplace_back(InnerRequestDescriptors_[index].Tag, GetResponse<TTypedResponse>(index));
        }
    }
    return taggedResponses;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedRequest>
TFuture<TIntrusivePtr<typename TTypedRequest::TTypedResponse> >
TObjectServiceProxy::Execute(TIntrusivePtr<TTypedRequest> innerRequest)
{
    using TTypedResponse = typename TTypedRequest::TTypedResponse;

    auto outerRequest = ExecuteBatch();
    outerRequest->AddRequest(innerRequest);
    return outerRequest->Invoke().Apply(BIND([] (const TRspExecuteBatchPtr& outerResponse) {
        return outerResponse->GetResponse<TTypedResponse>(0)
            .ValueOrThrow();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
