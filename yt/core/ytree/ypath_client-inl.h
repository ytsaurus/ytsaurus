#ifndef YPATH_CLIENT_INL_H_
#error "Direct inclusion of this file is not allowed, include ypath_client.h"
#endif
#undef YPATH_CLIENT_INL_H_

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedRequest>
TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> >
ExecuteVerb(IYPathServicePtr service, TIntrusivePtr<TTypedRequest> request)
{
    typedef typename TTypedRequest::TTypedResponse TTypedResponse;

    auto requestMessage = request->Serialize();
    return
        ExecuteVerb(service, requestMessage)
        .Apply(BIND([] (const TSharedRefArray& responseMessage) -> TIntrusivePtr<TTypedResponse> {
            auto response = New<TTypedResponse>();
            response->Deserialize(responseMessage);
            return response;
        }));
}

template <class TTypedRequest>
TIntrusivePtr<typename TTypedRequest::TTypedResponse>
SyncExecuteVerb(IYPathServicePtr service, TIntrusivePtr<TTypedRequest> request)
{
    auto response = ExecuteVerb(service, request).Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(*response);
    return response;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
