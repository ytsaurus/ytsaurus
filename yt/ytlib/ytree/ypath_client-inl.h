#ifndef YPATH_CLIENT_INL_H_
#error "Direct inclusion of this file is not allowed, include ypath_client.h"
#endif
#undef YPATH_CLIENT_INL_H_

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedRequest>
TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> >
ExecuteVerb(IYPathServicePtr service, TTypedRequest* request)
{
    typedef typename TTypedRequest::TTypedResponse TTypedResponse;

    auto requestMessage = request->Serialize();
    return
        ExecuteVerb(service, ~requestMessage)
        .Apply(BIND([] (NBus::IMessage::TPtr responseMessage) {
            auto response = New<TTypedResponse>();
            response->Deserialize(~responseMessage);
            return response;
        }));
}


template <class TTypedRequest>
TIntrusivePtr<typename TTypedRequest::TTypedResponse>
SyncExecuteVerb(IYPathServicePtr service, TTypedRequest* request)
{
    auto response = ExecuteVerb(service, request).Get();
    response->ThrowIfError();
    return response;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
