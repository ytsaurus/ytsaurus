#ifndef YPATH_CLIENT_INL_H_
#error "Direct inclusion of this file is not allowed, include ypath_client.h"
#endif
#undef YPATH_CLIENT_INL_H_

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedRequest>
TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
ExecuteVerb(
    IYPathService* rootService,
    TTypedRequest* request,
    IYPathExecutor* executor)
{
    typedef typename TTypedRequest::TTypedResponse TTypedResponse;

    auto requestMessage = request->Serialize();
    return
        ExecuteVerb(rootService, ~requestMessage, executor)
        ->Apply(FromFunctor([] (NBus::IMessage::TPtr responseMessage) -> TIntrusivePtr<TTypedResponse>
            {
                auto response = New<TTypedResponse>();
                response->Deserialize(~responseMessage);
                return response;
            }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
