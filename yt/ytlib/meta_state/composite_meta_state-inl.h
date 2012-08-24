#ifndef COMPOSITE_META_STATE_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_meta_state.h"
#endif

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class TResponse>
struct TMetaStatePart::TThunkTraits
{
    static void Thunk(
        TCallback<TResponse(const TRequest& request)> handler,
        TMutationContext* context)
    {
        TRequest request;
        YCHECK(DeserializeFromProtoWithEnvelope(&request, context->GetRequestData()));

        auto response = handler.Run(request);

        TSharedRef responseData;
        YCHECK(SerializeToProtoWithEnvelope(response, &responseData));

        context->SetResponseData(responseData);
    }
};

template <class TRequest>
struct TMetaStatePart::TThunkTraits<TRequest, void>
{
    static void Thunk(
        TCallback<void(const TRequest& request)> handler,
        TMutationContext* context)
    {
        TRequest request;
        YCHECK(DeserializeFromProtoWithEnvelope(&request, context->GetRequestData()));

        handler.Run(request);
    }
};

template <class TRequest, class TResponse>
void TMetaStatePart::RegisterMethod(
    TCallback<TResponse(const TRequest&)> handler)
{
    Stroka mutationType = TRequest().GetTypeName();
    auto wrappedHandler = BIND(
        &TThunkTraits<TRequest, TResponse>::Thunk,
        MoveRV(handler));
    YCHECK(MetaState->Methods.insert(MakePair(mutationType, wrappedHandler)).second);
}

template <class TRequest, class TResponse>
bool TMetaStatePart::HasMethod(
    TCallback<TResponse(const TRequest&)> handler)
{
    Stroka mutationType = TRequest().GetTypeName();
    return MetaState->Methods.count(mutationType) > 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
