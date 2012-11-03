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

template <class TContext>
void TMetaStatePart::RegisterSaver(
    int priority,
    const Stroka& name,
    i32 version,
    TCallback<void(const TContext&)> saver,
    const TContext& context)
{
    RegisterSaver(
        priority,
        name,
        version,
        BIND([=] (const TSaveContext& basicContext) {
            TContext combinedContext(context);
            static_cast<TSaveContext&>(combinedContext) = basicContext;
            saver.Run(combinedContext);
        }));
}

template <class TContext>
void TMetaStatePart::RegisterLoader(
    const Stroka& name,
    TVersionValidator versionValidator,
    TCallback<void(const TContext&)> loader,
    const TContext& context)
{
    RegisterLoader(
        name,
        versionValidator,
        BIND([=] (const TLoadContext& basicContext) {
            TContext combinedContext(context);
            static_cast<TLoadContext&>(combinedContext) = basicContext;
            loader.Run(combinedContext);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
