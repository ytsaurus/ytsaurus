#ifndef COMPOSITE_AUTOMATON_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_automaton.h"
#endif

#include <core/misc/serialize.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class TResponse>
struct TCompositeAutomatonPart::TThunkTraits
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
struct TCompositeAutomatonPart::TThunkTraits<TRequest, void>
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
void TCompositeAutomatonPart::RegisterMethod(
    TCallback<TResponse(const TRequest&)> handler)
{
    auto mutationType = TRequest().GetTypeName();
    auto wrappedHandler = BIND(
        &TThunkTraits<TRequest, TResponse>::Thunk,
        std::move(handler));
    YCHECK(Automaton->Methods.insert(std::make_pair(mutationType, wrappedHandler)).second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
