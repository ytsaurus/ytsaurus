#ifndef COMPOSITE_AUTOMATON_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_automaton.h"
#endif

#include "mutation.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest, class TResponse>
void TCompositeAutomatonPart::RegisterMethod(
    TCallback<TResponse(const TRequest&)> handler)
{
    auto wrappedHandler = BIND(
        &TMutationActionTraits<TRequest, TResponse>::Run,
        std::move(handler));
    YCHECK(Automaton->Methods.insert(std::make_pair(
        TRequest::default_instance().GetTypeName(),
        std::move(wrappedHandler))).second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
