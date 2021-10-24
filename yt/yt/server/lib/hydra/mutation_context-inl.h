#ifndef MUTATION_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include mutation_context.h"
// For the sake of sane code completion.
#include "mutation_context.h"
#endif

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class... Ts>
void TMutationContext::CombineStateHash(const Ts&... ks)
{
    (HashCombine(StateHash_, ks), ...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
