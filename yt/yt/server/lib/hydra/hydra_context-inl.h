#ifndef HYDRA_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include hydra_context.h"
// For the sake of sane code completion.
#include "hydra_context.h"
#endif

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class EReign>
EReign THydraContext::GetAutomatonReign() const
{
    return CheckedEnumCast<EReign>(AutomatonReign_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
