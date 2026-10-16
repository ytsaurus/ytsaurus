#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
// For the sake of sane code completion.
#include "serialize.h"
#endif

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsMasterReign(TReign reign)
{
    return reign >= MinMasterReign && reign <= MaxMasterReign;
}

constexpr bool IsTabletReign(TReign reign)
{
    return reign >= MinTabletReign && reign <= MaxTabletReign;
}

constexpr bool IsChaosReign(TReign reign)
{
    return reign >= MinChaosReign && reign <= MaxChaosReign;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
