#include "memory_zone.h"

namespace NYT::NYTAlloc {

using namespace NYTAlloc;

////////////////////////////////////////////////////////////////////////////////

TMemoryZoneGuard::TMemoryZoneGuard(EMemoryZone zone)
{
    PreviousZone_ = GetCurrentMemoryZone();
    SetCurrentMemoryZone(zone);
}

TMemoryZoneGuard::~TMemoryZoneGuard()
{
    SetCurrentMemoryZone(PreviousZone_);
}

TMemoryZoneGuard::TMemoryZoneGuard(TMemoryZoneGuard&& other)
    : PreviousZone_(other.PreviousZone_)
{
    other.PreviousZone_ = EMemoryZone::Normal;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
