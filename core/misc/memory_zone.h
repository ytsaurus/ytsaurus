#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMemoryZoneGuard
{
public:
    TMemoryZoneGuard() = default;
    explicit TMemoryZoneGuard(EMemoryZone zone);
    ~TMemoryZoneGuard();

    TMemoryZoneGuard(const TMemoryZoneGuard& other) = delete;
    TMemoryZoneGuard(TMemoryZoneGuard&& other);

private:
    bool Active_ = false;
    EMemoryZone PreviousZone_ = EMemoryZone::Normal;
};

////////////////////////////////////////////////////////////////////////////////

// Implementations in memory_zone.cpp are merely stubs, the intended
// implementations may be found within the allocator.
void SetCurrentMemoryZone(EMemoryZone zone);
EMemoryZone GetCurrentMemoryZone();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
