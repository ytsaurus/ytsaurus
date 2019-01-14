#pragma once

#include <yt/core/misc/common.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Memory zone is used to pass hint to the allocator:
// #Normal - default memory type,
// #Undumpable - memory is omitted from the core dump.
DEFINE_ENUM(EMemoryZone,
    (Normal)
    (Undumpable)
);

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
