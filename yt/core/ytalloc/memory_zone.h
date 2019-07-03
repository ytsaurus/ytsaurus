#pragma once

#include <library/ytalloc/api/ytalloc.h>

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

class TMemoryZoneGuard
{
public:
    TMemoryZoneGuard() = default;
    explicit TMemoryZoneGuard(NYTAlloc::EMemoryZone zone);
    ~TMemoryZoneGuard();

    TMemoryZoneGuard(const TMemoryZoneGuard& other) = delete;
    TMemoryZoneGuard(TMemoryZoneGuard&& other);

private:
    bool Active_ = false;
    NYTAlloc::EMemoryZone PreviousZone_ = NYTAlloc::EMemoryZone::Normal;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
