#include "memory_zone.h"

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/fiber.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TMemoryZoneGuard::TMemoryZoneGuard(EMemoryZone zone)
{ 
    if (auto* scheduler = TryGetCurrentScheduler()) {
        if (auto* fiber = scheduler->GetCurrentFiber()) {
            Active_ = true;
            PreviousZone_ = fiber->GetMemoryZone();
            fiber->SetMemoryZone(zone);
            SetCurrentMemoryZone(zone);
        }
    }
}

TMemoryZoneGuard::~TMemoryZoneGuard()
{
    if (Active_) {
        auto* scheduler = GetCurrentScheduler();
        auto* fiber = scheduler->GetCurrentFiber();
        YCHECK(fiber);
        fiber->SetMemoryZone(PreviousZone_);
        SetCurrentMemoryZone(PreviousZone_);
    }
}

TMemoryZoneGuard::TMemoryZoneGuard(TMemoryZoneGuard&& other)
    : Active_(other.Active_)
    , PreviousZone_(other.PreviousZone_)
{
    other.Active_ = false;
    other.PreviousZone_ = EMemoryZone::Normal;
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void SetCurrentMemoryZone(EMemoryZone /*zone*/)
{ }

Y_WEAK EMemoryZone GetCurrentMemoryZone()
{
    return EMemoryZone::Normal;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
