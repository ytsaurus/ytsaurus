#include "memory_tag.h"

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/fiber.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TMemoryTagGuard::TMemoryTagGuard(ui32 tag)
{ 
    if (auto* scheduler = TryGetCurrentScheduler()) {
        if (auto* fiber = scheduler->GetCurrentFiber()) {
            Active_ = true;
            PreviousTag_ = fiber->MemoryTag();
            fiber->MemoryTag() = tag;
            SetCurrentMemoryTag(tag);
        }
    }
}

TMemoryTagGuard::~TMemoryTagGuard()
{
    if (Active_) {
        auto* scheduler = TryGetCurrentScheduler();
        YCHECK(scheduler);
        auto* fiber = scheduler->GetCurrentFiber();
        YCHECK(fiber);
        fiber->MemoryTag() = PreviousTag_;
        SetCurrentMemoryTag(PreviousTag_);
    }
}

TMemoryTagGuard::TMemoryTagGuard(TMemoryTagGuard&& other)
    : Active_(other.Active_)
    , PreviousTag_(other.PreviousTag_)
{
    other.Active_ = false;
    other.PreviousTag_ = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
