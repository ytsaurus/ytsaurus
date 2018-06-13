#include "memory_tag.h"

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/fiber.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TMemoryTagGuard::TMemoryTagGuard(TMemoryTag tag)
{ 
    if (auto* scheduler = TryGetCurrentScheduler()) {
        if (auto* fiber = scheduler->GetCurrentFiber()) {
            Active_ = true;
            PreviousTag_ = fiber->GetMemoryTag();
            fiber->SetMemoryTag(tag);
            SetCurrentMemoryTag(tag);
        }
    }
}

TMemoryTagGuard::~TMemoryTagGuard()
{
    if (Active_) {
        auto* scheduler = GetCurrentScheduler();
        auto* fiber = scheduler->GetCurrentFiber();
        YCHECK(fiber);
        fiber->SetMemoryTag(PreviousTag_);
        SetCurrentMemoryTag(PreviousTag_);
    }
}

TMemoryTagGuard::TMemoryTagGuard(TMemoryTagGuard&& other)
    : Active_(other.Active_)
    , PreviousTag_(other.PreviousTag_)
{
    other.Active_ = false;
    other.PreviousTag_ = NullMemoryTag;
}

////////////////////////////////////////////////////////////////////////////////

void SetCurrentMemoryTag(TMemoryTag /* tag */)
{ }

ssize_t GetMemoryUsageForTag(TMemoryTag /* tag */)
{
    return 0;
}

void GetMemoryUsageForTagList(TMemoryTag* tagList, int count, ssize_t* result)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
