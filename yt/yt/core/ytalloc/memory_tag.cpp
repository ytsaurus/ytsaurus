#include "memory_tag.h"

namespace NYT {

using namespace NYTAlloc;

////////////////////////////////////////////////////////////////////////////////

TMemoryTagGuard::TMemoryTagGuard(TMemoryTag tag)
{
    PreviousTag_ = GetCurrentMemoryTag();
    SetCurrentMemoryTag(tag);
}

TMemoryTagGuard::~TMemoryTagGuard()
{
    SetCurrentMemoryTag(PreviousTag_);
}

TMemoryTagGuard::TMemoryTagGuard(TMemoryTagGuard&& other)
    : PreviousTag_(other.PreviousTag_)
{
    other.PreviousTag_ = NullMemoryTag;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
