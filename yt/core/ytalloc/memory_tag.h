#pragma once

#include <library/ytalloc/api/ytalloc.h>

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

class TMemoryTagGuard
{
public:
    TMemoryTagGuard() = default;
    explicit TMemoryTagGuard(NYTAlloc::TMemoryTag tag);
    ~TMemoryTagGuard();

    TMemoryTagGuard(const TMemoryTagGuard& other) = delete;
    TMemoryTagGuard(TMemoryTagGuard&& other);

private:
    bool Active_ = false;
    NYTAlloc::TMemoryTag PreviousTag_ = NYTAlloc::NullMemoryTag;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
