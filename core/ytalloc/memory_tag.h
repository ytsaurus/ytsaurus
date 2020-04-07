#pragma once

#include <library/cpp/ytalloc/api/ytalloc.h>

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
    NYTAlloc::TMemoryTag PreviousTag_ = NYTAlloc::NullMemoryTag;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
