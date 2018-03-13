#pragma once

#include <yt/core/misc/common.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using TMemoryTag = ui64;
constexpr TMemoryTag NullMemoryTag = 0;
constexpr TMemoryTag MaxMemoryTag = 1 << 16;

////////////////////////////////////////////////////////////////////////////////

class TMemoryTagGuard
{
public:
    TMemoryTagGuard() = default;
    TMemoryTagGuard(TMemoryTag tag);
    ~TMemoryTagGuard();

    TMemoryTagGuard(const TMemoryTagGuard& other) = delete;
    TMemoryTagGuard(TMemoryTagGuard&& other);
private:
    bool Active_ = false;
    TMemoryTag PreviousTag_ = NullMemoryTag;
};

////////////////////////////////////////////////////////////////////////////////

// Implementations in memory_tag.cpp are merely stubs, the intended
// implementations may be found in lf_allocX64.cpp.
Y_WEAK void SetCurrentMemoryTag(TMemoryTag tag);
Y_WEAK ssize_t GetMemoryUsageForTag(TMemoryTag tag);
Y_WEAK void GetMemoryUsageForTagList(TMemoryTag* tagList, int count, ssize_t* result);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
