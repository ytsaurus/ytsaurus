#pragma once

#include <yt/core/misc/common.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using TMemoryTag = ui32;
constexpr TMemoryTag NullMemoryTag = 0;
constexpr TMemoryTag MaxMemoryTag = (1ULL << 22) - 1;

////////////////////////////////////////////////////////////////////////////////

class TMemoryTagGuard
{
public:
    TMemoryTagGuard() = default;
    explicit TMemoryTagGuard(TMemoryTag tag);
    ~TMemoryTagGuard();

    TMemoryTagGuard(const TMemoryTagGuard& other) = delete;
    TMemoryTagGuard(TMemoryTagGuard&& other);

private:
    bool Active_ = false;
    TMemoryTag PreviousTag_ = NullMemoryTag;
};

////////////////////////////////////////////////////////////////////////////////

// Implementations in memory_tag.cpp are merely stubs, the intended
// implementations may be found within the allocator.
void SetCurrentMemoryTag(TMemoryTag tag);
size_t GetMemoryUsageForTag(TMemoryTag tag);
void GetMemoryUsageForTags(TMemoryTag* tags, size_t count, size_t* result);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
