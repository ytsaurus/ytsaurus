#pragma once

#include "common.h"

#include <library/lfalloc/helpers.h>

namespace NYT {
namespace NLFAlloc {

////////////////////////////////////////////////////////////////////////////////

i64 GetCurrentUsed();
i64 GetCurrentMmapped();
i64 GetCurrentMmappedCount();
i64 GetCurrentLargeBlocks();
i64 GetCurrentSmallBlocks();
i64 GetCurrentSystem();

////////////////////////////////////////////////////////////////////////////////

} // NLFAlloc
} // namespace NYT
