#pragma once

#include "public.h"

#include <yt/yt/ytlib/memory_trackers/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NChunkClient {

/////////////////////////////////////////////////////////////////////////////

TBlock ResetCategory(
    TBlock block,
    IBlockTrackerPtr tracker,
    std::optional<EMemoryCategory> category);

/////////////////////////////////////////////////////////////////////////////

TBlock AttachCategory(
    TBlock block,
    IBlockTrackerPtr tracker,
    std::optional<EMemoryCategory> category);

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
