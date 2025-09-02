#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

class THunkChunk
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NChunkClient::TChunkId, Id);
    DEFINE_BYVAL_RW_PROPERTY(i64, TotalHunkLength);
    DEFINE_BYVAL_RW_PROPERTY(i64, ReferencedTotalHunkLength);

public:
    THunkChunk(NChunkClient::TChunkId id, i64 totalHunkLength, i64 referencedTotalHunkLength);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
