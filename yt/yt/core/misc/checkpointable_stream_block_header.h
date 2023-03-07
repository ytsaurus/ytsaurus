#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TCheckpointableStreamBlockHeader
{
    static constexpr i64 CheckpointSentinel = 0;

    i64 Length;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

Y_DECLARE_PODTYPE(NYT::TCheckpointableStreamBlockHeader);
