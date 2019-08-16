#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TCheckpointableStreamBlockHeader
{
    static constexpr ui64 CheckpointSentinel = 0;

    ui64 Length;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

Y_DECLARE_PODTYPE(NYT::TCheckpointableStreamBlockHeader);
