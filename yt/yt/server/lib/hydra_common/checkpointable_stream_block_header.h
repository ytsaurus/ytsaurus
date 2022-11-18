#pragma once

#include "public.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TCheckpointableStreamBlockHeader
{
    static constexpr i64 CheckpointSentinel = 0;

    i64 Length;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

Y_DECLARE_PODTYPE(NYT::NHydra::TCheckpointableStreamBlockHeader);
