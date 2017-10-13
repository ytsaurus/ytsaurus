#pragma once

#include "public.h"

namespace NYT {

struct TBlockHeader
{
    static const ui64 CheckpointSentinel = 0;
    static const ui64 CheckpointsDisabled = 0xffffffffU;

    ui64 Length;
};

} // namespace NYT

Y_DECLARE_PODTYPE(NYT::TBlockHeader);
