#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IHunkChunkSweeper
    : public virtual TRefCounted
{
    virtual void Start() = 0;
};

DEFINE_REFCOUNTED_TYPE(IHunkChunkSweeper)

IHunkChunkSweeperPtr CreateHunkChunkSweeper(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
