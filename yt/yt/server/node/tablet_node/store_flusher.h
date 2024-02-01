#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IStoreFlusher
    : public virtual TRefCounted
{
    virtual void Start() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IStoreFlusher)

IStoreFlusherPtr CreateStoreFlusher(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
