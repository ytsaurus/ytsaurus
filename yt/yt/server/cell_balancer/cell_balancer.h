#pragma once

#include "private.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class ICellBalancer
    : public TRefCounted
{
public:
    virtual void Start() = 0;

    virtual NYTree::IYPathServicePtr CreateOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellBalancer)

////////////////////////////////////////////////////////////////////////////////

ICellBalancerPtr CreateCellBalancer(IBootstrap* bootstrap, TCellBalancerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
