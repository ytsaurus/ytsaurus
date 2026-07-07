#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IOverloadReporter
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Reconfigure(const TTabletNodeDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOverloadReporter)

////////////////////////////////////////////////////////////////////////////////

IOverloadReporterPtr CreateOverloadReporter(IBootstrap* const bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
