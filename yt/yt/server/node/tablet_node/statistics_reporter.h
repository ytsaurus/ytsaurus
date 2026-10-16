#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IStatisticsReporter
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Reconfigure(const TTabletNodeDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IStatisticsReporter);

////////////////////////////////////////////////////////////////////////////////

IStatisticsReporterPtr CreateStatisticsReporter(IBootstrap* const bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
