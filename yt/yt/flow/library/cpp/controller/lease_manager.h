#pragma once

#include "public.h"

namespace NYT::NFlow::NController {

struct ILeaseManager
    : public NYT::TRefCounted
{
    virtual void CheckLeases(const TFlowViewPtr& flowView) = 0;
    virtual void PrepareLeases(const TFlowViewPtr& flowView) = 0;
    virtual void TerminateStrayLeases(const TFlowViewPtr& flowView) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILeaseManager);

ILeaseManagerPtr CreateLeaseManager(
    IYTConnectorPtr connector,
    TLeaseManagerConfigPtr config);

} // namespace NYT::NFlow::NController
