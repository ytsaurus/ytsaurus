#pragma once

#include "public.h"

#include <yt/yt/ytlib/incumbent_client/incumbent_descriptor.h>

#include <yt/yt/client/hydra/public.h>

namespace NYT::NIncumbentServer {

////////////////////////////////////////////////////////////////////////////////

struct TPeerDescriptor
{
    TString Address;
    NHydra::EPeerState State;
};

////////////////////////////////////////////////////////////////////////////////

NIncumbentClient::TIncumbentMap ScheduleIncumbents(
    const TIncumbentSchedulerConfigPtr& config,
    const std::vector<TPeerDescriptor>& peers);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
