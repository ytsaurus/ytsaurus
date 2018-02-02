#pragma once

#include <yt/core/misc/public.h>

namespace NYT {
namespace NCellScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EControlQueue,
    (Default)
    (UserRequest)
    (MasterConnector)
    (Orchid)
    (PeriodicActivity)
    (Operation)
    (AgentTracker)
);

class TBootstrap;

DECLARE_REFCOUNTED_CLASS(TCellSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
