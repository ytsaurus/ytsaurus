#pragma once

#include <yp/server/misc/public.h>

namespace NYP {
namespace NServer {
namespace NNodes {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNodeTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TNodeTracker)

using TEpochId = TGuid;

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodes
} // namespace NServer
} // namespace NYP
