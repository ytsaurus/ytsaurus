#pragma once

// This header is the first intentionally.
#include <yp/server/lib/misc/public.h>

namespace NYP::NServer::NNodes {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNodeTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TNodeTracker)

using TEpochId = TGuid;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNodes
