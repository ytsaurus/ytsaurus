#pragma once

#include <yt/yt/flow/library/cpp/companion/public.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

using NCompanion::ECompanionComputationType;
using NCompanion::ECompanionResponseStatus;

class TPipeline;

DECLARE_REFCOUNTED_CLASS(TCompanionServer);
DECLARE_REFCOUNTED_CLASS(TJob);
DECLARE_REFCOUNTED_CLASS(TJobRegistry);
DECLARE_REFCOUNTED_CLASS(TResourceStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
