#pragma once

#include "public.h"

#include <yt/yt/library/profiling/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

NProfiling::ISensorProducerPtr CreateDiskSpaceSensorProducer(TCellMasterBootstrapConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
