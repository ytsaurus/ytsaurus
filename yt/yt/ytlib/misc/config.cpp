#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TNodeMemoryReferenceTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_memory_reference_tracker", &TThis::EnableMemoryReferenceTracker)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
