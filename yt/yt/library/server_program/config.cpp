#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TServerProgramConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_porto_resource_tracker", &TThis::EnablePortoResourceTracker)
        .Default(false);
    registrar.Parameter("pod_spec", &TThis::PodSpec)
        .DefaultNew();

    registrar.Parameter("hotswap_manager", &TThis::HotswapManager)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
