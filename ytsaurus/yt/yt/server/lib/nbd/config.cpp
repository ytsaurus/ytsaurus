#include "config.h"

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

void TNbdServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("port", &TThis::Port)
        .Default(10809);
    registrar.Parameter("max_backlog_size", &TThis::MaxBacklogSize)
        .Default(1000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
