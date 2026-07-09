#include "config.h"

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

void TWorkerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bus", &TThis::Bus)
        .DefaultNew();
    registrar.Parameter("message_service_threads", &TThis::MessageServiceThreads)
        .Default(3);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
