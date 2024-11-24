#include "config.h"

#include <yt/yt/ytlib/chunk_client/config.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

void TNativeDriverConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tvm_service", &TThis::TvmService)
        .Default();
    registrar.Parameter("chunk_client_dispatcher", &TThis::ChunkClientDispatcher)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
