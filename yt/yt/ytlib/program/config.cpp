#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TNativeSingletonsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("chunk_client_dispatcher", &TThis::ChunkClientDispatcher)
        .DefaultNew();
};

////////////////////////////////////////////////////////////////////////////////

void TNativeSingletonsDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("chunk_client_dispatcher", &TThis::ChunkClientDispatcher)
        .DefaultNew();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
