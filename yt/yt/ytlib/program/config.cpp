#include "config.h"

#include <yt/yt/library/containers/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TNativeSingletonsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("chunk_client_dispatcher", &TThis::ChunkClientDispatcher)
        .DefaultNew();
    registrar.Parameter("native_authentication_manager", &TThis::NativeAuthenticationManager)
        .DefaultNew();
    registrar.Parameter("enable_porto_resource_tracker", &TThis::EnablePortoResourceTracker)
        .Default(false);
    registrar.Parameter("pod_spec", &TThis::PodSpec)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TNativeSingletonsDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("chunk_client_dispatcher", &TThis::ChunkClientDispatcher)
        .DefaultNew();
    registrar.Parameter("native_authentication_manager", &TThis::NativeAuthenticationManager)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
