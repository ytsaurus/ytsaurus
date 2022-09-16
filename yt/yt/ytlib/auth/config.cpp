#include "config.h"
#include "public.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

void TNativeAuthenticationManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tvm_service", &TThis::TvmService)
        .Default();
    registrar.Parameter("enable_validation", &TThis::EnableValidation)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TNativeAuthenticationManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_validation", &TThis::EnableValidation)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
