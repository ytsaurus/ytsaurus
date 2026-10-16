#include "companion_entrypoint.h"

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

void TCompanionEntrypoint::Register(TRegistrar registrar)
{
    registrar.Parameter("executable", &TThis::Executable)
        .Default("");
    registrar.Parameter("args", &TThis::Args)
        .Default();
    registrar.Parameter("env", &TThis::Env)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
