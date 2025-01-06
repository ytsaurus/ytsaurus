#include "config.h"

namespace NYT::NMultidaemon {

////////////////////////////////////////////////////////////////////////////////

void TDaemonConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type);
    registrar.Parameter("config", &TThis::Config);
}

////////////////////////////////////////////////////////////////////////////////

void TMultidaemonProgramConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("daemons", &TThis::Daemons);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMultidaemon
