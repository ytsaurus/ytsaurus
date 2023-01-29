#include "config.h"

namespace NYT::NQueryTrackerClient {

///////////////////////////////////////////////////////////////////////////////

void TQueryTrackerStageConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("root", &TThis::Root)
        .Default("//sys/query_trackers");
}

///////////////////////////////////////////////////////////////////////////////

void TQueryTrackerConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("stages", &TThis::Stages)
        .Default();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTrackerClient
