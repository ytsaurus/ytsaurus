#include "config.h"

namespace NYT::NQueryTrackerClient {

///////////////////////////////////////////////////////////////////////////////

void TQueryTrackerStageConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("root", &TThis::Root)
        .Default("//sys/query_tracker");
    registrar.Parameter("user", &TThis::User)
        .Default("query_tracker");
}

///////////////////////////////////////////////////////////////////////////////

void TQueryTrackerConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("stages", &TThis::Stages)
        .Default();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTrackerClient
