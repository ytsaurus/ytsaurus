#include "config.h"

namespace NYT::NQueryTrackerClient {

////////////////////////////////////////////////////////////////////////////////

void TQueryTrackerChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("timeout", &TThis::Timeout)
        .Default(TDuration::Minutes(1));
}

///////////////////////////////////////////////////////////////////////////////

void TQueryTrackerStageConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("root", &TThis::Root)
        .Default("//sys/query_tracker");
    registrar.Parameter("user", &TThis::User)
        .Default("query_tracker");
    registrar.Parameter("channel", &TThis::Channel)
        .Default();
}

///////////////////////////////////////////////////////////////////////////////

void TQueryTrackerConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("stages", &TThis::Stages)
        .Default();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTrackerClient
