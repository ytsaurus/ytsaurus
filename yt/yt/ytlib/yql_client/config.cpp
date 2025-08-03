#include "config.h"

namespace NYT::NYqlClient {

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_progress_request_timeout", &TThis::DefaultProgressRequestTimeout)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentStageConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("channel", &TThis::Channel);
}

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("stages", &TThis::Stages)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlClient
