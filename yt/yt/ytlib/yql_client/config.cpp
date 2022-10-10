#include "config.h"

namespace NYT::NYqlClient {

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentChannelConfig::Register(TRegistrar /*registrar*/)
{ }

void TYqlAgentConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("channel", &TThis::Channel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlClient
