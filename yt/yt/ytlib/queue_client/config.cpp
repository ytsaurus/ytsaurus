#include "config.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NQueueClient {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentRegistrationTableConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("root", &TThis::Root);
    registrar.Parameter("cache_refresh_period", &TThis::CacheRefreshPeriod)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("user", &TThis::User)
        .Default(RootUserName);

    registrar.Postprocessor([] (TThis* config) {
        config->Root = config->Root.Normalize();
    });
}

////////////////////////////////////////////////////////////////////////////////

void TQueueAgentConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("stages", &TThis::Stages)
        .Default();
    registrar.Parameter("registration_table", &TThis::RegistrationTable)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
