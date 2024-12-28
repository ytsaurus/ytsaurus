#include "config.h"

#include <yt/yt/client/transaction_client/config.h>

namespace NYT::NTimestampProvider {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void TTimestampProviderBootstrapConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("bus_client", &TThis::BusClient)
        .DefaultNew();

    registrar.Parameter("clock_cluster_tag", &TThis::ClockClusterTag)
        .Default(InvalidCellTag);

    registrar.Parameter("timestamp_provider", &TThis::TimestampProvider)
        .DefaultNew();

    registrar.Parameter("alien_timestamp_providers", &TThis::AlienProviders)
        .Default();

}

////////////////////////////////////////////////////////////////////////////////

void TTimestampProviderProgramConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
