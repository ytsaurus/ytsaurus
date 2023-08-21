#include "config.h"

#include <yt/yt/client/transaction_client/config.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

void TTimestampProviderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("bus_client", &TThis::BusClient)
        .DefaultNew();

    registrar.Parameter("timestamp_provider", &TThis::TimestampProvider)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
