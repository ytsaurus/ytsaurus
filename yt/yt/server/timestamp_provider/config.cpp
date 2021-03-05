#include "config.h"

#include <yt/yt/client/transaction_client/config.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

TTimestampProviderConfig::TTimestampProviderConfig()
{
    RegisterParameter("abort_on_unrecognized_options", AbortOnUnrecognizedOptions)
        .Default(false);

    RegisterParameter("bus_client", BusClient)
        .DefaultNew();

    RegisterParameter("timestamp_provider", TimestampProvider)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
