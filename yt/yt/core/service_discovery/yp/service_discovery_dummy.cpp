#include "service_discovery.h"

#include "config.h"

namespace NYT::NServiceDiscovery::NYP {

////////////////////////////////////////////////////////////////////////////////

IServiceDiscoveryPtr CreateServiceDiscovery(TServiceDiscoveryConfigPtr /*config*/)
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery::NYP
