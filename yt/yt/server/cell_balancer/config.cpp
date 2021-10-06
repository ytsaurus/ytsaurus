#include "config.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

TCellBalancerConfig::TCellBalancerConfig()
{
    RegisterParameter("abort_on_unrecognized_options", AbortOnUnrecognizedOptions)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
