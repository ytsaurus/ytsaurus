#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/config.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerConfig
    : public TServerConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    TCellBalancerConfig();
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
