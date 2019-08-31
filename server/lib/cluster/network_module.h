#pragma once

#include "public.h"

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

class TNetworkModule
    : public NYT::TRefTracked<TNetworkModule>
{
public:
    DEFINE_BYREF_RW_PROPERTY(int, AllocatedInternetAddressCount);
    DEFINE_BYREF_RW_PROPERTY(int, InternetAddressCount);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
