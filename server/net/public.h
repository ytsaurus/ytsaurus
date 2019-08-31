#pragma once

// This header is the first intentionally.
#include <yp/server/lib/misc/public.h>

namespace NYP::NServer::NNet {

////////////////////////////////////////////////////////////////////////////////

using TProjectId = ui32;
using THostSubnet = ui64;
using TNonce = ui16;

class TInternetAddressManager;

DECLARE_REFCOUNTED_CLASS(TNetManagerConfig)

DECLARE_REFCOUNTED_STRUCT(TDnsSnapshot)
DECLARE_REFCOUNTED_CLASS(TNetManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNet
