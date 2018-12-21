#pragma once

#include <yp/server/misc/public.h>

namespace NYP::NServer::NNet {

////////////////////////////////////////////////////////////////////////////////

using TProjectId = ui32;
using THostSubnet = ui64;
using TNonce = ui16;

DECLARE_REFCOUNTED_CLASS(TNetManagerConfig)

DECLARE_REFCOUNTED_STRUCT(TDnsSnapshot)
DECLARE_REFCOUNTED_CLASS(TNetManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNet
