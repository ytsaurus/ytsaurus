#pragma once

#include "public.h"

#include <yt/core/ypath/public.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetCypressClustersPath();
NYPath::TYPath GetCypressClusterPath(const TString& name);

bool IsChunkTabletStoreType(NObjectClient::EObjectType type);
bool IsDynamicTabletStoreType(NObjectClient::EObjectType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

