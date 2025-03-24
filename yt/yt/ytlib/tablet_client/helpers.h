#pragma once

#include "public.h"

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetCypressClustersPath();
NYPath::TYPath GetCypressClusterPath(TStringBuf name);

bool IsChunkTabletStoreType(NObjectClient::EObjectType type);
bool IsDynamicTabletStoreType(NObjectClient::EObjectType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

