#pragma once

#include "public.h"

#include <yt/client/object_client/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

void AddCellTagToSyncWith(const NRpc::IClientRequestPtr& request, NObjectClient::TCellTag cellTag);
void AddCellTagToSyncWith(const NRpc::IClientRequestPtr& request, NObjectClient::TObjectId objectId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
