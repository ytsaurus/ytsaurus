#pragma once

#include "public.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

void AddCellTagToSyncWith(const NRpc::IClientRequestPtr& request, NObjectClient::TCellTag cellTag);
void AddCellTagToSyncWith(const NRpc::IClientRequestPtr& request, NObjectClient::TObjectId objectId);

bool GetSuppressUpstreamSync(const NRpc::NProto::TRequestHeader& requestHeader);
bool GetSuppressTransactionCoordinatorSync(const NRpc::NProto::TRequestHeader& requestHeader);

void SetSuppressUpstreamSync(NRpc::NProto::TRequestHeader* requestHeader, bool value);
void SetSuppressTransactionCoordinatorSync(NRpc::NProto::TRequestHeader* requestHeader, bool value);

bool IsRetriableObjectServiceError(int /*attempt*/, const TError& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
