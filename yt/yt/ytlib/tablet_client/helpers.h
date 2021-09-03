#pragma once

#include "public.h"

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

//! Represents that only values with timestamp in range [|RetentionTimestamp|, |Timestamp|]
//! should be read.
//! Note that both endpoints are inclusive.
struct TReadTimestampRange
{
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::NullTimestamp;
    NTransactionClient::TTimestamp RetentionTimestamp = NTransactionClient::NullTimestamp;
};

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetCypressClustersPath();
NYPath::TYPath GetCypressClusterPath(const TString& name);

bool IsChunkTabletStoreType(NObjectClient::EObjectType type);
bool IsDynamicTabletStoreType(NObjectClient::EObjectType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient

