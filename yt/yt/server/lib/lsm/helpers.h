#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/public.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

constexpr auto CompactionTimestampAccuracy = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

TRowDigestUpcomingCompactionInfo GetUpcomingCompactionInfo(
    TStoreId storeId,
    const TTableMountConfigPtr& mountConfig,
    const NTableClient::TVersionedRowDigest& digest);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
