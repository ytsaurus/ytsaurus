#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TFetchRowsFromOrderedStoreResult
{
    std::vector<TSharedRef> Rowsets;

    i64 RowCount = 0;
    i64 DataWeight = 0;
};

TFuture<TFetchRowsFromOrderedStoreResult> FetchRowsFromOrderedStore(
    TTabletSnapshotPtr tabletSnapshot,
    const IOrderedStorePtr& store,
    int tabletIndex,
    i64 rowIndex,
    i64 maxRowCount,
    i64 maxDataWeight,
    i64 maxPullQueueResponseDataWeight,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
