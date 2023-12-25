#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

// Fetch chunk view sizes to determine if they need to be compacted due to too narrow bounds.
struct IChunkViewSizeFetcher
    : public TRefCounted
{
    virtual void FetchChunkViewSizes(
        TTablet* tablet,
        std::optional<TRange<IStorePtr>> stores = std::nullopt) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkViewSizeFetcher)

////////////////////////////////////////////////////////////////////////////////

IChunkViewSizeFetcherPtr CreateChunkViewSizeFetcher(
    TTabletCellId cellId,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr automatonInvoker,
    IInvokerPtr heavyInvoker,
    NApi::NNative::IClientPtr client,
    NChunkClient::IChunkReplicaCachePtr chunkReplicaCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

