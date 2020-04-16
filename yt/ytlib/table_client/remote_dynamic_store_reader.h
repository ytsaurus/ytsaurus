#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/client/node_tracker_client/public.h>

#include <yt/client/tablet_client/public.h>

#include <yt/core/concurrency/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRemoteDynamicStoreReader(
    const NChunkClient::NProto::TChunkSpec& chunkSpec,
    TTableSchema schema,
    NTabletClient::TRemoteDynamicStoreReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NChunkClient::TTrafficMeterPtr trafficMeter,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT:NTabletClient
