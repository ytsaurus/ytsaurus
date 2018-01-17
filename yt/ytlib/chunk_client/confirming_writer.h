#pragma once

#include "public.h"
#include "chunk_writer.h"
#include "client_block_cache.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateConfirmingWriter(
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    NObjectClient::TCellTag cellTag,
    const NTransactionClient::TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NApi::INativeClientPtr client,
    IBlockCachePtr blockCache = GetNullBlockCache(),
    TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

