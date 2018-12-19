#pragma once

#include "public.h"
#include "chunk_writer.h"
#include "client_block_cache.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateConfirmingWriter(
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    NObjectClient::TCellTag cellTag,
    NTransactionClient::TTransactionId transactionId,
    TChunkListId parentChunkListId,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NApi::NNative::IClientPtr client,
    IBlockCachePtr blockCache = GetNullBlockCache(),
    TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

