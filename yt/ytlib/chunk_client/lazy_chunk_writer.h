#pragma once

#include "public.h"

#include "chunk_writer.h"
#include "client_block_cache.h"

#include <ytlib/transaction_client/public.h>
#include <ytlib/api/public.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/concurrency/public.h>
#include <core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateLazyChunkWriter(
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    const NTransactionClient::TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NApi::IClientPtr client,
    IBlockCachePtr blockCache = GetNullBlockCache(),
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

