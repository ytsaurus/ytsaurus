#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/public.h>

namespace NYT::NFileClient {

////////////////////////////////////////////////////////////////////////////////

NApi::IFileFragmentWriterPtr CreateFileFragmentWriter(
    NApi::TFileWriterConfigPtr config,
    NApi::TWriteFileFragmentCookie cookie,
    NApi::NNative::IClientPtr client,
    NCypressClient::TTransactionId transactionId,
    NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
