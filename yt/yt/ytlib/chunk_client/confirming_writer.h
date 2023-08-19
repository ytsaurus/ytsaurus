#pragma once

#include "public.h"
#include "chunk_writer.h"
#include "client_block_cache.h"
#include "session_id.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateConfirmingWriter(
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    NObjectClient::TCellTag cellTag,
    NTransactionClient::TTransactionId transactionId,
    NTableClient::TMasterTableSchemaId schemaId,
    TChunkListId parentChunkListId,
    NApi::NNative::IClientPtr client,
    TString localHostName,
    IBlockCachePtr blockCache = GetNullBlockCache(),
    TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    TSessionId sessionId = {},
    TChunkReplicaWithMediumList targetReplicas = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

