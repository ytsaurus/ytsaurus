#pragma once

#include "replication_log_batch_reader.h"

#include <yt/yt/server/node/tablet_node/replication_log.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/table_client/config.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

TReplicationLogBatchDescriptor ReadReplicationBatch(
    const NTabletNode::TTabletSnapshotPtr& tabletSnapshot,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const NTableClient::TRowBatchReadOptions& rowBatchReadOptions,
    const NChaosClient::TReplicationProgress& progress,
    const NTabletNode::IReplicationLogParserPtr& logParser,
    NLogging::TLogger logger,
    i64 startRowIndex,
    NTransactionClient::TTimestamp upperTimestamp,
    i64 maxDataWeight,
    NTableClient::IWireProtocolWriter* writer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
