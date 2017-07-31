#pragma once

#include "config.h"
#include "in_memory_manager.h"
#include "tablet.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/ytlib/table_client/versioned_row.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterPool
{
public:
    TChunkWriterPool(
        TInMemoryManagerPtr inMemoryManager,
        TTabletSnapshotPtr tabletSnapshot,
        int poolSize,
        NTableClient::TTableWriterConfigPtr writerConfig,
        TTabletWriterOptionsPtr writerOptions,
        NApi::INativeClientPtr client,
        const NObjectClient::TTransactionId& transactionId);

    NTableClient::IVersionedMultiChunkWriterPtr AllocateWriter();

    void ReleaseWriter(NTableClient::IVersionedMultiChunkWriterPtr writer);

    const std::vector<NTableClient::IVersionedMultiChunkWriterPtr>& GetAllWriters();

private:
    const TInMemoryManagerPtr InMemoryManager_;
    const TTabletSnapshotPtr TabletSnapshot_;
    const int PoolSize_;
    const NTableClient::TTableWriterConfigPtr WriterConfig_;
    const TTabletWriterOptionsPtr WriterOptions_;
    const NApi::INativeClientPtr Client_;
    const NObjectClient::TTransactionId TransactionId_;

    std::vector<NTableClient::IVersionedMultiChunkWriterPtr> FreshWriters_;
    std::vector<NTableClient::IVersionedMultiChunkWriterPtr> ReleasedWriters_;
    std::vector<NTableClient::IVersionedMultiChunkWriterPtr> ClosedWriters_;

    void CloseWriters();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT


