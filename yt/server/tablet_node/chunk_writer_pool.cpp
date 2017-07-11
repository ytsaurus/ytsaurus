#include "private.h"
#include "config.h"
#include "in_memory_manager.h"
#include "in_memory_chunk_writer.h"
#include "tablet.h"
#include "chunk_writer_pool.h"

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/ytlib/table_client/versioned_reader.h>
#include <yt/ytlib/table_client/versioned_row.h>

#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/native_client.h>

namespace NYT {
namespace NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TChunkWriterPool::TChunkWriterPool(
    TInMemoryManagerPtr inMemoryManager,
    TTabletSnapshotPtr tabletSnapshot,
    int poolSize,
    TTableWriterConfigPtr writerConfig,
    TTabletWriterOptionsPtr writerOptions,
    INativeClientPtr client,
    const TTransactionId& transactionId)
    : InMemoryManager_(std::move(inMemoryManager))
    , TabletSnapshot_(std::move(tabletSnapshot))
    , PoolSize_(poolSize)
    , WriterConfig_(std::move(writerConfig))
    , WriterOptions_(std::move(writerOptions))
    , Client_(std::move(client))
    , TransactionId_(transactionId)
{ }

IVersionedMultiChunkWriterPtr TChunkWriterPool::AllocateWriter()
{
    if (FreshWriters_.empty()) {
        std::vector<TFuture<void>> asyncResults;
        for (int index = 0; index < PoolSize_; ++index) {
            auto blockCache = InMemoryManager_->CreateInterceptingBlockCache(
                TabletSnapshot_->Config->InMemoryMode,
                TabletSnapshot_->InMemoryConfigRevision);
            auto underlyingWriter = CreateVersionedMultiChunkWriter(
                WriterConfig_,
                WriterOptions_,
                TabletSnapshot_->PhysicalSchema,
                Client_,
                Client_->GetNativeConnection()->GetPrimaryMasterCellTag(),
                TransactionId_,
                NullChunkListId,
                GetUnlimitedThrottler(),
                blockCache);
            auto writer = CreateInMemoryVersionedMultiChunkWriter(
                InMemoryManager_,
                TabletSnapshot_,
                std::move(underlyingWriter));
            FreshWriters_.push_back(writer);
            asyncResults.push_back(writer->Open());
        }

        WaitFor(Combine(asyncResults))
            .ThrowOnError();
    }

    auto writer = FreshWriters_.back();
    FreshWriters_.pop_back();
    return writer;
}

void TChunkWriterPool::ReleaseWriter(IVersionedMultiChunkWriterPtr writer)
{
    ReleasedWriters_.push_back(writer);
    if (ReleasedWriters_.size() >= PoolSize_) {
        CloseWriters();
    }
}

const std::vector<IVersionedMultiChunkWriterPtr>& TChunkWriterPool::GetAllWriters()
{
    CloseWriters();
    return ClosedWriters_;
}

void TChunkWriterPool::CloseWriters()
{
    std::vector<TFuture<void>> asyncResults;
    for (const auto& writer : ReleasedWriters_) {
        asyncResults.push_back(writer->Close());
    }

    WaitFor(Combine(asyncResults))
        .ThrowOnError();

    ClosedWriters_.insert(ClosedWriters_.end(), ReleasedWriters_.begin(), ReleasedWriters_.end());
    ReleasedWriters_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

