#include "private.h"
#include "config.h"
#include "in_memory_manager.h"
#include "tablet.h"
#include "writer_pool.h"

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/ytlib/table_client/versioned_reader.h>
#include <yt/ytlib/table_client/versioned_row.h>

namespace NYT {
namespace NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterPool::TFinalizingWriter
    : public IVersionedMultiChunkWriter
{
public:
    TFinalizingWriter(
        TInMemoryManagerPtr inMemoryManager,
        TTabletSnapshotPtr tabletSnapshot,
        IVersionedMultiChunkWriterPtr underlyingWriter)
        : InMemoryManager_(std::move(inMemoryManager))
        , TabletSnapshot_(std::move(tabletSnapshot))
        , UnderlyingWriter_(std::move(underlyingWriter))
    { }

    virtual bool Write(const std::vector<TVersionedRow>& rows) override
    {
        return UnderlyingWriter_->Write(rows);
    }

    virtual TFuture<void> Open() override
    {
        return UnderlyingWriter_->Open();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return UnderlyingWriter_->GetReadyEvent();
    }

    virtual TFuture<void> Close() override
    {
        auto result = UnderlyingWriter_->Close();
        result.Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<void>& valueOrError) {
            if (!valueOrError.IsOK()) {
                return;
            }

            auto chunkSpecs = GetWrittenChunksFullMeta();
            for (const auto& chunkSpec : chunkSpecs) {
                InMemoryManager_->FinalizeChunk(
                    FromProto<TChunkId>(chunkSpec.chunk_id()),
                    chunkSpec.chunk_meta(),
                    TabletSnapshot_);
            }
        }));

        return result;
    }

    virtual void SetProgress(double progress) override
    {
        UnderlyingWriter_->SetProgress(progress);
    }

    virtual const std::vector<TChunkSpec>& GetWrittenChunksMasterMeta() const override
    {
        return UnderlyingWriter_->GetWrittenChunksMasterMeta();
    }

    virtual const std::vector<TChunkSpec>& GetWrittenChunksFullMeta() const override
    {
        return UnderlyingWriter_->GetWrittenChunksFullMeta();
    }

    virtual TNodeDirectoryPtr GetNodeDirectory() const override
    {
        return UnderlyingWriter_->GetNodeDirectory();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingWriter_->GetDataStatistics();
    }

private:
    const TInMemoryManagerPtr InMemoryManager_;
    const TTabletSnapshotPtr TabletSnapshot_;
    const IVersionedMultiChunkWriterPtr UnderlyingWriter_;
};

////////////////////////////////////////////////////////////////////////////////

TChunkWriterPool::TChunkWriterPool(
    TInMemoryManagerPtr inMemoryManager,
    TTabletSnapshotPtr tabletSnapshot,
    int poolSize,
    TTableWriterConfigPtr writerConfig,
    TTabletWriterOptionsPtr writerOptions,
    TTableMountConfigPtr tabletConfig,
    const TTableSchema& schema,
    IClientPtr client,
    const TTransactionId& transactionId)
    : InMemoryManager_(std::move(inMemoryManager))
    , TabletSnapshot_(std::move(tabletSnapshot))
    , PoolSize_(poolSize)
    , WriterConfig_(std::move(writerConfig))
    , WriterOptions_(std::move(writerOptions))
    , TabletConfig_(std::move(tabletConfig))
    , Schema_(schema)
    , Client_(std::move(client))
    , TransactionId_(transactionId)
{ }

IVersionedMultiChunkWriterPtr TChunkWriterPool::AllocateWriter()
{
    if (FreshWriters_.empty()) {
        std::vector<TFuture<void>> asyncResults;
        for (int index = 0; index < PoolSize_; ++index) {
            auto blockCache = InMemoryManager_->CreateInterceptingBlockCache(TabletConfig_->InMemoryMode);
            auto underlyingWriter = CreateVersionedMultiChunkWriter(
                WriterConfig_,
                WriterOptions_,
                Schema_,
                Client_,
                Client_->GetConnection()->GetPrimaryMasterCellTag(),
                TransactionId_,
                NullChunkListId,
                GetUnlimitedThrottler(),
                blockCache);
            auto writer = New<TFinalizingWriter>(InMemoryManager_, TabletSnapshot_, std::move(underlyingWriter));
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

