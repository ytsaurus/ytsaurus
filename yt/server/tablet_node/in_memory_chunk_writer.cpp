#include "tablet.h"
#include "in_memory_chunk_writer.h"
#include "in_memory_manager.h"

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/table_client/versioned_chunk_writer.h>

namespace NYT {
namespace NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NTableClient;

using NYT::TRange;

////////////////////////////////////////////////////////////////////////////////

template <class TWriter, class TRow>
class TInMemoryChunkWriterBase
    : public TWriter
{
public:
    TInMemoryChunkWriterBase(
        TInMemoryManagerPtr inMemoryManager,
        TTabletSnapshotPtr tabletSnapshot,
        TIntrusivePtr<TWriter> underlyingWriter,
        IChunkWriterPtr underlyingChunkWriter)
        : InMemoryManager_(std::move(inMemoryManager))
        , TabletSnapshot_(std::move(tabletSnapshot))
        , UnderlyingWriter_(std::move(underlyingWriter))
        , UnderlyingChunkWriter_(std::move(underlyingChunkWriter))
    { }

    virtual bool Write(const TRange<TRow>& rows) override
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

            InMemoryManager_->FinalizeChunk(
                UnderlyingChunkWriter_->GetChunkId(),
                GetNodeMeta(),
                TabletSnapshot_);
        }));

        return result;
    }

    virtual i64 GetMetaSize() const override
    {
        return UnderlyingWriter_->GetMetaSize();
    }

    virtual i64 GetDataSize() const override
    {
        return UnderlyingWriter_->GetDataSize();
    }

    virtual bool IsCloseDemanded() const override
    {
        return UnderlyingWriter_->IsCloseDemanded();
    }

    virtual TChunkMeta GetMasterMeta() const override
    {
        return UnderlyingWriter_->GetMasterMeta();
    }

    virtual TChunkMeta GetSchedulerMeta() const override
    {
        return UnderlyingWriter_->GetSchedulerMeta();
    }

    virtual TChunkMeta GetNodeMeta() const override
    {
        return UnderlyingWriter_->GetNodeMeta();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingWriter_->GetDataStatistics();
    }

protected:
    const TInMemoryManagerPtr InMemoryManager_;
    const TTabletSnapshotPtr TabletSnapshot_;
    const TIntrusivePtr<TWriter> UnderlyingWriter_;
    const IChunkWriterPtr UnderlyingChunkWriter_;
};

////////////////////////////////////////////////////////////////////////////////

class TInMemorySchemalessChunkWriter
    : public TInMemoryChunkWriterBase<ISchemalessChunkWriter, TUnversionedRow>
{
    using TBase = TInMemoryChunkWriterBase<ISchemalessChunkWriter, TUnversionedRow>;

public:
    TInMemorySchemalessChunkWriter(
        TInMemoryManagerPtr inMemoryManager,
        TTabletSnapshotPtr tabletSnapshot,
        ISchemalessChunkWriterPtr underlyingWriter,
        IChunkWriterPtr underlyingChunkWriter)
        : TBase(std::move(inMemoryManager),
            std::move(tabletSnapshot),
            std::move(underlyingWriter),
            std::move(underlyingChunkWriter))
    { }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return UnderlyingWriter_->GetNameTable();
    }

    virtual const TTableSchema& GetSchema() const override
    {
        return UnderlyingWriter_->GetSchema();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TInMemoryVersionedChunkWriter
    : public TInMemoryChunkWriterBase<IVersionedChunkWriter, TVersionedRow>
{
    using TBase = TInMemoryChunkWriterBase<IVersionedChunkWriter, TVersionedRow>;

public:
    TInMemoryVersionedChunkWriter(
        TInMemoryManagerPtr inMemoryManager,
        TTabletSnapshotPtr tabletSnapshot,
        IVersionedChunkWriterPtr underlyingWriter,
        IChunkWriterPtr underlyingChunkWriter)
        : TBase(std::move(inMemoryManager),
            std::move(tabletSnapshot),
            std::move(underlyingWriter),
            std::move(underlyingChunkWriter))
    { }

    virtual i64 GetRowCount() const override
    {
        return UnderlyingWriter_->GetRowCount();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TInMemoryVersionedMultiChunkWriter
    : public IVersionedMultiChunkWriter
{
public:
    TInMemoryVersionedMultiChunkWriter(
        TInMemoryManagerPtr inMemoryManager,
        TTabletSnapshotPtr tabletSnapshot,
        IVersionedMultiChunkWriterPtr underlyingWriter)
        : InMemoryManager_(std::move(inMemoryManager))
        , TabletSnapshot_(std::move(tabletSnapshot))
        , UnderlyingWriter_(std::move(underlyingWriter))
    { }

    virtual bool Write(const TRange<TVersionedRow>& rows) override
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

ISchemalessChunkWriterPtr CreateInMemorySchemalessChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TInMemoryManagerPtr inMemoryManager,
    TTabletSnapshotPtr tabletSnapshot,
    IChunkWriterPtr chunkWriter,
    const TChunkTimestamps& chunkTimestamps,
    NChunkClient::IBlockCachePtr blockCache)
{
    auto underlyingWriter = CreateSchemalessChunkWriter(
        config,
        options,
        tabletSnapshot->PhysicalSchema,
        chunkWriter,
        chunkTimestamps,
        std::move(blockCache));

    return New<TInMemorySchemalessChunkWriter>(
        std::move(inMemoryManager),
        std::move(tabletSnapshot),
        std::move(underlyingWriter),
        std::move(chunkWriter));
}

IVersionedChunkWriterPtr CreateInMemoryVersionedChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TInMemoryManagerPtr inMemoryManager,
    TTabletSnapshotPtr tabletSnapshot,
    IChunkWriterPtr chunkWriter,
    NChunkClient::IBlockCachePtr blockCache)
{
    auto underlyingWriter = CreateVersionedChunkWriter(
        config,
        options,
        tabletSnapshot->PhysicalSchema,
        chunkWriter,
        std::move(blockCache));

    return New<TInMemoryVersionedChunkWriter>(
        std::move(inMemoryManager),
        std::move(tabletSnapshot),
        std::move(underlyingWriter),
        std::move(chunkWriter));
}

IVersionedMultiChunkWriterPtr CreateInMemoryVersionedMultiChunkWriter(
    TInMemoryManagerPtr inMemoryManager,
    TTabletSnapshotPtr tabletSnapshot,
    IVersionedMultiChunkWriterPtr underlyingWriter)
{
    return New<TInMemoryVersionedMultiChunkWriter>(
        std::move(inMemoryManager),
        std::move(tabletSnapshot),
        std::move(underlyingWriter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT


