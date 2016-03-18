#include "ordered_dynamic_store.h"
#include "tablet.h"
#include "automaton.h"
#include "config.h"

#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/chunked_memory_pool.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/ytlib/table_client/schemaful_reader_adapter.h>
#include <yt/ytlib/table_client/schemaful_writer_adapter.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/schemaful_writer.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>

#include <yt/ytlib/chunk_client/config.h>
#include <yt/ytlib/chunk_client/memory_reader.h>
#include <yt/ytlib/chunk_client/memory_writer.h>
#include <yt/ytlib/chunk_client/chunk_spec.pb.h>
#include <yt/ytlib/chunk_client/chunk_meta.pb.h>

namespace NYT {
namespace NTabletNode {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NConcurrency;

using NChunkClient::NProto::TChunkSpec;
using NChunkClient::NProto::TChunkMeta;

////////////////////////////////////////////////////////////////////////////////

static const size_t ReaderPoolSize = (size_t) 16 * 1024;
static const int SnapshotRowsPerRead = 1024;

struct TOrderedDynamicStoreReaderPoolTag
{ };

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicStore::TReader
    : public ISchemafulReader
{
public:
    TReader(
        TOrderedDynamicStorePtr store,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        const TNullable<TTableSchema>& schema)
        : Store_(std::move(store))
        , UpperRowIndex_(std::min(upperRowIndex, Store_->GetRowCount()))
        , Schema_(schema)
        , CurrentRowIndex_(std::max(lowerRowIndex, static_cast<i64>(0)))
    { }

    void Initialize()
    {
        if (!Schema_) {
            return;
        }

        Pool_ = std::make_unique<TChunkedMemoryPool>(TOrderedDynamicStoreReaderPoolTag(), ReaderPoolSize);

        if (Schema_->GetKeyColumnCount() > 0) {
            THROW_ERROR_EXCEPTION("Reader schema cannot contain column keys");
        }

        const auto& srcTableSchema = Store_->Schema_;
        const auto& dstTableSchema = *Schema_;
        for (int dstIndex = 0; dstIndex < dstTableSchema.Columns().size(); ++dstIndex) {
            const auto& dstColumnSchema = dstTableSchema.Columns()[dstIndex];
            int srcIndex = srcTableSchema.GetColumnIndexOrThrow(dstColumnSchema.Name);
            const auto& srcColumnSchema = srcTableSchema.Columns()[srcIndex];
            if (srcColumnSchema.Type != dstColumnSchema.Type) {
                THROW_ERROR_EXCEPTION("Reader schema for column %Qv has invalid type: expected %Qlv, got %Qlv",
                    srcColumnSchema.Name,
                    srcColumnSchema.Type,
                    dstColumnSchema.Type);
            }
            IdMapping_.push_back(&srcColumnSchema - srcTableSchema.Columns().data());
        }
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        rows->clear();
        while (rows->size() < rows->capacity() && CurrentRowIndex_ < UpperRowIndex_) {
            rows->push_back(CaptureRow(Store_->GetRow(CurrentRowIndex_)));
            ++CurrentRowIndex_;
        }
        return !rows->empty();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        YUNREACHABLE();
    }

private:
    const TOrderedDynamicStorePtr Store_;
    const i64 UpperRowIndex_;
    const TNullable<TTableSchema> Schema_;

    std::unique_ptr<TChunkedMemoryPool> Pool_;

    i64 CurrentRowIndex_;

    //! Maps Schema_ ids to Store_->Schema_ ids.
    SmallVector<int, TypicalColumnCount> IdMapping_;


    TUnversionedRow CaptureRow(TOrderedDynamicRow dynamicRow)
    {
        if (!Schema_) {
            // For flushes and snapshots only.
            return dynamicRow;
        }

        int columnCount = Schema_->Columns().size();
        auto row = TMutableUnversionedRow::Allocate(Pool_.get(), columnCount);
        for (int index = 0; index < columnCount; ++index) {
            auto& dstValue = row[index];
            const auto& srcValue = dynamicRow[IdMapping_[index]];
            dstValue = srcValue;
            dstValue.Id = index;
        }
        return row;
    }
};

////////////////////////////////////////////////////////////////////////////////

TOrderedDynamicStore::TOrderedDynamicStore(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet)
    : TStoreBase(config, id, tablet)
    , TDynamicStoreBase(config, id, tablet)
    , TOrderedStoreBase(config, id, tablet)
{
    AllocateCurrentSegment(InitialOrderedDynamicSegmentIndex);

    LOG_DEBUG("Ordered dynamic store created (TabletId: %v)",
        TabletId_);
}

TOrderedDynamicStore::~TOrderedDynamicStore()
{
    LOG_DEBUG("Ordered dynamic memory store destroyed");
}

ISchemafulReaderPtr TOrderedDynamicStore::CreateFlushReader()
{
    YCHECK(FlushRowCount_ != -1);
    return DoCreateReader(
        0,
        FlushRowCount_,
        Null);
}

ISchemafulReaderPtr TOrderedDynamicStore::CreateSnapshotReader()
{
    return DoCreateReader(
        0,
        GetRowCount(),
        Null);
}

TOrderedDynamicRow TOrderedDynamicStore::WriteRow(TTransaction* /*transaction*/, TUnversionedRow row)
{
    YASSERT(Atomicity_ == EAtomicity::Full);

    auto result = DoWriteSchemalessRow(row);

    OnMemoryUsageUpdated();

    ++PerformanceCounters_->DynamicRowWriteCount;

    return result;
}

TOrderedDynamicRow TOrderedDynamicStore::MigrateRow(TTransaction* /*transaction*/, TOrderedDynamicRow row)
{
    auto result = DoWriteSchemafulRow(row);

    OnMemoryUsageUpdated();

    return result;
}

void TOrderedDynamicStore::PrepareRow(TTransaction* /*transaction*/, TOrderedDynamicRow /*row*/)
{ }

void TOrderedDynamicStore::CommitRow(TTransaction* /*transaction*/, TOrderedDynamicRow row)
{
    DoCommitRow(row);
}

void TOrderedDynamicStore::AbortRow(TTransaction* /*transaction*/, TOrderedDynamicRow /*row*/)
{ }

TOrderedDynamicRow TOrderedDynamicStore::GetRow(i64 rowIndex)
{
    YASSERT(rowIndex >= 0 && rowIndex < StoreRowCount_);
    int segmentIndex;
    i64 segmentRowIndex;
    if (rowIndex < (1ULL << InitialOrderedDynamicSegmentIndex)) {
        segmentIndex = InitialOrderedDynamicSegmentIndex;
        segmentRowIndex = rowIndex;
    } else {
        segmentIndex = 64 - __builtin_clzl(rowIndex);
        segmentRowIndex = rowIndex - (1ULL << (segmentIndex - 1));
    }
    return TOrderedDynamicRow((*Segments_[segmentIndex])[segmentRowIndex]);
}

std::vector<TOrderedDynamicRow> TOrderedDynamicStore::GetAllRows()
{
    std::vector<TOrderedDynamicRow> rows;
    for (int index = 0; index < GetRowCount(); ++index) {
        rows.push_back(GetRow(index));
    }
    return rows;
}

EStoreType TOrderedDynamicStore::GetType() const
{
    return EStoreType::OrderedDynamic;
}

i64 TOrderedDynamicStore::GetRowCount() const
{
    return StoreRowCount_;
}

TCallback<void(TSaveContext&)> TOrderedDynamicStore::AsyncSave()
{
    using NYT::Save;

    auto tableReader = CreateSnapshotReader();

    return BIND([=, this_ = MakeStrong(this)] (TSaveContext& context) {
        auto chunkWriter = New<TMemoryWriter>();
        auto tableWriterConfig = New<TChunkWriterConfig>();
        auto tableWriterOptions = New<TChunkWriterOptions>();
        auto nameTable = TNameTable::FromSchema(Schema_);
        // TODO(babenko): replace with native schemaful writer
        auto schemalessTableWriter = CreateSchemalessChunkWriter(
            tableWriterConfig,
            tableWriterOptions,
            nameTable,
            TKeyColumns(),
            chunkWriter);
        auto tableWriter = CreateSchemafulWriterAdapter(schemalessTableWriter);

        WaitFor(schemalessTableWriter->Open())
            .ThrowOnError();

        std::vector<TUnversionedRow> rows;
        rows.reserve(SnapshotRowsPerRead);

        i64 rowCount = 0;
        while (tableReader->Read(&rows)) {
            if (rows.empty()) {
                WaitFor(tableReader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            rowCount += rows.size();
            if (!tableWriter->Write(rows)) {
                WaitFor(tableWriter->GetReadyEvent())
                    .ThrowOnError();
            }
        }

        // pushsin@ forbids empty chunks.
        if (rowCount == 0) {
            Save(context, false);
            return;
        }

        Save(context, true);

        // NB: This also closes chunkWriter.
        WaitFor(tableWriter->Close())
            .ThrowOnError();

        Save(context, chunkWriter->GetChunkMeta());
        Save(context, chunkWriter->GetBlocks());
    });
}

void TOrderedDynamicStore::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;

    if (Load<bool>(context)) {
        auto chunkMeta = Load<TChunkMeta>(context);
        auto blocks = Load<std::vector<TSharedRef>>(context);

        auto chunkReader = CreateMemoryReader(chunkMeta, blocks);

        auto readerFactory = [&] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) -> ISchemalessReaderPtr {
            TChunkSpec chunkSpec;
            ToProto(chunkSpec.mutable_chunk_id(), StoreId_);
            *chunkSpec.mutable_chunk_meta() = chunkMeta;
            auto tableReaderConfig = New<TChunkReaderConfig>();
            auto tableReaderOptions = New<TChunkReaderOptions>();
            return CreateSchemalessChunkReader(
                chunkSpec,
                tableReaderConfig,
                tableReaderOptions,
                chunkReader,
                nameTable,
                GetNullBlockCache(),
                TKeyColumns(),
                columnFilter,
                std::vector<TReadRange>{TReadRange()});
        };

        // TODO(babenko): replace with native schemaful writer
        auto tableReader = CreateSchemafulReaderAdapter(readerFactory, Schema_);

        std::vector<TUnversionedRow> rows;
        rows.reserve(SnapshotRowsPerRead);

        while (tableReader->Read(&rows)) {
            if (rows.empty()) {
                WaitFor(tableReader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            for (auto row : rows) {
                LoadRow(row);
            }
        }
    }

    if (StoreState_ == EStoreState::PassiveDynamic) {
        // NB: No more changes are possible after load.
        YCHECK(FlushRowCount_ == -1);
        FlushRowCount_ = GetRowCount();
    }

    OnMemoryUsageUpdated();
}

TOrderedDynamicStorePtr TOrderedDynamicStore::AsOrderedDynamic()
{
    return this;
}

ISchemafulReaderPtr TOrderedDynamicStore::CreateReader(
    i64 lowerRowIndex,
    i64 upperRowIndex,
    const TTableSchema& schema,
    const TWorkloadDescriptor& /*workloadDescriptor*/)
{
    return DoCreateReader(
        lowerRowIndex,
        upperRowIndex,
        schema);
}

void TOrderedDynamicStore::OnSetPassive()
{
    YCHECK(FlushRowCount_ == -1);
    FlushRowCount_ = GetRowCount();
}

void TOrderedDynamicStore::AllocateCurrentSegment(int index)
{
    CurrentSegmentIndex_ = index;
    CurrentSegmentCapacity_ = 1LL << (index - (index == InitialOrderedDynamicSegmentIndex ? 0 : 1));
    CurrentSegmentSize_ = 0;
    Segments_[CurrentSegmentIndex_] = std::make_unique<TOrderedDynamicRowSegment>(CurrentSegmentCapacity_);
}

void TOrderedDynamicStore::OnMemoryUsageUpdated()
{
    SetMemoryUsage(GetUncompressedDataSize());
}

TOrderedDynamicRow TOrderedDynamicStore::DoWriteSchemafulRow(TUnversionedRow row)
{
    return RowBuffer_->Capture(row, true);
}

TOrderedDynamicRow TOrderedDynamicStore::DoWriteSchemalessRow(TUnversionedRow row)
{
    int columnCount = Schema_.Columns().size();
    auto dynamicRow = TMutableUnversionedRow::Allocate(RowBuffer_->GetPool(), columnCount);
    for (int index = 0; index < columnCount; ++index) {
        dynamicRow[index] = MakeSentinelValue<TUnversionedValue>(EValueType::Null, index);
    }
    for (int index = 0; index < row.GetCount(); ++index) {
        const auto& srcValue = row[index];
        auto& dstValue = dynamicRow[srcValue.Id];
        dstValue = RowBuffer_->Capture(srcValue);
    }
    return dynamicRow;
}

void TOrderedDynamicStore::DoCommitRow(TOrderedDynamicRow row)
{
    if (CurrentSegmentSize_ == CurrentSegmentCapacity_) {
        AllocateCurrentSegment(CurrentSegmentIndex_ + 1);
    }
    (*Segments_[CurrentSegmentIndex_])[CurrentSegmentSize_] = row.GetHeader();
    ++CurrentSegmentSize_;
    StoreRowCount_ += 1;
    StoreValueCount_ += row.GetCount();
}

void TOrderedDynamicStore::LoadRow(TUnversionedRow row)
{
    DoCommitRow(DoWriteSchemafulRow(row));
}

ISchemafulReaderPtr TOrderedDynamicStore::DoCreateReader(
    i64 lowerRowIndex,
    i64 upperRowIndex,
    const TNullable<TTableSchema>& schema)
{
    auto reader = New<TReader>(
        this,
        lowerRowIndex,
        upperRowIndex,
        schema);
    reader->Initialize();
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
