#include "tablet_replication_log_reader.h"
#include "replication_log_batch_reader.h"

#include <yt/yt/server/node/tablet_node/public.h>
#include <yt/yt/server/node/tablet_node/replication_log.h>
#include <yt/yt/server/node/tablet_node/tablet.h>
#include <yt/yt/server/node/tablet_node/tablet_reader.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/client/chaos_client/helpers.h>
#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NQueryAgent {

using namespace NConcurrency;
using namespace NChaosClient;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NTabletNode;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TTypeErasedRow ReadVersionedReplicationRow(
    const NTabletNode::TTabletSnapshotPtr& tabletSnapshot,
    const IReplicationLogParserPtr& logParser,
    const TRowBufferPtr& rowBuffer,
    TUnversionedRow replicationLogRow,
    TTimestamp* timestamp)
{
    TTypeErasedRow replicationRow;
    NApi::ERowModificationType modificationType;
    i64 rowIndex;

    logParser->ParseLogRow(
        tabletSnapshot,
        replicationLogRow,
        rowBuffer,
        &replicationRow,
        &modificationType,
        &rowIndex,
        timestamp,
        /*isVersioned*/ true);

    return replicationRow;
}

class TTabletBatchFetcher
    : public IReplicationLogBatchFetcher
{
public:
    TTabletBatchFetcher(
        TLegacyOwningKey lower,
        TLegacyOwningKey upper,
        const TColumnFilter& columnFilter,
        NTabletNode::TTabletSnapshotPtr tabletSnapshot,
        const TClientChunkReadOptions& chunkReaderOptions,
        const TRowBatchReadOptions& rowBatchReadOptions,
        const NTabletClient::TTabletId& tabletId,
        const NLogging::TLogger& logger)
        : TabletId_(tabletId)
        , RowBatchReadOptions_(rowBatchReadOptions)
        , Reader_(CreateSchemafulRangeTabletReader(
            std::move(tabletSnapshot),
            columnFilter,
            std::move(lower),
            std::move(upper),
            /*timestampRange*/ {},
            chunkReaderOptions,
            /*tabletThrottlerKind*/ std::nullopt,
            EWorkloadCategory::SystemTabletReplication))
        , Logger(logger)
    { }

    IUnversionedRowBatchPtr ReadNextRowBatch(i64 currentRowIndex) override
    {
        IUnversionedRowBatchPtr batch;
        while (true) {
            batch = Reader_->Read(RowBatchReadOptions_);
            if (!batch || !batch->IsEmpty()) {
                break;
            }

            YT_LOG_DEBUG("Waiting for replicated rows from tablet reader (TabletId: %v, StartRowIndex: %v)",
                TabletId_,
                currentRowIndex);

            WaitFor(Reader_->GetReadyEvent())
                .ThrowOnError();
        }

        return batch;
    }

private:
    const NTabletClient::TTabletId& TabletId_;
    const TRowBatchReadOptions& RowBatchReadOptions_;
    const ISchemafulUnversionedReaderPtr Reader_;
    const NLogging::TLogger Logger;
};

class TTabletRowBatchReader
    : public TReplicationLogBatchReaderBase
{
public:
    TTabletRowBatchReader(
        NTabletNode::TTabletSnapshotPtr tabletSnapshot,
        TClientChunkReadOptions chunkReaderOptions,
        TRowBatchReadOptions rowBatchReadOptions,
        TReplicationProgress progress,
        IWireProtocolWriter* writer,
        NLogging::TLogger logger)
        : TReplicationLogBatchReaderBase(
            tabletSnapshot->Settings.MountConfig,
            tabletSnapshot->TabletId,
            std::move(logger))
        , TablerSnapshotPtr_(std::move(tabletSnapshot))
        , ChunkReadOptions_(std::move(chunkReaderOptions))
        , RowBatchReadOptions_(std::move(rowBatchReadOptions))
        , Progress_(std::move(progress))
        , Writer_(writer)
    { }

protected:
    const NTabletNode::TTabletSnapshotPtr TablerSnapshotPtr_;
    const TClientChunkReadOptions ChunkReadOptions_;
    const TRowBatchReadOptions RowBatchReadOptions_;
    const TReplicationProgress Progress_;
    IWireProtocolWriter* Writer_;

    std::unique_ptr<IReplicationLogBatchFetcher> MakeBatchFetcher(
        TLegacyOwningKey lower,
        TLegacyOwningKey upper,
        const TColumnFilter& columnFilter) const override
    {
        return std::make_unique<TTabletBatchFetcher>(
            std::move(lower),
            std::move(upper),
            columnFilter,
            TablerSnapshotPtr_,
            ChunkReadOptions_,
            RowBatchReadOptions_,
            TabletId_,
            Logger);
    }
};

class TOrderedRowBatchReader
    : public TTabletRowBatchReader
{
public:
    TOrderedRowBatchReader(
        const NTabletNode::TTabletSnapshotPtr& tabletSnapshot,
        const TClientChunkReadOptions& chunkReaderOptions,
        const TRowBatchReadOptions& rowBatchReadOptions,
        const TReplicationProgress& progress,
        const IReplicationLogParserPtr& logParser,
        IWireProtocolWriter* writer,
        const NLogging::TLogger& logger)
        : TTabletRowBatchReader(
            tabletSnapshot,
            chunkReaderOptions,
            rowBatchReadOptions,
            progress,
            writer,
            logger)
    {
        ValidateOrderedTabletReplicationProgress(progress);
        if (!logParser->GetTimestampColumnId()) {
            THROW_ERROR_EXCEPTION("Invalid table schema: %Qlv column is absent",
                TimestampColumnName);
        }

        TimestampColumnIndex_ = *logParser->GetTimestampColumnId() + 1;
        TabletIndex_ = progress.Segments[0].LowerKey.GetCount() > 0
            ? FromUnversionedValue<i64>(progress.Segments[0].LowerKey[0])
            : 0;
    }

protected:
    TColumnFilter CreateColumnFilter() const override
    {
        // Without a filter first two columns are (tablet index, row index). Add tablet index column to row.
        TColumnFilter::TIndexes columnFilterIndexes{0};
        for (int id = 0; id < TablerSnapshotPtr_->TableSchema->GetColumnCount(); ++id) {
            columnFilterIndexes.push_back(id + 2);
        }

        return TColumnFilter(std::move(columnFilterIndexes));
    }

    TLegacyOwningKey MakeBoundKey(i64 currentRowIndex) const override
    {
        return MakeRowBound(currentRowIndex, TabletIndex_);
    }

    bool ToTypeErasedRow(
        const TUnversionedRow& row,
        const TRowBufferPtr& /*rowBuffer*/,
        TTypeErasedRow* replicationRow,
        TTimestamp* timestamp,
        i64* rowDataWeight) const override
    {
        auto rowTimestamp = row[TimestampColumnIndex_].Data.Uint64;
        // Check that row has greater timestamp than progress.
        if (rowTimestamp <= Progress_.Segments[0].Timestamp) {
            return false;
        }

        *timestamp = rowTimestamp;
        *replicationRow = row.ToTypeErasedRow();
        *rowDataWeight = GetDataWeight(TUnversionedRow(*replicationRow));
        return true;
    }

    void WriteTypeErasedRow(TTypeErasedRow row) override
    {
        Writer_->WriteSchemafulRow(TUnversionedRow(std::move(row)));
    }

private:
    int TimestampColumnIndex_ = 0;
    int TabletIndex_ = 0;
};

class TSortedRowBatchReader
    : public TTabletRowBatchReader
{
public:
    TSortedRowBatchReader(
        const NTabletNode::TTabletSnapshotPtr& tabletSnapshot,
        const TClientChunkReadOptions& chunkReaderOptions,
        const TRowBatchReadOptions& rowBatchReadOptions,
        const TReplicationProgress& progress,
        IReplicationLogParserPtr logParser,
        IWireProtocolWriter* writer,
        const NLogging::TLogger& logger)
        : TTabletRowBatchReader(
            tabletSnapshot,
            chunkReaderOptions,
            rowBatchReadOptions,
            progress,
            writer,
            logger)
        , LogParser_(std::move(logParser))
    { }

    TLegacyOwningKey MakeBoundKey(i64 currentRowIndex) const override
    {
        return MakeRowBound(currentRowIndex);
    }

    bool ToTypeErasedRow(
        const TUnversionedRow& row,
        const TRowBufferPtr& rowBuffer,
        TTypeErasedRow* replicationRow,
        TTimestamp* timestamp,
        i64* rowDataWeight) const override
    {
        *replicationRow = ReadVersionedReplicationRow(
            TablerSnapshotPtr_,
            LogParser_,
            rowBuffer,
            row,
            timestamp);
        auto versionedRow = TVersionedRow(*replicationRow);
        *rowDataWeight = GetDataWeight(versionedRow);
        // Check that row fits into replication progress key range and has greater timestamp than progress.
        auto progressTimestamp = FindReplicationProgressTimestampForKey(Progress_, versionedRow.Keys());
        return progressTimestamp && *progressTimestamp < *timestamp;
    }

    void WriteTypeErasedRow(TTypeErasedRow row) override
    {
        Writer_->WriteVersionedRow(TVersionedRow(std::move((row))));
    }

private:
    const IReplicationLogParserPtr LogParser_;
    const TRowBufferPtr RowBuffer_;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TReplicationLogBatchDescriptor ReadReplicationBatch(
    const NTabletNode::TTabletSnapshotPtr& tabletSnapshot,
    const TClientChunkReadOptions& chunkReadOptions,
    const TRowBatchReadOptions& rowBatchReadOptions,
    const TReplicationProgress& progress,
    const IReplicationLogParserPtr& logParser,
    TLogger logger,
    i64 startRowIndex,
    TTimestamp upperTimestamp,
    i64 maxDataWeight,
    IWireProtocolWriter* writer)
{
    if (tabletSnapshot->TableSchema->IsSorted()) {
        return NDetail::TSortedRowBatchReader(
            tabletSnapshot,
            chunkReadOptions,
            rowBatchReadOptions,
            progress,
            logParser,
            writer,
            logger)
            .ReadReplicationBatch(
                startRowIndex,
                upperTimestamp,
                maxDataWeight);
    } else {
        return NDetail::TOrderedRowBatchReader(
            tabletSnapshot,
            chunkReadOptions,
            rowBatchReadOptions,
            progress,
            logParser,
            writer,
            logger)
            .ReadReplicationBatch(
                startRowIndex,
                upperTimestamp,
                maxDataWeight);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
