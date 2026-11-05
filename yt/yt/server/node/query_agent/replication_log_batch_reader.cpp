#include "replication_log_batch_reader.h"

#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/server/lib/tablet_node/config.h>

namespace NYT::NQueryAgent {

using namespace NTabletNode;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NLogging;

// Default chunk reading size.
static constexpr i64 MinBatchWeight = 16_MB;

////////////////////////////////////////////////////////////////////////////////

TReplicationLogBatchReaderBase::TReplicationLogBatchReaderBase(
    TTableMountConfigPtr mountConfig,
    TTabletId tabletId,
    IReservingMemoryUsageTrackerPtr memoryUsageTracker,
    TLogger logger)
    : TableMountConfig_(std::move(mountConfig))
    , TabletId_(std::move(tabletId))
    , MemoryUsageTracker_(std::move(memoryUsageTracker))
    , Logger(std::move(logger))
{ }

TColumnFilter TReplicationLogBatchReaderBase::CreateColumnFilter() const
{
    return TColumnFilter();
}

TReplicationLogBatchDescriptor TReplicationLogBatchReaderBase::ReadReplicationBatch(
    i64 startRowIndex,
    TTimestamp upperTimestamp,
    i64 maxDataWeight,
    i64 readDataWeightLimit,
    TInstant maxAllowedCommitInstant,
    TInstant requestDeadLine)
{
    auto currentRowIndex = startRowIndex;

    i64 batchDataWeight = 0;
    i64 readRowCount = 0;
    i64 readTimestampCount = 0;
    int timestampCount = 0;
    int batchRowCount = 0;
    int discardedByProgress = 0;
    auto readAllRows = true;

    auto prevTimestamp = MinTimestamp;
    auto maxTimestamp = NullTimestamp;

    auto columnFilter = CreateColumnFilter();

    std::vector<TUnversionedRow> readerRows;

    if (maxDataWeight > TableMountConfig_->MaxDataWeightPerReplicationCommit) {
        maxDataWeight = TableMountConfig_->MaxDataWeightPerReplicationCommit;
    }

    while (true) {
        if (MemoryUsageTracker_->TryReserve(maxDataWeight).IsOK()) {
            break;
        }

        if (maxDataWeight /= 2; maxDataWeight <= MinBatchWeight) {
            THROW_ERROR_EXCEPTION("Failed to reserve memory for pull rows request");
        }
    }

    while (readAllRows) {
        i64 readAmount = 2 * TableMountConfig_->MaxRowsPerReplicationCommit;
        auto batchFetcher = MakeBatchFetcher(
            MakeBoundKey(currentRowIndex),
            MakeBoundKey(currentRowIndex + readAmount),
            columnFilter);

        bool needCheckNextRange = false;

        struct TReplicationBatchTag
        { };

        auto rowBuffer = New<TRowBuffer>(
            TReplicationBatchTag(),
            TChunkedMemoryPool::DefaultStartChunkSize,
            MemoryUsageTracker_);

        while (readAllRows) {
            auto batch = batchFetcher->ReadNextRowBatch(currentRowIndex);
            if (!batch) {
                YT_TLOG_DEBUG("Received empty batch from tablet reader")
                    .With("TabletId", TabletId_)
                    .With("StartRowIndex", currentRowIndex);
                break;
            }

            needCheckNextRange = true;

            auto range = batch->MaterializeRows();
            readerRows.assign(range.begin(), range.end());

            bool isRequestDeadlineExceeded = TInstant::Now() >= requestDeadLine;
            bool isDataWeightPerPullRowsLimitExceeded = readDataWeightLimit <= 0;

            for (auto replicationLogRow : readerRows) {
                TTypeErasedRow replicationRow;
                TTimestamp timestamp;
                i64 rowDataWeight = 0;

                ToTypeErasedRow(
                    replicationLogRow,
                    rowBuffer,
                    &replicationRow,
                    &timestamp,
                    &rowDataWeight);

                readDataWeightLimit -= rowDataWeight;

                bool isRowFitIntoProgress = IsRowFitIntoProgress(replicationRow, timestamp);

                if (!isRowFitIntoProgress) {
                    ++discardedByProgress;
                }

                if (timestamp != prevTimestamp) {
                    // TODO(savrus): Throttle pulled data.

                    // Upper timestamp should be some era start ts, so no tx should have it as a commit ts.
                    YT_VERIFY(upperTimestamp == NullTimestamp || timestamp != upperTimestamp);

                    if (upperTimestamp != NullTimestamp && timestamp > upperTimestamp) {
                        maxTimestamp = std::max(maxTimestamp, upperTimestamp);
                        readAllRows = false;

                        YT_TLOG_DEBUG("Stopped reading replication batch because upper timestamp has been reached")
                            .With("TabletId", TabletId_)
                            .With("Timestamp", timestamp)
                            .With("UpperTimestamp", upperTimestamp)
                            .With("LastTimestamp", maxTimestamp);
                        break;
                    }

                    bool maxAllowedCommitInstantExceeded =
                        TimestampToInstant(timestamp).first > maxAllowedCommitInstant;
                    if (batchRowCount >= TableMountConfig_->MaxRowsPerReplicationCommit ||
                        batchDataWeight >= maxDataWeight ||
                        timestampCount >= TableMountConfig_->MaxTimestampsPerReplicationCommit ||
                        isRequestDeadlineExceeded ||
                        isDataWeightPerPullRowsLimitExceeded ||
                        (maxAllowedCommitInstantExceeded && readTimestampCount > 0))
                    {
                        readAllRows = false;
                        YT_TLOG_DEBUG("Stopped reading replication batch because stopping conditions are met")
                            .With("TabletId", TabletId_)
                            .With("Timestamp", timestamp)
                            .With("ReadRowCountOverflow", batchRowCount >= TableMountConfig_->MaxRowsPerReplicationCommit)
                            .With("ReadDataWeightOverflow", batchDataWeight >= maxDataWeight)
                            .With("TimestampCountOverflow", timestampCount >= TableMountConfig_->MaxTimestampsPerReplicationCommit)
                            .With("RequestDeadlineExceeded", isRequestDeadlineExceeded)
                            .With("DataWeightLimitPerPullRowsIteration", isDataWeightPerPullRowsLimitExceeded)
                            .With("MaxAllowedCommitInstantExceeded", maxAllowedCommitInstantExceeded);
                        break;
                    }

                    if (isRowFitIntoProgress) {
                        ++timestampCount;
                    }

                    ++readTimestampCount;
                }

                if (isRowFitIntoProgress) {
                    auto writtenSize = WriteTypeErasedRow(replicationRow);
                    MemoryUsageTracker_->Acquire(writtenSize);
                    batchRowCount += 1;
                    batchDataWeight += rowDataWeight;
                }

                rowBuffer->Clear();
                maxTimestamp = std::max(maxTimestamp, timestamp);
                prevTimestamp = timestamp;
                ++currentRowIndex;
            }

            readRowCount += readerRows.size();
        }

        if (!needCheckNextRange) {
            break;
        }
    }

    YT_TLOG_DEBUG("Read replication batch")
        .With("TabletId", TabletId_)
        .With("StartRowIndex", startRowIndex)
        .With("EndRowIndex", currentRowIndex)
        .With("ReadRowCount", readRowCount)
        .With("ResponseRowCount", batchRowCount)
        .With("ResponseDataWeight", batchDataWeight)
        .With("RowsDiscardedByProgress", discardedByProgress)
        .With("TimestampCount", timestampCount);

    return TReplicationLogBatchDescriptor{
        .ReadRowCount = readRowCount,
        .ResponseRowCount = batchRowCount,
        .ResponseDataWeight = batchDataWeight,
        .MaxTimestamp = maxTimestamp,
        .ReadAllRows = readAllRows,
        .EndReplicationRowIndex = currentRowIndex,
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
