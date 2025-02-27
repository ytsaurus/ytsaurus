#include "replication_log_batch_reader.h"

#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/server/lib/tablet_node/config.h>

namespace NYT::NQueryAgent {

using namespace NTabletNode;
using namespace NTableClient;
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
    TInstant requestDeadLine)
{
    auto currentRowIndex = startRowIndex;

    i64 batchDataWeight = 0;
    i64 readRowCount = 0;
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
                YT_LOG_DEBUG("Received empty batch from tablet reader (TabletId: %v, StartRowIndex: %v)",
                    TabletId_,
                    currentRowIndex);
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

                if (!ToTypeErasedRow(replicationLogRow, rowBuffer, &replicationRow, &timestamp, &rowDataWeight)) {
                    ++discardedByProgress;
                    readDataWeightLimit -= rowDataWeight;
                    if (!isRequestDeadlineExceeded && !isDataWeightPerPullRowsLimitExceeded) {
                        ++currentRowIndex;
                        continue;
                    }
                }

                if (timestamp != prevTimestamp) {
                    // TODO(savrus): Throttle pulled data.

                    // Upper timestamp should be some era start ts, so no tx should have it as a commit ts.
                    YT_VERIFY(upperTimestamp == NullTimestamp || timestamp != upperTimestamp);

                    if (upperTimestamp != NullTimestamp && timestamp > upperTimestamp) {
                        maxTimestamp = std::max(maxTimestamp, upperTimestamp);
                        readAllRows = false;

                        YT_LOG_DEBUG("Stopped reading replication batch because upper timestamp has been reached "
                            "(TabletId: %v, Timestamp: %v, UpperTimestamp: %v, LastTimestamp: %v)",
                            TabletId_,
                            timestamp,
                            upperTimestamp,
                            maxTimestamp);
                        break;
                    }

                    if (batchRowCount >= TableMountConfig_->MaxRowsPerReplicationCommit ||
                        batchDataWeight >= maxDataWeight ||
                        timestampCount >= TableMountConfig_->MaxTimestampsPerReplicationCommit ||
                        isRequestDeadlineExceeded ||
                        isDataWeightPerPullRowsLimitExceeded)
                    {
                        readAllRows = false;
                        YT_LOG_DEBUG("Stopped reading replication batch because stopping conditions are met "
                            "(TabletId: %v, Timestamp: %v, ReadRowCountOverflow: %v, ReadDataWeightOverflow: %v, "
                            "TimestampCountOverflow: %v, RequestDeadlineExceeded: %v, "
                            "DataWeightLimitPerPullRowsIteration: %v)",
                            TabletId_,
                            timestamp,
                            batchRowCount >= TableMountConfig_->MaxRowsPerReplicationCommit,
                            batchDataWeight >= maxDataWeight,
                            timestampCount >= TableMountConfig_->MaxTimestampsPerReplicationCommit,
                            isRequestDeadlineExceeded,
                            isDataWeightPerPullRowsLimitExceeded);
                        break;
                    }

                    ++timestampCount;
                }

                MemoryUsageTracker_->Acquire(WriteTypeErasedRow(replicationRow));
                rowBuffer->Clear();

                maxTimestamp = std::max(maxTimestamp, timestamp);
                prevTimestamp = timestamp;

                batchRowCount += 1;
                batchDataWeight += rowDataWeight;

                ++currentRowIndex;
            }

            readRowCount += readerRows.size();
        }

        if (!needCheckNextRange) {
            break;
        }
    }

    YT_LOG_DEBUG("Read replication batch (TabletId: %v, StartRowIndex: %v, EndRowIndex: %v, ReadRowCount: %v, "
        "ResponseRowCount: %v, ResponseDataWeight: %v, RowsDiscardedByProgress: %v, TimestampCount: %v)",
        TabletId_,
        startRowIndex,
        currentRowIndex,
        readRowCount,
        batchRowCount,
        batchDataWeight,
        discardedByProgress,
        timestampCount);

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
