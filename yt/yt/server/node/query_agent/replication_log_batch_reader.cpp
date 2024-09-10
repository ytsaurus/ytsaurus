#include "replication_log_batch_reader.h"

#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/server/lib/tablet_node/config.h>

namespace NYT::NQueryAgent {

using namespace NTabletNode;
using namespace NTableClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TReplicationLogBatchReaderBase::TReplicationLogBatchReaderBase(
    TTableMountConfigPtr mountConfig,
    TTabletId tabletId,
    TLogger logger)
    : TableMountConfig_(std::move(mountConfig))
    , TabletId_(std::move(tabletId))
    , Logger(std::move(logger))
{ }

TColumnFilter TReplicationLogBatchReaderBase::CreateColumnFilter() const
{
    return TColumnFilter();
}

void TReplicationLogBatchReaderBase::ReadReplicationBatch(
    i64* currentRowIndex,
    TTimestamp upperTimestamp,
    i64 maxDataWeight,
    i64* totalRowCount,
    i64* batchRowCount,
    i64* batchDataWeight,
    TTimestamp* maxTimestamp,
    bool* readAllRows)
{
    auto prevTimestamp = MinTimestamp;
    int timestampCount = 0;
    int discardedByProgress = 0;
    auto startRowIndex = *currentRowIndex;
    auto columnFilter = CreateColumnFilter();
    *readAllRows = true;
    std::vector<TUnversionedRow> readerRows;

    if (maxDataWeight > TableMountConfig_->MaxDataWeightPerReplicationCommit) {
        maxDataWeight = TableMountConfig_->MaxDataWeightPerReplicationCommit;
    }

    while (*readAllRows) {
        i64 readAmount = 2 * TableMountConfig_->MaxRowsPerReplicationCommit;
        auto batchFetcher = MakeBatchFetcher(
            MakeBoundKey(*currentRowIndex),
            MakeBoundKey(*currentRowIndex + readAmount),
            columnFilter);

        bool needCheckNextRange = false;

        while (*readAllRows) {
            auto batch = batchFetcher->ReadNextRowBatch(*currentRowIndex);
            if (!batch) {
                YT_LOG_DEBUG("Received empty batch from tablet reader (TabletId: %v, StartRowIndex: %v)",
                    TabletId_,
                    *currentRowIndex);
                break;
            }
            needCheckNextRange = true;

            auto range = batch->MaterializeRows();
            readerRows.assign(range.begin(), range.end());

            for (auto replicationLogRow : readerRows) {
                TTypeErasedRow replicationRow;
                TTimestamp timestamp;
                i64 rowDataWeight;

                if (!ToTypeErasedRow(replicationLogRow, &replicationRow, &timestamp, &rowDataWeight)) {
                    ++*currentRowIndex;
                    ++discardedByProgress;
                    continue;
                }

                if (timestamp != prevTimestamp) {
                    // TODO(savrus): Throttle pulled data.

                    // Upper timestamp should be some era start ts, so no tx should have it as a commit ts.
                    YT_VERIFY(upperTimestamp == NullTimestamp || timestamp != upperTimestamp);

                    if (upperTimestamp != NullTimestamp && timestamp > upperTimestamp) {
                        *maxTimestamp = std::max(*maxTimestamp, upperTimestamp);
                        *readAllRows = false;

                        YT_LOG_DEBUG("Stopped reading replication batch because upper timestamp has been reached "
                            "(TabletId: %v, Timestamp: %v, UpperTimestamp: %v, LastTimestamp: %v)",
                            TabletId_,
                            timestamp,
                            upperTimestamp,
                            *maxTimestamp);
                        break;
                    }

                    if (*batchRowCount >= TableMountConfig_->MaxRowsPerReplicationCommit ||
                        *batchDataWeight >= maxDataWeight ||
                        timestampCount >= TableMountConfig_->MaxTimestampsPerReplicationCommit)
                    {
                        *readAllRows = false;
                        YT_LOG_DEBUG("Stopped reading replication batch because stopping conditions are met "
                            "(TabletId: %v, Timestamp: %v, ReadRowCountOverflow: %v, ReadDataWeightOverflow: %v, TimestampCountOverflow: %v",
                            TabletId_,
                            timestamp,
                            *batchRowCount >= TableMountConfig_->MaxRowsPerReplicationCommit,
                            *batchDataWeight >= maxDataWeight,
                            timestampCount >= TableMountConfig_->MaxTimestampsPerReplicationCommit);
                        break;
                    }

                    ++timestampCount;
                }

                WriteTypeErasedRow(replicationRow);

                *maxTimestamp = std::max(*maxTimestamp, timestamp);
                prevTimestamp = timestamp;

                *batchRowCount += 1;
                *batchDataWeight += rowDataWeight;

                ++*currentRowIndex;
            }

            *totalRowCount += readerRows.size();
        }

        if (!needCheckNextRange) {
            break;
        }
    }

    YT_LOG_DEBUG("Read replication batch (TabletId: %v, StartRowIndex: %v, EndRowIndex: %v, ReadRowCount: %v, "
        "ResponseRowCount: %v, ResponseDataWeight: %v, RowsDiscardedByProgress: %v, TimestampCount: %v)",
        TabletId_,
        startRowIndex,
        *currentRowIndex,
        *totalRowCount,
        *batchRowCount,
        *batchDataWeight,
        discardedByProgress,
        timestampCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
