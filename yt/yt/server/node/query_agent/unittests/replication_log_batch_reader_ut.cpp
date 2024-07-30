#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/node/query_agent/replication_log_batch_reader.h>
#include <yt/yt/server/node/tablet_node/replication_log.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/server/lib/tablet_node/config.h>

namespace NYT::NTabletNode {
namespace {

using namespace NYT::NQueryAgent;
using namespace NTableClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

struct TFakeRow
{
    TTimestamp Timestamp;
    int Weight;

    TFakeRow(TTimestamp timestamp, int weight)
        : Timestamp(timestamp)
        , Weight(weight)
    { }
};

class TFakeBatchFetcher
    : public IReplicationLogBatchFetcher
{
public:
    TFakeBatchFetcher(std::vector<TUnversionedRow> batch, int batchSize = 10)
        : Batch_(std::move(batch))
        , BatchSize_(batchSize)
        , Position_(0)
    { }

    IUnversionedRowBatchPtr ReadNextRowBatch(i64 /*currentRowIndex*/) override
    {
        int amount = std::min<int>(Batch_.size() - Position_, BatchSize_);
        if (amount == 0) {
            return nullptr;
        }
        std::vector<TUnversionedRow> rows(Batch_.begin() + Position_, Batch_.begin() + Position_ + amount);
        Position_ += amount;
        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows)));
    }

private:
    const std::vector<TUnversionedRow> Batch_;
    const int BatchSize_;
    int Position_;
};


class TFakeReplicationLogBatchReader
    : public TReplicationLogBatchReaderBase
{
public:
    TFakeReplicationLogBatchReader(
        TTableMountConfigPtr mountConfig,
        TTabletId tabletId,
        TLogger logger,
        const std::vector<TFakeRow>& replicationLogFakeRows)
        : TReplicationLogBatchReaderBase(
            std::move(mountConfig),
            std::move(tabletId),
            std::move(logger))
        , ReplicationLogRows_(BuildReplicationLogRows(replicationLogFakeRows))
    { }

    const std::vector<int>& GetProcessedRows() const
    {
        return ProcessedRows_;
    }

    int GetReadsCount() const
    {
        return ReadsCount_;
    }

protected:
    TLegacyOwningKey MakeBoundKey(i64 currentRowIndex) const override
    {
        return MakeRowBound(currentRowIndex);
    }

    std::unique_ptr<IReplicationLogBatchFetcher> MakeBatchFetcher(
        NTableClient::TLegacyOwningKey lower,
        NTableClient::TLegacyOwningKey upper,
        const NTableClient::TColumnFilter& /*columnFilter*/) const override

    {
        ++ReadsCount_;

        auto lowerRowIndex = std::max<i64>(GetLogRowIndex(lower), 0);
        auto upperRowIndex = std::min<i64>(std::max<i64>(GetLogRowIndex(upper), 0), ReplicationLogRows_.size());

        if (lowerRowIndex >= i64(ReplicationLogRows_.size())) {
            return std::make_unique<TFakeBatchFetcher>(std::vector<TUnversionedRow>());
        }

        std::vector<TUnversionedRow> rows;
        rows.reserve(upperRowIndex - lowerRowIndex);
        for (int index = lowerRowIndex; index < upperRowIndex; ++index) {
            rows.push_back(ReplicationLogRows_[index].Get());
        }

        return std::make_unique<TFakeBatchFetcher>(std::move(rows));
    }

    bool ToTypeErasedRow(
        const TUnversionedRow& row,
        const TRowBufferPtr& /*rowBuffer*/,
        TTypeErasedRow* replicationRow,
        TTimestamp* timestamp,
        i64* rowDataWeight) const override
    {
        *replicationRow = row.ToTypeErasedRow();
        *timestamp = row[0].Data.Uint64;
        *rowDataWeight = row[1].Data.Int64;
        return true;
    }

    void WriteTypeErasedRow(TTypeErasedRow row) override
    {
        ProcessedRows_.push_back(TUnversionedRow(std::move(row))[2].Data.Int64);
    }

private:
    const std::vector<TUnversionedOwningRow> ReplicationLogRows_;
    std::vector<int> ProcessedRows_;
    mutable int ReadsCount_ = 0;

    static std::vector<TUnversionedOwningRow> BuildReplicationLogRows(
        const std::vector<TFakeRow>& replicationLogFakeRows)
    {
        std::vector<TUnversionedOwningRow> replicationLogRows;
        replicationLogRows.reserve(replicationLogRows.size());
        int i = 0;
        for (const auto& t : replicationLogFakeRows) {
            replicationLogRows.push_back(MakeUnversionedOwningRow(t.Timestamp, t.Weight, i));
            ++i;
        }
        return replicationLogRows;
    }
};

////////////////////////////////////////////////////////////////////////////////

void AppendReplicationLogRows(
    TTimestamp timestamp,
    int rowWeight,
    int rowsCount,
    std::vector<TFakeRow>* replicationLogRows)
{
    for (; rowsCount > 0; --rowsCount) {
        replicationLogRows->emplace_back(timestamp, rowWeight);
    }
}

////////////////////////////////////////////////////////////////////////////////

void CheckReaderContinious(const TFakeReplicationLogBatchReader& reader, int expectedRowsCount)
{
    const auto& rowIds = reader.GetProcessedRows();
    EXPECT_EQ(int(rowIds.size()), expectedRowsCount);
    for (int index = 0; index < int(rowIds.size()); ++index) {
        EXPECT_EQ(rowIds[index], index);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TReplicationLogBatchReaderTest, TestReadEmpty)
{
    TTableMountConfigPtr mountConfig = New<TTableMountConfig>();
    mountConfig->MaxRowsPerReplicationCommit = 1000;
    mountConfig->MaxDataWeightPerReplicationCommit = 1000;
    mountConfig->MaxTimestampsPerReplicationCommit = 1000;

    TLogger logger;
    std::vector<TFakeRow> transactions;

    TFakeReplicationLogBatchReader reader(mountConfig, TTabletId::Create(), logger, transactions);

    auto result = reader.ReadReplicationBatch(
        0,
        NullTimestamp,
        /*maxDataWeight*/ 1_GB);

    EXPECT_TRUE(result.ReadAllRows);
    EXPECT_TRUE(reader.GetProcessedRows().empty());
    EXPECT_EQ(result.ReadRowCount, 0ll);
    EXPECT_EQ(result.ResponseRowCount, 0ll);
    EXPECT_EQ(result.ResponseDataWeight, 0ll);
    EXPECT_EQ(result.MaxTimestamp, 0ull);
    EXPECT_EQ(reader.GetReadsCount(), 1);
}

TEST(TReplicationLogBatchReaderTest, TestReadAll)
{
    TTableMountConfigPtr mountConfig = New<TTableMountConfig>();
    mountConfig->MaxRowsPerReplicationCommit = 1000;
    mountConfig->MaxDataWeightPerReplicationCommit = 1000;
    mountConfig->MaxTimestampsPerReplicationCommit = 1000;

    TLogger logger;
    std::vector<TFakeRow> replicationLogRows;
    AppendReplicationLogRows(1, 10, 10, &replicationLogRows);
    AppendReplicationLogRows(2, 10, 10, &replicationLogRows);
    AppendReplicationLogRows(3, 10, 10, &replicationLogRows);
    TFakeReplicationLogBatchReader reader(mountConfig, TTabletId::Create(), logger, replicationLogRows);

    auto result = reader.ReadReplicationBatch(
        0,
        NullTimestamp,
        /*maxDataWeight*/ 1_GB);

    EXPECT_TRUE(result.ReadAllRows);
    EXPECT_EQ(result.ReadRowCount, 30ll);
    EXPECT_EQ(result.ResponseRowCount, 30ll);
    EXPECT_EQ(result.ResponseDataWeight, 300ll);
    EXPECT_EQ(result.MaxTimestamp, 3ull);
    EXPECT_EQ(reader.GetReadsCount(), 2);
    CheckReaderContinious(reader, 30);
}

TEST(TReplicationLogBatchReaderTest, TestReadUntilLimits)
{
    TTableMountConfigPtr mountConfig = New<TTableMountConfig>();
    mountConfig->MaxRowsPerReplicationCommit = 12;
    mountConfig->MaxDataWeightPerReplicationCommit = 1000;
    mountConfig->MaxTimestampsPerReplicationCommit = 1000;

    TLogger logger;
    std::vector<TFakeRow> transactions;
    AppendReplicationLogRows(1, 10, 10, &transactions);
    AppendReplicationLogRows(2, 10, 10, &transactions);
    AppendReplicationLogRows(3, 10, 10, &transactions);
    TFakeReplicationLogBatchReader reader(mountConfig, TTabletId::Create(), logger, transactions);

    auto result = reader.ReadReplicationBatch(
        0,
        NullTimestamp,
        /*maxDataWeight*/ 1_GB);

    EXPECT_FALSE(result.ReadAllRows);
    EXPECT_EQ(result.ReadRowCount, 24ll);
    EXPECT_EQ(result.ResponseRowCount, 20ll);
    EXPECT_EQ(result.ResponseDataWeight, 200ll);
    EXPECT_EQ(result.MaxTimestamp, 2ull);
    EXPECT_EQ(reader.GetReadsCount(), 1);
    EXPECT_EQ(result.EndReplicationRowIndex, 20);
    CheckReaderContinious(reader, 20);

    result = reader.ReadReplicationBatch(
        20,
        NullTimestamp,
        /*maxDataWeight*/ 1_GB);

    EXPECT_TRUE(result.ReadAllRows);
    EXPECT_EQ(result.ReadRowCount, 10ll);
    EXPECT_EQ(result.ResponseRowCount, 10ll);
    EXPECT_EQ(result.ResponseDataWeight, 100ll);
    EXPECT_EQ(result.MaxTimestamp, 3ull);
    // 1 from previous read, 1 till the end and 1 to get null batch
    EXPECT_EQ(reader.GetReadsCount(), 3);
    EXPECT_EQ(result.EndReplicationRowIndex, 30);
    CheckReaderContinious(reader, 30);
}

TEST(TReplicationLogBatchReaderTest, TestReadLargeTransactionBreakingLimits)
{
    TTableMountConfigPtr mountConfig = New<TTableMountConfig>();
    mountConfig->MaxRowsPerReplicationCommit = 12;
    mountConfig->MaxDataWeightPerReplicationCommit = 1000;
    mountConfig->MaxTimestampsPerReplicationCommit = 1000;

    TLogger logger;
    std::vector<TFakeRow> transactions;
    AppendReplicationLogRows(1, 10, 100, &transactions);
    AppendReplicationLogRows(2, 10, 10, &transactions);
    AppendReplicationLogRows(3, 10, 10, &transactions);
    TFakeReplicationLogBatchReader reader(mountConfig, TTabletId::Create(), logger, transactions);

    auto result = reader.ReadReplicationBatch(
        0,
        NullTimestamp,
        /*maxDataWeight*/ 1_GB);

    EXPECT_FALSE(result.ReadAllRows);
    EXPECT_EQ(result.ReadRowCount, 106ll);
    EXPECT_EQ(result.ResponseRowCount, 100ll);
    EXPECT_EQ(result.ResponseDataWeight, 1000ll);
    EXPECT_EQ(result.MaxTimestamp, 1ull);
    EXPECT_EQ(reader.GetReadsCount(), 5);
    CheckReaderContinious(reader, 100);
    EXPECT_EQ(result.EndReplicationRowIndex, 100);

    result = reader.ReadReplicationBatch(
        100,
        NullTimestamp,
        /*maxDataWeight*/ 1_GB);

    EXPECT_TRUE(result.ReadAllRows);
    EXPECT_EQ(result.ReadRowCount, 20ll);
    EXPECT_EQ(result.ResponseRowCount, 20ll);
    EXPECT_EQ(result.ResponseDataWeight, 200ll);
    EXPECT_EQ(result.MaxTimestamp, 3ull);
    // 5 from previous read, 1 till the end and 1 to get null batch
    EXPECT_EQ(reader.GetReadsCount(), 7);
    EXPECT_EQ(result.EndReplicationRowIndex, 120);
    CheckReaderContinious(reader, 120);
}

} // namespace
} // namespace NYT::NTabletNode
