#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/api/native/tablet_request_batcher.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_row.h>

namespace NYT::NApi::NNative {
namespace {

using namespace NCompression;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////


class TTabletRequestBatcherTest
    : public ::testing::Test
{
protected:
    TTableSchemaPtr Schema_;
    ITabletRequestBatcherPtr Batcher_;
    TLockMask PrimaryExclusiveMask_;

    void SetUp() override
    {
        PrimaryExclusiveMask_.Set(PrimaryLockIndex, ELockType::Exclusive);
    }

    void CreateBatcher(bool sorted, TTabletRequestBatcherOptions options = {})
    {
        auto sortOrder = sorted ? std::make_optional(ESortOrder::Ascending) : std::nullopt;
        std::vector<TColumnSchema> columns({
            TColumnSchema(/*name*/ "k", /*type*/ EValueType::Int64, sortOrder),
            TColumnSchema(/*name*/ "v", /*type*/ EValueType::Int64)
        });
        Schema_ = New<TTableSchema>(std::move(columns));
        auto columnEvaluator = TColumnEvaluator::Create(Schema_, /*typeInferrers*/ nullptr, /*profilers*/ nullptr);

        Batcher_ = CreateTabletRequestBatcher(std::move(options), Schema_, std::move(columnEvaluator));
    }

    TUnversionedOwningRow MakeRow(int key, std::optional<int> value = std::nullopt)
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(key, /*id*/ 0));
        if (value) {
            builder.AddValue(MakeUnversionedInt64Value(*value, /*id*/ 1));
        }

        return builder.FinishRow();
    }

    std::unique_ptr<IWireProtocolReader> GetSingularBatchReader()
    {
        auto batches = Batcher_->PrepareBatches();
        EXPECT_EQ(1, std::ssize(batches));

        auto* batch = batches[0].get();
        batch->Materialize(ECodec::None);

        return CreateWireProtocolReader(batch->RequestData);
    }

    TLockMask GetPrimaryLockExclusiveMask() const
    {
        return PrimaryExclusiveMask_;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTabletRequestBatcherTest, SimpleSorted)
{
    CreateBatcher(/*sorted*/ true);

    auto row1 = MakeRow(/*key*/ 1, /*value*/ 2);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteAndLockRow, row1, GetPrimaryLockExclusiveMask());
    auto row2 = MakeRow(/*key*/ 2);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::DeleteRow, row2, TLockMask());
    auto row3 = MakeRow(/*key*/ 3, /*value*/ 42);
    TLockMask mask3;
    mask3.Set(PrimaryLockIndex, ELockType::Exclusive);
    mask3.Set(1, ELockType::SharedWeak);
    mask3.Set(2, ELockType::SharedStrong);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteAndLockRow, row3, mask3);

    auto reader = GetSingularBatchReader();

    EXPECT_EQ(EWireProtocolCommand::WriteAndLockRow, reader->ReadCommand());
    EXPECT_EQ(row1, reader->ReadUnversionedRow(/*captureValues*/ true));
    TLockMask expectedMask1;
    expectedMask1.Set(PrimaryLockIndex, ELockType::Exclusive);
    EXPECT_EQ(expectedMask1, reader->ReadLockMask());

    EXPECT_EQ(EWireProtocolCommand::DeleteRow, reader->ReadCommand());
    EXPECT_EQ(row2, reader->ReadUnversionedRow(/*captureValues*/ true));

    EXPECT_EQ(EWireProtocolCommand::WriteAndLockRow, reader->ReadCommand());
    EXPECT_EQ(row3, reader->ReadUnversionedRow(/*captureValues*/ true));
    EXPECT_EQ(mask3, reader->ReadLockMask());

    EXPECT_TRUE(reader->IsFinished());
}

TEST_F(TTabletRequestBatcherTest, SimpleOrdered)
{
    CreateBatcher(/*sorted*/ false);

    auto row1 = MakeRow(/*key*/ 1, /*value*/ 1);
    auto row2 = MakeRow(/*key*/ 1, /*value*/ 2);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteRow, row1, TLockMask());
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteRow, row2, TLockMask());

    auto reader = GetSingularBatchReader();
    EXPECT_EQ(EWireProtocolCommand::WriteRow, reader->ReadCommand());
    EXPECT_EQ(row1, reader->ReadUnversionedRow(/*captureValues*/ true));
    EXPECT_EQ(EWireProtocolCommand::WriteRow, reader->ReadCommand());
    EXPECT_EQ(row2, reader->ReadUnversionedRow(/*captureValues*/ true));
    EXPECT_TRUE(reader->IsFinished());
}

TEST_F(TTabletRequestBatcherTest, SimpleVersionedSorted)
{
    CreateBatcher(/*sorted*/ true);

    auto rowBuffer = New<TRowBuffer>();
    TVersionedRowBuilder builder(rowBuffer);
    builder.AddKey(MakeUnversionedInt64Value(1, /*id*/ 0));
    builder.AddValue(MakeVersionedInt64Value(1, /*timestamp*/ 0x42, /*id*/ 0));
    auto row = builder.FinishRow();

    Batcher_->SubmitVersionedRow(row.ToTypeErasedRow());

    auto reader = GetSingularBatchReader();
    EXPECT_EQ(EWireProtocolCommand::VersionedWriteRow, reader->ReadCommand());
    auto schemaData = IWireProtocolReader::GetSchemaData(*Schema_);
    EXPECT_EQ(ToString(row), ToString(reader->ReadVersionedRow(schemaData, /*captureValues*/ true)));
    EXPECT_TRUE(reader->IsFinished());
}

TEST_F(TTabletRequestBatcherTest, SimpleVersionedOrdered)
{
    CreateBatcher(/*sorted*/ false);

    auto row = MakeRow(/*key*/ 1, /*value*/ 1);
    Batcher_->SubmitVersionedRow(TUnversionedRow(row).ToTypeErasedRow());

    auto reader = GetSingularBatchReader();
    EXPECT_EQ(EWireProtocolCommand::VersionedWriteRow, reader->ReadCommand());
    EXPECT_EQ(row, reader->ReadUnversionedRow(/*captureValues*/ true));
    EXPECT_TRUE(reader->IsFinished());
}

TEST_F(TTabletRequestBatcherTest, UnversionedVersionedIntermix)
{
    CreateBatcher(/*sorted*/ false);

    auto row = MakeRow(/*key*/ 1, /*value*/ 1);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteRow, row, TLockMask());
    Batcher_->SubmitVersionedRow(TUnversionedRow(row).ToTypeErasedRow());

    EXPECT_THROW_WITH_SUBSTRING(Batcher_->PrepareBatches(), "Cannot intermix versioned and unversioned writes");
}

TEST_F(TTabletRequestBatcherTest, MaxRowsPerBatch)
{
    CreateBatcher(/*sorted*/ true, TTabletRequestBatcherOptions{.MaxRowsPerBatch = 3});

    std::vector<TUnversionedOwningRow> rows;
    for (int index = 0; index < 10; ++index) {
        rows.push_back(MakeRow(/*key*/ index, /*value*/ index));
        Batcher_->SubmitUnversionedRow(
            EWireProtocolCommand::WriteAndLockRow, rows.back(), GetPrimaryLockExclusiveMask());
    }

    auto batches = Batcher_->PrepareBatches();
    EXPECT_EQ(4, std::ssize(batches));

    int rowIndex = 0;
    for (int batchIndex = 0; batchIndex < 4; ++batchIndex) {
        auto* batch = batches[batchIndex].get();
        batch->Materialize(ECodec::None);

        auto reader = CreateWireProtocolReader(batch->RequestData);
        auto rowCount = batchIndex == 3 ? 1 : 3;
        for (int batchRowIndex = 0; batchRowIndex < rowCount; ++batchRowIndex) {
            EXPECT_EQ(EWireProtocolCommand::WriteAndLockRow, reader->ReadCommand());
            EXPECT_EQ(rows[rowIndex++], reader->ReadUnversionedRow(/*captureValues*/ true));
            EXPECT_EQ(GetPrimaryLockExclusiveMask(), reader->ReadLockMask());
        }
        EXPECT_TRUE(reader->IsFinished());
    }
}

TEST_F(TTabletRequestBatcherTest, MaxDataWeightPerBatch)
{
    CreateBatcher(/*sorted*/ true, TTabletRequestBatcherOptions{.MaxDataWeightPerBatch = 51});

    std::vector<TUnversionedOwningRow> rows;
    for (int index = 0; index < 10; ++index) {
        rows.push_back(MakeRow(/*key*/ index, /*value*/ index));
        Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteAndLockRow, rows.back(), GetPrimaryLockExclusiveMask());
    }

    auto batches = Batcher_->PrepareBatches();
    EXPECT_EQ(4, std::ssize(batches));

    int rowIndex = 0;
    for (int batchIndex = 0; batchIndex < 4; ++batchIndex) {
        auto* batch = batches[batchIndex].get();
        batch->Materialize(ECodec::None);

        auto reader = CreateWireProtocolReader(batch->RequestData);
        auto rowCount = batchIndex == 3 ? 1 : 3;
        for (int batchRowIndex = 0; batchRowIndex < rowCount; ++batchRowIndex) {
            EXPECT_EQ(EWireProtocolCommand::WriteAndLockRow, reader->ReadCommand());
            EXPECT_EQ(rows[rowIndex++], reader->ReadUnversionedRow(/*captureValues*/ true));
            EXPECT_EQ(GetPrimaryLockExclusiveMask(), reader->ReadLockMask());
        }
        EXPECT_TRUE(reader->IsFinished());
    }
}

TEST_F(TTabletRequestBatcherTest, MaxRowsPerTablet)
{
    CreateBatcher(/*sorted*/ false, TTabletRequestBatcherOptions{.MaxRowsPerTablet = 1});

    auto row = MakeRow(/*key*/ 1, /*value*/ 1);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteRow, row, TLockMask());
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteRow, row, TLockMask());

    EXPECT_THROW_WITH_SUBSTRING(Batcher_->PrepareBatches(), "Transaction affects too many rows in tablet");
}

TEST_F(TTabletRequestBatcherTest, RowMerger1)
{
    CreateBatcher(/*sorted*/ true);

    auto row1 = MakeRow(/*key*/ 1, /*value*/ 1);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteAndLockRow, row1, GetPrimaryLockExclusiveMask());
    auto row2 = MakeRow(/*key*/ 1);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::DeleteRow, row2, TLockMask());

    auto reader = GetSingularBatchReader();
    EXPECT_EQ(EWireProtocolCommand::DeleteRow, reader->ReadCommand());
    EXPECT_EQ(row2, reader->ReadUnversionedRow(/*captureValues*/ true));
    EXPECT_TRUE(reader->IsFinished());
}

TEST_F(TTabletRequestBatcherTest, RowMerger2)
{
    CreateBatcher(/*sorted*/ true);

    auto row1 = MakeRow(/*key*/ 1);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::DeleteRow, row1, TLockMask());
    auto row2 = MakeRow(/*key*/ 1, /*value*/ 1);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteAndLockRow, row2, GetPrimaryLockExclusiveMask());

    auto reader = GetSingularBatchReader();
    EXPECT_EQ(EWireProtocolCommand::WriteAndLockRow, reader->ReadCommand());
    EXPECT_EQ(row2, reader->ReadUnversionedRow(/*captureValues*/ true));
    EXPECT_EQ(GetPrimaryLockExclusiveMask(), reader->ReadLockMask());
    EXPECT_TRUE(reader->IsFinished());
}

TEST_F(TTabletRequestBatcherTest, RowMerger3)
{
    CreateBatcher(/*sorted*/ true);

    auto row1 = MakeRow(/*key*/ 1, /*value*/ 1);
    TLockMask mask1;
    mask1.Set(0, ELockType::Exclusive);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteAndLockRow, row1, mask1);
    TLockMask mask2;
    mask2.Set(0, ELockType::SharedWeak);
    mask2.Set(1, ELockType::SharedWeak);
    auto row2 = MakeRow(/*key*/ 1, /*value*/ 2);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteAndLockRow, row2, mask2);

    auto reader = GetSingularBatchReader();
    EXPECT_EQ(EWireProtocolCommand::WriteAndLockRow, reader->ReadCommand());
    TLockMask mask;
    mask.Set(0, ELockType::Exclusive);
    mask.Set(1, ELockType::SharedWeak);
    EXPECT_EQ(row2, reader->ReadUnversionedRow(/*captureValues*/ true));
    EXPECT_EQ(mask, reader->ReadLockMask());
    EXPECT_TRUE(reader->IsFinished());
}

TEST_F(TTabletRequestBatcherTest, LockAfterDelete)
{
    CreateBatcher(/*sorted*/ true);

    auto row = MakeRow(/*key*/ 1);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::DeleteRow, row, TLockMask());
    TLockMask mask;
    mask.Set(1, ELockType::SharedWeak);
    Batcher_->SubmitUnversionedRow(EWireProtocolCommand::WriteAndLockRow, row, mask);

    auto reader = GetSingularBatchReader();
    EXPECT_EQ(EWireProtocolCommand::DeleteRow, reader->ReadCommand());
    EXPECT_EQ(row, reader->ReadUnversionedRow(/*captureValues*/ true));
    EXPECT_TRUE(reader->IsFinished());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NApi::NNative
