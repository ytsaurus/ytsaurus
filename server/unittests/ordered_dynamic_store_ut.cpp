#include "ordered_dynamic_store_ut_helpers.h"

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicStoreTest
    : public TOrderedDynamicStoreTestBase
{
protected:
    virtual void SetUp() override
    {
        TOrderedDynamicStoreTestBase::SetUp();
        CreateDynamicStore();
    }


    TTimestamp WriteRow(const TUnversionedOwningRow& row)
    {
        TWriteContext context;
        context.Phase = EWritePhase::Commit;
        context.CommitTimestamp = GenerateTimestamp();
        EXPECT_NE(TOrderedDynamicRow(), Store_->WriteRow(row, &context));
        return context.CommitTimestamp;
    }

    std::vector<TUnversionedOwningRow> ReadRows(
        int tabletIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        const TColumnFilter& columnFilter)
    {
        auto reader = Store_->CreateReader(
            Tablet_->BuildSnapshot(nullptr),
            tabletIndex,
            lowerRowIndex,
            upperRowIndex,
            columnFilter,
            TWorkloadDescriptor(),
            TReadSessionId());

        std::vector<TUnversionedOwningRow> allRows;
        std::vector<TUnversionedRow> someRows;
        someRows.reserve(100);
        while (reader->Read(&someRows)) {
            YCHECK(!someRows.empty());
            for (auto row : someRows) {
                allRows.push_back(TUnversionedOwningRow(row));
            }
        }
        return allRows;
    }


    TString DumpStore()
    {
        TStringBuilder builder;
        builder.AppendFormat("RowCount=%v ValueCount=%v\n",
            Store_->GetRowCount(),
            Store_->GetValueCount());

        int schemaColumnCount = Tablet_->PhysicalSchema().GetColumnCount();
        for (auto row : Store_->GetAllRows()) {
            builder.AppendChar('[');
            for (int i = 0; i < schemaColumnCount; ++i) {
                builder.AppendFormat(" %v", row[i]);
            }
            builder.AppendString(" ]");
            builder.AppendChar('\n');
        }
        return builder.Flush();
    }


    TOrderedDynamicStorePtr Store_;

private:
    virtual void CreateDynamicStore() override
    {
        auto config = New<TTabletManagerConfig>();
        Store_ = New<TOrderedDynamicStore>(
            config,
            TTabletId(),
            Tablet_.get());
    }

    virtual IDynamicStorePtr GetDynamicStore() override
    {
        return Store_;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TOrderedDynamicStoreTest, Empty)
{
    EXPECT_EQ(0, Store_->GetRowCount());
    EXPECT_EQ(0, Store_->GetValueCount());
}

TEST_F(TOrderedDynamicStoreTest, Write)
{
    WriteRow(BuildRow("a=1"));
    EXPECT_EQ(1, Store_->GetRowCount());
    EXPECT_EQ(3, Store_->GetValueCount());
}

TEST_F(TOrderedDynamicStoreTest, SerializeEmpty)
{
    auto check = [&] () {
        EXPECT_EQ(0, Store_->GetRowCount());
        EXPECT_EQ(0, Store_->GetValueCount());
    };

    check();

    auto dump = DumpStore();
    ReserializeStore();
    EXPECT_EQ(dump, DumpStore());

    check();
}

TEST_F(TOrderedDynamicStoreTest, SerializeNonempty)
{
    WriteRow(BuildRow("a=1;b=3.14"));
    WriteRow(BuildRow("c=test"));

    auto check = [&] () {
        EXPECT_EQ(2, Store_->GetRowCount());
        EXPECT_EQ(6, Store_->GetValueCount());
    };

    check();

    auto dump = DumpStore();
    ReserializeStore();
    EXPECT_EQ(dump, DumpStore());

    check();
}

TEST_F(TOrderedDynamicStoreTest, Reader1)
{
    WriteRow(BuildRow("a=1;b=3.14"));
    WriteRow(BuildRow("a=2;c=text"));
    WriteRow(BuildRow("a=3;b=2.7"));

    auto rows = ReadRows(5, 0, 3, TColumnFilter());
    EXPECT_EQ(3, rows.size());
    EXPECT_TRUE(AreQueryRowsEqual(rows[0], "\"$tablet_index\"=5;\"$row_index\"=0;a=1;b=3.14"));
    EXPECT_TRUE(AreQueryRowsEqual(rows[1], "\"$tablet_index\"=5;\"$row_index\"=1;a=2;c=text"));
    EXPECT_TRUE(AreQueryRowsEqual(rows[2], "\"$tablet_index\"=5;\"$row_index\"=2;a=3;b=2.7"));
}

TEST_F(TOrderedDynamicStoreTest, Reader2)
{
    WriteRow(BuildRow("a=1;b=3.14"));

    auto rows = ReadRows(5, 1, 2, TColumnFilter());
    EXPECT_EQ(0, rows.size());
}

TEST_F(TOrderedDynamicStoreTest, Reader3)
{
    WriteRow(BuildRow("a=1;b=3.14"));
    WriteRow(BuildRow("a=2;c=text"));
    WriteRow(BuildRow("a=3;b=2.7"));

    auto rows = ReadRows(5, 0, 3, TColumnFilter({1,2}));
    EXPECT_EQ(3, rows.size());
    EXPECT_TRUE(AreQueryRowsEqual(rows[0], "\"$row_index\"=0;a=1"));
    EXPECT_TRUE(AreQueryRowsEqual(rows[1], "\"$row_index\"=1;a=2"));
    EXPECT_TRUE(AreQueryRowsEqual(rows[2], "\"$row_index\"=2;a=3"));
}

TEST_F(TOrderedDynamicStoreTest, Reader4)
{
    WriteRow(BuildRow("a=1;b=3.14"));
    WriteRow(BuildRow("a=2;c=text"));
    WriteRow(BuildRow("a=3;b=2.7"));

    Store_->SetStartingRowIndex(10);
    auto rows = ReadRows(5, 10, 13, TColumnFilter());
    EXPECT_EQ(3, rows.size());
    EXPECT_TRUE(AreQueryRowsEqual(rows[0], "\"$tablet_index\"=5;\"$row_index\"=10;a=1;b=3.14"));
    EXPECT_TRUE(AreQueryRowsEqual(rows[1], "\"$tablet_index\"=5;\"$row_index\"=11;a=2;c=text"));
    EXPECT_TRUE(AreQueryRowsEqual(rows[2], "\"$tablet_index\"=5;\"$row_index\"=12;a=3;b=2.7"));
}

TEST_F(TOrderedDynamicStoreTest, Reader5)
{
    WriteRow(BuildRow("a=1;b=3.14"));
    WriteRow(BuildRow("a=2;c=text"));
    WriteRow(BuildRow("a=3;b=2.7"));

    auto rows = ReadRows(5, 1, 3, TColumnFilter({1}));
    EXPECT_EQ(2, rows.size());
    EXPECT_TRUE(AreQueryRowsEqual(rows[0], "\"$row_index\"=1"));
    EXPECT_TRUE(AreQueryRowsEqual(rows[1], "\"$row_index\"=2"));
}

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicStoreReadTest
    : public TOrderedDynamicStoreTest
    , public ::testing::WithParamInterface<std::tuple<int, int, int>>
{ };

TEST_P(TOrderedDynamicStoreReadTest, Read)
{
    int count = std::get<0>(GetParam());
    for (int i = 0; i < count; ++i) {
        WriteRow(BuildRow(Format("a=%v", i)));
    }

    int lowerIndex = std::get<1>(GetParam());
    int adjustedLowerIndex = std::min(std::max(0, lowerIndex), count);
    int upperIndex = std::get<2>(GetParam());
    int adjustedUpperIndex = std::max(std::min(upperIndex, count), 0);
    auto rows = ReadRows(0, lowerIndex, upperIndex, TColumnFilter({2}));
    EXPECT_EQ(adjustedUpperIndex - adjustedLowerIndex, static_cast<int>(rows.size()));
    for (int index = 0; index < rows.size(); ++index) {
        EXPECT_TRUE(AreQueryRowsEqual(rows[index], Format("a=%v", index + adjustedLowerIndex)));
    }
}

INSTANTIATE_TEST_CASE_P(
    Read,
    TOrderedDynamicStoreReadTest,
    ::testing::Values(
        std::make_tuple(1,      0,   0),
        std::make_tuple(1,      0,   1),
        std::make_tuple(1,    -10,  -10),
        std::make_tuple(1,     10,   10),
        std::make_tuple(100,   50,   60),
        std::make_tuple(100,   60,  200),
        std::make_tuple(100,  -10,   20),
        std::make_tuple(1000,   0, 1000)));

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicStoreWriteTest
    : public TOrderedDynamicStoreTest
    , public ::testing::WithParamInterface<int>
{ };

TEST_P(TOrderedDynamicStoreWriteTest, Write)
{
    EXPECT_EQ(0, Store_->GetRowCount());
    EXPECT_EQ(0, Store_->GetValueCount());

    int count = GetParam();
    for (int i = 0; i < count; ++i) {
        EXPECT_EQ(i, Store_->GetRowCount());
        EXPECT_EQ(i * 3, Store_->GetValueCount());
        WriteRow(BuildRow(Format("a=%v", i)));
    }

    auto rows = Store_->GetAllRows();
    EXPECT_EQ(count, rows.size());
    for (int i = 0; i < count; ++i) {
        EXPECT_TRUE(AreRowsEqual(rows[i], Format("a=%v", i)));
    }
}

INSTANTIATE_TEST_CASE_P(
    Write,
    TOrderedDynamicStoreWriteTest,
    ::testing::Values(
        1,
        10,
        1000,
        2000,
        10000));

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicStoreTimestampColumnTest
    : public TOrderedDynamicStoreTest
{
protected:
    virtual TTableSchema GetSchema() const override
    {
        return TTableSchema({
            TColumnSchema("a", EValueType::Int64),
            TColumnSchema("$timestamp", EValueType::Uint64)
        });
    }
};

TEST_F(TOrderedDynamicStoreTimestampColumnTest, Write)
{
    auto ts1 = WriteRow(BuildRow("a=1"));
    auto ts2 = WriteRow(BuildRow("a=2"));
    auto ts3 = WriteRow(BuildRow("a=3"));

    auto rows = Store_->GetAllRows();
    EXPECT_EQ(3, rows.size());

    EXPECT_TRUE(AreRowsEqual(rows[0], Format("a=1;\"$timestamp\"=%vu", ts1)));
    EXPECT_TRUE(AreRowsEqual(rows[1], Format("a=2;\"$timestamp\"=%vu", ts2)));
    EXPECT_TRUE(AreRowsEqual(rows[2], Format("a=3;\"$timestamp\"=%vu", ts3)));
}

TEST_F(TOrderedDynamicStoreTimestampColumnTest, Serialize)
{
    WriteRow(BuildRow("a=1"));
    WriteRow(BuildRow("a=2"));
    WriteRow(BuildRow("a=3"));

    auto dump = DumpStore();
    ReserializeStore();
    EXPECT_EQ(dump, DumpStore());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT

