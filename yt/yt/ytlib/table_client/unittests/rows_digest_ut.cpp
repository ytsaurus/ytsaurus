#include <yt/yt/ytlib/table_client/rows_digest.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TRowsDigestComputerTest
    : public ::testing::Test
{
protected:
    TNameTablePtr NameTable_ = New<TNameTable>();

    int GetColumnId(const TString& name)
    {
        return NameTable_->GetIdOrRegisterName(name);
    }

    TUnversionedOwningRow BuildRow(auto... values)
    {
        TUnversionedOwningRowBuilder builder;
        (builder.AddValue(values), ...);
        return builder.FinishRow();
    }

    std::mt19937 Gen_{42};
    std::uniform_int_distribution<ui64> Ui64Distribution_{0, std::numeric_limits<ui64>::max()};
};

TEST_F(TRowsDigestComputerTest, EqualRowsProduceSameDigest)
{
    TRowsDigestComputer computer1(NameTable_);
    TRowsDigestComputer computer2(NameTable_);

    computer1.ProcessRow(BuildRow(
        MakeUnversionedInt64Value(42, GetColumnId("column_a"))
    ));
    computer2.ProcessRow(BuildRow(
        MakeUnversionedInt64Value(42, GetColumnId("column_a"))
    ));

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, DifferentValuesProduceDifferentDigests)
{
    TRowsDigestComputer computer1(NameTable_);
    TRowsDigestComputer computer2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedInt64Value(42, GetColumnId("column_a")));
    auto row2 = BuildRow(MakeUnversionedInt64Value(43, GetColumnId("column_a")));

    computer1.ProcessRow(row1);
    computer2.ProcessRow(row2);

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_NE(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, ColumnOrderDoesNotMatter)
{
    TRowsDigestComputer computer1(NameTable_);
    TRowsDigestComputer computer2(NameTable_);

    auto row1 = BuildRow(
        MakeUnversionedInt64Value(1, GetColumnId("a")),
        MakeUnversionedUint64Value(2, GetColumnId("b")),
        MakeUnversionedInt64Value(3, GetColumnId("c"))
    );

    auto row2 = BuildRow(
        MakeUnversionedInt64Value(3, GetColumnId("c")),
        MakeUnversionedInt64Value(1, GetColumnId("a")),
        MakeUnversionedUint64Value(2, GetColumnId("b"))
    );

    computer1.ProcessRow(row1);
    computer2.ProcessRow(row2);

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, DifferentColumnNamesProduceDifferentDigests)
{
    TRowsDigestComputer computer1(NameTable_);
    TRowsDigestComputer computer2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedInt64Value(42, GetColumnId("a")));
    auto row2 = BuildRow(MakeUnversionedInt64Value(42, GetColumnId("b")));

    computer1.ProcessRow(row1);
    computer2.ProcessRow(row2);

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_NE(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, DifferentNameTablesWithSameColumnNames)
{
    auto nameTable1 = New<TNameTable>();
    auto nameTable2 = New<TNameTable>();

    TRowsDigestComputer computer1(nameTable1);
    TRowsDigestComputer computer2(nameTable2);

    // Register columns in different order in each name table.
    // In nameTable1: "a" gets ID 0, "b" gets ID 1.
    int id1_a = nameTable1->GetIdOrRegisterName("a");
    int id1_b = nameTable1->GetIdOrRegisterName("b");

    // In nameTable2: "b" gets ID 0, "a" gets ID 1 (reversed).
    int id2_b = nameTable2->GetIdOrRegisterName("b");
    int id2_a = nameTable2->GetIdOrRegisterName("a");

    auto row1 = BuildRow(
        MakeUnversionedInt64Value(1, id1_a),
        MakeUnversionedInt64Value(2, id1_b)
    );

    auto row2 = BuildRow(
        MakeUnversionedInt64Value(1, id2_a),
        MakeUnversionedInt64Value(2, id2_b)
    );

    computer1.ProcessRow(row1);
    computer2.ProcessRow(row2);

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    // Digests should be equal because column names are the same.
    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, MultipleRowsAccumulate)
{
    TRowsDigestComputer computer1(NameTable_);
    TRowsDigestComputer computer2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedInt64Value(1, GetColumnId("a")));
    auto row2 = BuildRow(MakeUnversionedInt64Value(2, GetColumnId("a")));

    computer1.ProcessRow(row1);
    computer1.ProcessRow(row2);

    auto row3 = BuildRow(MakeUnversionedInt64Value(1, GetColumnId("a")));
    auto row4 = BuildRow(MakeUnversionedInt64Value(2, GetColumnId("a")));

    computer2.ProcessRow(row3);
    computer2.ProcessRow(row4);

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, RowOrderMatters)
{
    TRowsDigestComputer computer1(NameTable_);
    TRowsDigestComputer computer2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedInt64Value(1, GetColumnId("a")));
    auto row2 = BuildRow(MakeUnversionedInt64Value(2, GetColumnId("a")));

    computer1.ProcessRow(row1);
    computer1.ProcessRow(row2);

    computer2.ProcessRow(row2);
    computer2.ProcessRow(row1);

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_NE(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, DifferentValueTypes)
{
    auto nameTable1 = New<TNameTable>();
    auto nameTable2 = New<TNameTable>();

    // Register some columns in nameTable1 before building the row.
    nameTable1->GetIdOrRegisterName("null_col");
    nameTable1->GetIdOrRegisterName("bool_col");

    // Register different columns in nameTable2 before building the row.
    nameTable2->GetIdOrRegisterName("string_col");
    nameTable2->GetIdOrRegisterName("int_col");

    TRowsDigestComputer computer1(nameTable1);
    TRowsDigestComputer computer2(nameTable2);

    auto row1 = BuildRow(
        MakeUnversionedInt64Value(42, nameTable1->GetIdOrRegisterName("int_col")),
        MakeUnversionedUint64Value(100, nameTable1->GetIdOrRegisterName("uint_col")),
        MakeUnversionedCompositeValue("[1;2;3]", nameTable1->GetIdOrRegisterName("composite_col")),
        MakeUnversionedDoubleValue(3.14, nameTable1->GetIdOrRegisterName("double_col")),
        MakeUnversionedBooleanValue(true, nameTable1->GetIdOrRegisterName("bool_col")),
        MakeUnversionedStringValue("test", nameTable1->GetIdOrRegisterName("string_col")),
        MakeUnversionedNullValue(nameTable1->GetIdOrRegisterName("null_col")),
        MakeUnversionedAnyValue("{key=value}", nameTable1->GetIdOrRegisterName("any_col"))
    );

    computer1.ProcessRow(row1);
    auto digest1 = computer1.GetDigest();

    auto row2 = BuildRow(
        MakeUnversionedNullValue(nameTable2->GetIdOrRegisterName("null_col")),
        MakeUnversionedStringValue("test", nameTable2->GetIdOrRegisterName("string_col")),
        MakeUnversionedBooleanValue(true, nameTable2->GetIdOrRegisterName("bool_col")),
        MakeUnversionedDoubleValue(3.14, nameTable2->GetIdOrRegisterName("double_col")),
        MakeUnversionedUint64Value(100, nameTable2->GetIdOrRegisterName("uint_col")),
        MakeUnversionedCompositeValue("[1;2;3]", nameTable2->GetIdOrRegisterName("composite_col")),
        MakeUnversionedInt64Value(42, nameTable2->GetIdOrRegisterName("int_col")),
        MakeUnversionedAnyValue("{key=value}", nameTable2->GetIdOrRegisterName("any_col"))
    );

    computer2.ProcessRow(row2);
    auto digest2 = computer2.GetDigest();

    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, ValueTypeMatters)
{
    TRowsDigestComputer computer1(NameTable_);
    TRowsDigestComputer computer2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedInt64Value(42, GetColumnId("col")));
    auto row2 = BuildRow(MakeUnversionedUint64Value(42, GetColumnId("col")));

    computer1.ProcessRow(row1);
    computer2.ProcessRow(row2);

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_NE(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, DifferentColumnCount)
{
    TRowsDigestComputer computer1(NameTable_);
    TRowsDigestComputer computer2(NameTable_);

    auto row1 = BuildRow(
        MakeUnversionedInt64Value(1, GetColumnId("a")),
        MakeUnversionedInt64Value(2, GetColumnId("b"))
    );

    auto row2 = BuildRow(
        MakeUnversionedInt64Value(3, GetColumnId("a")),
        MakeUnversionedInt64Value(4, GetColumnId("b"))
    );

    auto row3 = BuildRow(
        MakeUnversionedInt64Value(3, GetColumnId("a")),
        MakeUnversionedInt64Value(4, GetColumnId("b")),
        MakeUnversionedInt64Value(5, GetColumnId("c"))
    );

    computer1.ProcessRow(row1);
    computer1.ProcessRow(row2);

    computer2.ProcessRow(row1);
    computer2.ProcessRow(row3);

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_NE(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, CompositeAndAnyValueTypes)
{
    TRowsDigestComputer computer1(NameTable_);
    TRowsDigestComputer computer2(NameTable_);

    // First row with Composite and Any types.
    auto row1 = BuildRow(
        MakeUnversionedCompositeValue("[1;2;3]", GetColumnId("composite_col")),
        MakeUnversionedAnyValue("{key=value}", GetColumnId("any_col"))
    );

    // Second row with same values.
    auto row2 = BuildRow(
        MakeUnversionedCompositeValue("[1;2;3]", GetColumnId("composite_col")),
        MakeUnversionedAnyValue("{key=value}", GetColumnId("any_col"))
    );

    computer1.ProcessRow(row1);
    computer2.ProcessRow(row2);

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, StringContentMatters)
{
    TRowsDigestComputer computer1(NameTable_);
    TRowsDigestComputer computer2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedStringValue("test", GetColumnId("col")));
    auto row2 = BuildRow(MakeUnversionedStringValue("test1", GetColumnId("col")));

    computer1.ProcessRow(row1);
    computer2.ProcessRow(row2);

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_NE(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, ManyRowsWithDynamicColumns)
{
    auto nameTable1 = New<TNameTable>();
    auto nameTable2 = New<TNameTable>();
    TRowsDigestComputer computer1(nameTable1);
    TRowsDigestComputer computer2(nameTable2);

    // Process 10 rows, each adding a new column.
    for (int i = 0; i < 10; ++i) {
        std::vector<TUnversionedValue> values1;
        std::vector<TUnversionedValue> values2;

        std::vector<int> indices1;
        std::vector<int> indices2;
        for (int j = 0; j <= i; ++j) {
            indices1.push_back(j);
            indices2.push_back(j);
        }

        std::shuffle(indices1.begin(), indices1.end(), Gen_);
        std::shuffle(indices2.begin(), indices2.end(), Gen_);

        for (int j : indices1) {
            auto colName = Format("col_%v", j);
            values1.push_back(MakeUnversionedInt64Value(j * 10 + i, nameTable1->GetIdOrRegisterName(colName)));
        }

        for (int j : indices2) {
            auto colName = Format("col_%v", j);
            values2.push_back(MakeUnversionedInt64Value(j * 10 + i, nameTable2->GetIdOrRegisterName(colName)));
        }

        TUnversionedOwningRowBuilder builder1;
        for (const auto& value : values1) {
            builder1.AddValue(value);
        }
        auto row1 = builder1.FinishRow();

        TUnversionedOwningRowBuilder builder2;
        for (const auto& value : values2) {
            builder2.AddValue(value);
        }
        auto row2 = builder2.FinishRow();

        computer1.ProcessRow(row1);
        computer2.ProcessRow(row2);
    }

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestComputerTest, LongStringValues)
{
    TRowsDigestComputer computer1(NameTable_);
    TRowsDigestComputer computer2(NameTable_);

    TString longString(1000, 'a');

    auto row1 = BuildRow(MakeUnversionedStringValue(longString, GetColumnId("col")));
    auto row2 = BuildRow(MakeUnversionedStringValue(longString, GetColumnId("col")));

    computer1.ProcessRow(row1);
    computer2.ProcessRow(row2);

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_EQ(digest1, digest2);

    TRowsDigestComputer computer3(NameTable_);
    TString longString2(1000, 'a');
    longString2[999] = 'b';

    auto row3 = BuildRow(MakeUnversionedStringValue(longString2, GetColumnId("col")));

    computer3.ProcessRow(row3);
    auto digest3 = computer3.GetDigest();

    EXPECT_NE(digest1, digest3);
}

TEST_F(TRowsDigestComputerTest, EmptyStringValues)
{
    TRowsDigestComputer computer1(NameTable_);
    TRowsDigestComputer computer2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedStringValue("", GetColumnId("col")));
    auto row2 = BuildRow(MakeUnversionedStringValue("", GetColumnId("col")));

    computer1.ProcessRow(row1);
    computer2.ProcessRow(row2);

    auto digest1 = computer1.GetDigest();
    auto digest2 = computer2.GetDigest();

    EXPECT_EQ(digest1, digest2);

    TRowsDigestComputer computer3(NameTable_);

    auto row3 = BuildRow(MakeUnversionedNullValue(GetColumnId("col")));

    computer3.ProcessRow(row3);
    auto digest3 = computer3.GetDigest();

    EXPECT_NE(digest1, digest3);
}

TEST_F(TRowsDigestComputerTest, NullValuesWithRandomData)
{
    auto computeDigest = [&] () {
        TRowsDigestComputer computer(NameTable_);

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedNullValue(GetColumnId("col")));
        builder.BeginValues()->Data.Uint64 = Ui64Distribution_(Gen_);
        computer.ProcessRow(builder.FinishRow());

        return computer.GetDigest();
    };

    EXPECT_EQ(computeDigest(), computeDigest());
}

TEST_F(TRowsDigestComputerTest, BoolValuesWithRandomData)
{
    auto computeDigest = [&] (bool value) {
        TRowsDigestComputer computer(NameTable_);

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedBooleanValue(false, GetColumnId("col")));
        builder.BeginValues()->Data.Uint64 = Ui64Distribution_(Gen_);
        builder.BeginValues()->Data.Boolean = value;
        computer.ProcessRow(builder.FinishRow());

        return computer.GetDigest();
    };

    EXPECT_EQ(computeDigest(false), computeDigest(false));
    EXPECT_EQ(computeDigest(true), computeDigest(true));
    EXPECT_NE(computeDigest(true), computeDigest(false));
    EXPECT_NE(computeDigest(false), computeDigest(true));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
