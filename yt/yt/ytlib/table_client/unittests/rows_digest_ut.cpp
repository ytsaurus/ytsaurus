#include <yt/yt/ytlib/table_client/rows_digest.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TRowsDigestBuilderTest
    : public ::testing::Test
{
protected:
    TNameTablePtr NameTable_ = New<TNameTable>();

    int GetColumnId(const std::string& name)
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

TEST_F(TRowsDigestBuilderTest, EqualRowsProduceSameDigest)
{
    TRowsDigestBuilder digestBuilder1(NameTable_);
    TRowsDigestBuilder digestBuilder2(NameTable_);

    digestBuilder1.ProcessRow(BuildRow(
        MakeUnversionedInt64Value(42, GetColumnId("column_a"))));
    digestBuilder2.ProcessRow(BuildRow(
        MakeUnversionedInt64Value(42, GetColumnId("column_a"))));

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, DifferentValuesProduceDifferentDigests)
{
    TRowsDigestBuilder digestBuilder1(NameTable_);
    TRowsDigestBuilder digestBuilder2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedInt64Value(42, GetColumnId("column_a")));
    auto row2 = BuildRow(MakeUnversionedInt64Value(43, GetColumnId("column_a")));

    digestBuilder1.ProcessRow(row1);
    digestBuilder2.ProcessRow(row2);

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_NE(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, ColumnOrderDoesNotMatter)
{
    TRowsDigestBuilder digestBuilder1(NameTable_);
    TRowsDigestBuilder digestBuilder2(NameTable_);

    auto row1 = BuildRow(
        MakeUnversionedInt64Value(1, GetColumnId("a")),
        MakeUnversionedUint64Value(2, GetColumnId("b")),
        MakeUnversionedInt64Value(3, GetColumnId("c")));

    auto row2 = BuildRow(
        MakeUnversionedInt64Value(3, GetColumnId("c")),
        MakeUnversionedInt64Value(1, GetColumnId("a")),
        MakeUnversionedUint64Value(2, GetColumnId("b")));

    digestBuilder1.ProcessRow(row1);
    digestBuilder2.ProcessRow(row2);

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, DifferentColumnNamesProduceDifferentDigests)
{
    TRowsDigestBuilder digestBuilder1(NameTable_);
    TRowsDigestBuilder digestBuilder2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedInt64Value(42, GetColumnId("a")));
    auto row2 = BuildRow(MakeUnversionedInt64Value(42, GetColumnId("b")));

    digestBuilder1.ProcessRow(row1);
    digestBuilder2.ProcessRow(row2);

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_NE(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, DifferentNameTablesWithSameColumnNames)
{
    auto nameTable1 = New<TNameTable>();
    auto nameTable2 = New<TNameTable>();

    TRowsDigestBuilder digestBuilder1(nameTable1);
    TRowsDigestBuilder digestBuilder2(nameTable2);

    // Register columns in different order in each name table.
    // In nameTable1: "a" gets ID 0, "b" gets ID 1.
    int id1_a = nameTable1->GetIdOrRegisterName("a");
    int id1_b = nameTable1->GetIdOrRegisterName("b");

    // In nameTable2: "b" gets ID 0, "a" gets ID 1 (reversed).
    int id2_b = nameTable2->GetIdOrRegisterName("b");
    int id2_a = nameTable2->GetIdOrRegisterName("a");

    auto row1 = BuildRow(
        MakeUnversionedInt64Value(1, id1_a),
        MakeUnversionedInt64Value(2, id1_b));

    auto row2 = BuildRow(
        MakeUnversionedInt64Value(1, id2_a),
        MakeUnversionedInt64Value(2, id2_b));

    digestBuilder1.ProcessRow(row1);
    digestBuilder2.ProcessRow(row2);

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    // Digests should be equal because column names are the same.
    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, MultipleRowsAccumulate)
{
    TRowsDigestBuilder digestBuilder1(NameTable_);
    TRowsDigestBuilder digestBuilder2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedInt64Value(1, GetColumnId("a")));
    auto row2 = BuildRow(MakeUnversionedInt64Value(2, GetColumnId("a")));

    digestBuilder1.ProcessRow(row1);
    digestBuilder1.ProcessRow(row2);

    auto row3 = BuildRow(MakeUnversionedInt64Value(1, GetColumnId("a")));
    auto row4 = BuildRow(MakeUnversionedInt64Value(2, GetColumnId("a")));

    digestBuilder2.ProcessRow(row3);
    digestBuilder2.ProcessRow(row4);

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, RowOrderMatters)
{
    TRowsDigestBuilder digestBuilder1(NameTable_);
    TRowsDigestBuilder digestBuilder2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedInt64Value(1, GetColumnId("a")));
    auto row2 = BuildRow(MakeUnversionedInt64Value(2, GetColumnId("a")));

    digestBuilder1.ProcessRow(row1);
    digestBuilder1.ProcessRow(row2);

    digestBuilder2.ProcessRow(row2);
    digestBuilder2.ProcessRow(row1);

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_NE(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, DifferentValueTypes)
{
    auto nameTable1 = New<TNameTable>();
    auto nameTable2 = New<TNameTable>();

    // Register some columns in nameTable1 before building the row.
    nameTable1->GetIdOrRegisterName("null_col");
    nameTable1->GetIdOrRegisterName("bool_col");

    // Register different columns in nameTable2 before building the row.
    nameTable2->GetIdOrRegisterName("string_col");
    nameTable2->GetIdOrRegisterName("int_col");

    TRowsDigestBuilder digestBuilder1(nameTable1);
    TRowsDigestBuilder digestBuilder2(nameTable2);

    auto row1 = BuildRow(
        MakeUnversionedInt64Value(42, nameTable1->GetIdOrRegisterName("int_col")),
        MakeUnversionedUint64Value(100, nameTable1->GetIdOrRegisterName("uint_col")),
        MakeUnversionedCompositeValue("[1;2;3]", nameTable1->GetIdOrRegisterName("composite_col")),
        MakeUnversionedDoubleValue(3.14, nameTable1->GetIdOrRegisterName("double_col")),
        MakeUnversionedBooleanValue(true, nameTable1->GetIdOrRegisterName("bool_col")),
        MakeUnversionedStringValue("test", nameTable1->GetIdOrRegisterName("string_col")),
        MakeUnversionedNullValue(nameTable1->GetIdOrRegisterName("null_col")),
        MakeUnversionedAnyValue("{key=value}", nameTable1->GetIdOrRegisterName("any_col")));

    digestBuilder1.ProcessRow(row1);
    auto digest1 = digestBuilder1.GetDigest();

    auto row2 = BuildRow(
        MakeUnversionedNullValue(nameTable2->GetIdOrRegisterName("null_col")),
        MakeUnversionedStringValue("test", nameTable2->GetIdOrRegisterName("string_col")),
        MakeUnversionedBooleanValue(true, nameTable2->GetIdOrRegisterName("bool_col")),
        MakeUnversionedDoubleValue(3.14, nameTable2->GetIdOrRegisterName("double_col")),
        MakeUnversionedUint64Value(100, nameTable2->GetIdOrRegisterName("uint_col")),
        MakeUnversionedCompositeValue("[1;2;3]", nameTable2->GetIdOrRegisterName("composite_col")),
        MakeUnversionedInt64Value(42, nameTable2->GetIdOrRegisterName("int_col")),
        MakeUnversionedAnyValue("{key=value}", nameTable2->GetIdOrRegisterName("any_col")));

    digestBuilder2.ProcessRow(row2);
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, ValueTypeMatters)
{
    TRowsDigestBuilder digestBuilder1(NameTable_);
    TRowsDigestBuilder digestBuilder2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedInt64Value(42, GetColumnId("col")));
    auto row2 = BuildRow(MakeUnversionedUint64Value(42, GetColumnId("col")));

    digestBuilder1.ProcessRow(row1);
    digestBuilder2.ProcessRow(row2);

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_NE(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, DifferentColumnCount)
{
    TRowsDigestBuilder digestBuilder1(NameTable_);
    TRowsDigestBuilder digestBuilder2(NameTable_);

    auto row1 = BuildRow(
        MakeUnversionedInt64Value(1, GetColumnId("a")),
        MakeUnversionedInt64Value(2, GetColumnId("b")));

    auto row2 = BuildRow(
        MakeUnversionedInt64Value(3, GetColumnId("a")),
        MakeUnversionedInt64Value(4, GetColumnId("b")));

    auto row3 = BuildRow(
        MakeUnversionedInt64Value(3, GetColumnId("a")),
        MakeUnversionedInt64Value(4, GetColumnId("b")),
        MakeUnversionedInt64Value(5, GetColumnId("c")));

    digestBuilder1.ProcessRow(row1);
    digestBuilder1.ProcessRow(row2);

    digestBuilder2.ProcessRow(row1);
    digestBuilder2.ProcessRow(row3);

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_NE(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, CompositeAndAnyValueTypes)
{
    TRowsDigestBuilder digestBuilder1(NameTable_);
    TRowsDigestBuilder digestBuilder2(NameTable_);

    // First row with Composite and Any types.
    auto row1 = BuildRow(
        MakeUnversionedCompositeValue("[1;2;3]", GetColumnId("composite_col")),
        MakeUnversionedAnyValue("{key=value}", GetColumnId("any_col")));

    // Second row with same values.
    auto row2 = BuildRow(
        MakeUnversionedCompositeValue("[1;2;3]", GetColumnId("composite_col")),
        MakeUnversionedAnyValue("{key=value}", GetColumnId("any_col")));

    digestBuilder1.ProcessRow(row1);
    digestBuilder2.ProcessRow(row2);

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, StringContentMatters)
{
    TRowsDigestBuilder digestBuilder1(NameTable_);
    TRowsDigestBuilder digestBuilder2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedStringValue("test", GetColumnId("col")));
    auto row2 = BuildRow(MakeUnversionedStringValue("test1", GetColumnId("col")));

    digestBuilder1.ProcessRow(row1);
    digestBuilder2.ProcessRow(row2);

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_NE(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, ManyRowsWithDynamicColumns)
{
    auto nameTable1 = New<TNameTable>();
    auto nameTable2 = New<TNameTable>();
    TRowsDigestBuilder digestBuilder1(nameTable1);
    TRowsDigestBuilder digestBuilder2(nameTable2);

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

        digestBuilder1.ProcessRow(row1);
        digestBuilder2.ProcessRow(row2);
    }

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_EQ(digest1, digest2);
}

TEST_F(TRowsDigestBuilderTest, LongStringValues)
{
    TRowsDigestBuilder digestBuilder1(NameTable_);
    TRowsDigestBuilder digestBuilder2(NameTable_);

    std::string longString(1000, 'a');

    auto row1 = BuildRow(MakeUnversionedStringValue(longString, GetColumnId("col")));
    auto row2 = BuildRow(MakeUnversionedStringValue(longString, GetColumnId("col")));

    digestBuilder1.ProcessRow(row1);
    digestBuilder2.ProcessRow(row2);

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_EQ(digest1, digest2);

    TRowsDigestBuilder digestBuilder3(NameTable_);
    std::string longString2(1000, 'a');
    longString2[999] = 'b';

    auto row3 = BuildRow(MakeUnversionedStringValue(longString2, GetColumnId("col")));

    digestBuilder3.ProcessRow(row3);
    auto digest3 = digestBuilder3.GetDigest();

    EXPECT_NE(digest1, digest3);
}

TEST_F(TRowsDigestBuilderTest, EmptyStringValues)
{
    TRowsDigestBuilder digestBuilder1(NameTable_);
    TRowsDigestBuilder digestBuilder2(NameTable_);

    auto row1 = BuildRow(MakeUnversionedStringValue("", GetColumnId("col")));
    auto row2 = BuildRow(MakeUnversionedStringValue("", GetColumnId("col")));

    digestBuilder1.ProcessRow(row1);
    digestBuilder2.ProcessRow(row2);

    auto digest1 = digestBuilder1.GetDigest();
    auto digest2 = digestBuilder2.GetDigest();

    EXPECT_EQ(digest1, digest2);

    TRowsDigestBuilder digestBuilder3(NameTable_);

    auto row3 = BuildRow(MakeUnversionedNullValue(GetColumnId("col")));

    digestBuilder3.ProcessRow(row3);
    auto digest3 = digestBuilder3.GetDigest();

    EXPECT_NE(digest1, digest3);
}

TEST_F(TRowsDigestBuilderTest, NullValuesWithRandomData)
{
    auto computeDigest = [&] () {
        TRowsDigestBuilder digestBuilder(NameTable_);

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedNullValue(GetColumnId("col")));
        builder.BeginValues()->Data.Uint64 = Ui64Distribution_(Gen_);
        digestBuilder.ProcessRow(builder.FinishRow());

        return digestBuilder.GetDigest();
    };

    EXPECT_EQ(computeDigest(), computeDigest());
}

TEST_F(TRowsDigestBuilderTest, BoolValuesWithRandomData)
{
    auto computeDigest = [&] (bool value) {
        TRowsDigestBuilder digestBuilder(NameTable_);

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedBooleanValue(false, GetColumnId("col")));
        builder.BeginValues()->Data.Uint64 = Ui64Distribution_(Gen_);
        builder.BeginValues()->Data.Boolean = value;
        digestBuilder.ProcessRow(builder.FinishRow());

        return digestBuilder.GetDigest();
    };

    EXPECT_EQ(computeDigest(false), computeDigest(false));
    EXPECT_EQ(computeDigest(true), computeDigest(true));
    EXPECT_NE(computeDigest(true), computeDigest(false));
    EXPECT_NE(computeDigest(false), computeDigest(true));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
