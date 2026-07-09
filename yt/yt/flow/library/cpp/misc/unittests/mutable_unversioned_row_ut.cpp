#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/mutable_unversioned_row.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSharedRef TEST_STRING = TSharedRef::FromString("Hello world!");

TUnversionedOwningRow MakeTestRow()
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(42, 0));
    builder.AddValue(MakeUnversionedNullValue(1));
    builder.AddValue(MakeUnversionedStringValue(TEST_STRING.ToStringBuf(), 2));
    builder.AddValue(MakeUnversionedNullValue(1));
    return builder.FinishRow();
}

TMutableUnversionedOwningRow MakeTestRowByPushBack(bool reserve = true)
{
    TMutableUnversionedOwningRow row;
    if (reserve) {
        row.Reserve(4);
    }
    row.PushBack(MakeUnversionedInt64Value(42, 0));
    row.PushBack(MakeUnversionedNullValue(1));
    row.PushBack(MakeUnversionedStringOwningValue(TEST_STRING, 2));
    row.PushBack(MakeUnversionedNullValue(1));
    return row;
}

std::string GetDiff(TUnversionedRow actual, TUnversionedRow expected)
{
    if (TDefaultUnversionedRowEqual()(actual, expected)) {
        return "";
    }
    return NYT::Format("Has diff. Actual: %v, expected: %v", actual, expected);
}

TEST(TMutableUnversionedOwningRowTest, Constructors)
{
    // Default constructor.
    // + copy constructors.
    {
        TMutableUnversionedOwningRow row;
        EXPECT_FALSE(row);
        EXPECT_FALSE(row.Get());
        EXPECT_EQ(row.GetCount(), 0);
        EXPECT_EQ(row.GetSpaceUsed(), 0u);

        TMutableUnversionedOwningRow rowCopy = row;
        EXPECT_FALSE(rowCopy);

        TMutableUnversionedOwningRow rowMovedCopy = std::move(row);
        EXPECT_FALSE(rowMovedCopy);
    }

    // TMutableUnversionedOwningRow(NTableClient::TUnversionedValueRange range).
    // explicit TMutableUnversionedOwningRow(NTableClient::TUnversionedRow other).
    // + copy constructors.
    {
        auto original = MakeTestRow();
        TMutableUnversionedOwningRow row1(original.Elements());
        TMutableUnversionedOwningRow row2(original);
        TMutableUnversionedOwningRow row3(row1);
        TMutableUnversionedOwningRow row4Original(row1);
        TMutableUnversionedOwningRow row4(std::move(row4Original));
        // NOLINTNEXTLINE(bugprone-use-after-move)
        EXPECT_FALSE(row4Original);
        for (auto& row : {row1, row2, row3, row4}) {
            EXPECT_TRUE(row);
            EXPECT_TRUE(row.Get());
            EXPECT_EQ(row.GetCount(), original.GetCount());
            EXPECT_GE(row.GetSpaceUsed(), 20u);
            EXPECT_LE(row.GetSpaceUsed(), original.GetSpaceUsed() * 2u);
            EXPECT_EQ(GetDiff(row.Get(), original), "");
        }
    }
}

TEST(TMutableUnversionedOwningRowTest, FirstNElements)
{
    EXPECT_EQ(
        GetDiff(
            TUnversionedOwningRow(TMutableUnversionedOwningRow(MakeTestRow()).FirstNElements(3)),
            TUnversionedOwningRow(MakeTestRow().FirstNElements(3))),
        "");
}

TEST(TMutableUnversionedOwningRowTest, ElementOperations)
{
    EXPECT_EQ(GetDiff(MakeTestRow(), MakeTestRowByPushBack()), "");

    {
        auto row = MakeTestRowByPushBack();

        auto tmp = row.GetOwning(2);
        row.Set(2, row.GetOwning(1));
        row.Set(1, tmp);

        EXPECT_NE(GetDiff(MakeTestRow(), row), "");

        tmp = row.GetOwning(2);
        row.Set(2, row.GetOwning(1));
        row.Set(1, tmp);

        EXPECT_EQ(GetDiff(MakeTestRow(), row), "");
    }
}

TEST(TMutableUnversionedOwningRowTest, Reserve)
{
    auto row = MakeTestRowByPushBack();
    int count = row.GetCount();
    size_t spaceUsed = row.GetSpaceUsed();

    row.Reserve(1);

    EXPECT_EQ(row.GetCount(), count);
    EXPECT_EQ(row.GetSpaceUsed(), spaceUsed);
    EXPECT_EQ(GetDiff(MakeTestRow(), row), "");

    row.Reserve(1000);

    EXPECT_EQ(row.GetCount(), count);
    EXPECT_GE(row.GetSpaceUsed(), spaceUsed * 10);

    EXPECT_EQ(GetDiff(MakeTestRow(), row), "");

    EXPECT_EQ(GetDiff(MakeTestRowByPushBack(), MakeTestRowByPushBack(/*reserve*/ false)), "");

    // Large row.
    {
        auto makeRow = [] (bool reserve) {
            constexpr int count = 1000;
            TMutableUnversionedOwningRow row;
            if (reserve) {
                row.Reserve(count);
            }
            for (int i = 0; i < count; ++i) {
                row.PushBack(MakeUnversionedStringOwningValue(TSharedRef::FromString("Hello you!"), i));
            }
            return row;
        };
        EXPECT_EQ(GetDiff(makeRow(true), makeRow(false)), "");
    }
}

TEST(TMutableUnversionedOwningRowTest, CopyIsIndependent)
{
    auto row = MakeTestRowByPushBack();
    auto rowCopy = row;
    rowCopy.Set(0, rowCopy.GetOwning(2));
    EXPECT_EQ(GetDiff(row, MakeTestRow()), "");
}

TEST(TMutableUnversionedOwningRowTest, NoStringCopy)
{
    auto row = MakeTestRowByPushBack();
    EXPECT_EQ(static_cast<const void*>(row[2].Data.String), static_cast<const void*>(TEST_STRING.data()));
    EXPECT_EQ(static_cast<const void*>(row.GetOwning(2).GetStringRef().data()), static_cast<const void*>(TEST_STRING.data()));

    // Sanity check (TUnversionedOwningRow copies strings in constructor).
    auto copyingRow = MakeTestRow();
    EXPECT_NE(static_cast<const void*>(copyingRow[2].Data.String), static_cast<const void*>(TEST_STRING.data()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
