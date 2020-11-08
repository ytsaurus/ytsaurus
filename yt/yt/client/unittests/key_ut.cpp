#include <yt/client/table_client/key.h>

#include <yt/core/test_framework/framework.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyTest, Simple)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedDoubleValue(3.14, 0));
    builder.AddValue(MakeUnversionedInt64Value(-42, 1));
    builder.AddValue(MakeUnversionedUint64Value(27, 2));
    TString str = "Foo";
    builder.AddValue(MakeUnversionedStringValue(str, 3));

    auto owningRow = builder.FinishRow();
    // Builder captures string, so this address is different from str.data().
    auto* strPtr = owningRow[3].Data.String;

    auto row = owningRow;
    auto rowBeginPtr = row.Begin();
    {
        auto key = TKey::FromRow(row);
        EXPECT_EQ(row, key.AsRow());
        EXPECT_EQ(rowBeginPtr, key.Begin());
    }
    {
        // Steal row.
        auto stolenKey = TKey::FromRow(std::move(row));
        EXPECT_EQ(owningRow, stolenKey.AsRow());
        EXPECT_EQ(rowBeginPtr, stolenKey.Begin());
    }
    {
        auto owningKey = TOwningKey::FromRow(owningRow);
        EXPECT_EQ(owningRow, owningKey.AsRow());
    }
    {
        // Steal owningRow.
        auto stolenOwningKey = TOwningKey::FromRow(std::move(owningRow));
        EXPECT_EQ(EValueType::String, stolenOwningKey[3].Type);
        EXPECT_EQ(strPtr, stolenOwningKey[3].Data.String);
    }

    // Does not compile:
    // TOwningKey key;
    // TOwningKey key((TUnversionedRow()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
