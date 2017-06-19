#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

void CheckSerialize(TUnversionedRow original)
{
    auto serialized = NYT::ToProto<TString>(original);
    auto deserialized =  NYT::FromProto<TUnversionedOwningRow>(serialized);

    ASSERT_EQ(original, deserialized);
}

TEST(TUnversionedRowTest, Serialize1)
{
    TUnversionedOwningRowBuilder builder;
    auto row = builder.FinishRow();
    CheckSerialize(row);
}

TEST(TUnversionedRowTest, Serialize2)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 0));
    builder.AddValue(MakeUnversionedInt64Value(42, 1));
    builder.AddValue(MakeUnversionedDoubleValue(0.25, 2));
    CheckSerialize(builder.FinishRow());
}

TEST(TUnversionedRowTest, Serialize3)
{
    // TODO(babenko): cannot test Any type at the moment since CompareRowValues does not work
    // for it.
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeUnversionedStringValue("string1", 10));
    builder.AddValue(MakeUnversionedInt64Value(1234, 20));
    builder.AddValue(MakeUnversionedStringValue("string2", 30));
    builder.AddValue(MakeUnversionedDoubleValue(4321.0, 1000));
    builder.AddValue(MakeUnversionedStringValue("", 10000));
    CheckSerialize(builder.FinishRow());
}

TEST(TUnversionedRowTest, Serialize4)
{
    // TODO(babenko): cannot test Any type at the moment since CompareRowValues does not work
    // for it.
    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedStringValue("string1"));
    builder.AddValue(MakeUnversionedStringValue("string2"));
    CheckSerialize(builder.GetRow());
}

TEST(TUnversionedRowTest, Serialize5)
{
    CheckSerialize(TUnversionedRow());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT
