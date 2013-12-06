#include "stdafx.h"

#include <core/misc/protobuf_helpers.h>

#include <ytlib/new_table_client/row.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NVersionedTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

bool AreRowsEqual(TUnversionedRow lhs, TUnversionedRow rhs)
{
    if (!lhs && rhs)
        return false;
    if (lhs && !rhs)
        return false;
    if (!lhs && !rhs)
        return true;

    if (lhs.GetValueCount() != rhs.GetValueCount())
        return false;

    for (int index = 0; index < lhs.GetValueCount(); ++index) {
        if (CompareRowValues(lhs[index], rhs[index]) != 0)
            return false;
    }
    
    return true;
}

void CheckSerialize(TUnversionedRow row)
{
    TUnversionedOwningRow original(row);

    ASSERT_TRUE(AreRowsEqual(row, original));

    auto serialized = NYT::ToProto<Stroka>(original);
    auto deserialized =  NYT::FromProto<TUnversionedOwningRow>(serialized);

    ASSERT_TRUE(AreRowsEqual(original, deserialized));
}

TEST(TUnversionedRowTest, Serialize1)
{
    TUnversionedOwningRowBuilder builder;
    auto row = builder.Finish();
    CheckSerialize(row);
}

TEST(TUnversionedRowTest, Serialize2)
{
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeSentinelValue<TUnversionedValue>(EValueType::Null, 0));
    builder.AddValue(MakeIntegerValue<TUnversionedValue>(42, 1));
    builder.AddValue(MakeDoubleValue<TUnversionedValue>(0.25, 2));
    CheckSerialize(builder.Finish());
}

TEST(TUnversionedRowTest, Serialize3)
{
    // TODO(babenko): cannot test Any type at the moment since CompareRowValues does not work
    // for it.
    TUnversionedOwningRowBuilder builder;
    builder.AddValue(MakeStringValue<TUnversionedValue>("string1", 10));
    builder.AddValue(MakeIntegerValue<TUnversionedValue>(1234, 20));
    builder.AddValue(MakeStringValue<TUnversionedValue>("string2", 30));
    builder.AddValue(MakeDoubleValue<TUnversionedValue>(4321.0, 1000));
    builder.AddValue(MakeStringValue<TUnversionedValue>("", 10000));
    CheckSerialize(builder.Finish());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NVersionedTableClient
} // namespace NYT
