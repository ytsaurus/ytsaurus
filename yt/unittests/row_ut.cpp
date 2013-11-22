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
    TUnversionedOwningRow owningRow(row);

    ASSERT_TRUE(AreRowsEqual(row, owningRow));

    auto protoRow = NYT::ToProto<Stroka>(owningRow);
    auto owningRow2 =  NYT::FromProto<TUnversionedOwningRow>(protoRow);

    ASSERT_TRUE(AreRowsEqual(owningRow, owningRow2));
}

TEST(TUnversionedRowTest, Serialize1)
{
    TUnversionedRowBuilder builder;
    auto row = builder.GetRow();
    CheckSerialize(row);
}

TEST(TUnversionedRowTest, Serialize2)
{
    TUnversionedRowBuilder builder;
    builder.AddValue(TUnversionedValue::MakeSentinel(EValueType::Null, 0));
    builder.AddValue(TUnversionedValue::MakeInteger(42, 1));
    builder.AddValue(TUnversionedValue::MakeDouble(0.25, 2));
    CheckSerialize(builder.GetRow());
}

TEST(TUnversionedRowTest, Serialize3)
{
    // TODO(babenko): cannot test Any type at the moment since CompareRowValues does not work
    // for it.
    TUnversionedRowBuilder builder;
    builder.AddValue(TUnversionedValue::MakeString("string1", 10));
    builder.AddValue(TUnversionedValue::MakeInteger(1234, 20));
    builder.AddValue(TUnversionedValue::MakeString("string2", 30));
    builder.AddValue(TUnversionedValue::MakeDouble(4321.0, 1000));
    builder.AddValue(TUnversionedValue::MakeString("", 10000));
    CheckSerialize(builder.GetRow());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NVersionedTableClient
} // namespace NYT
