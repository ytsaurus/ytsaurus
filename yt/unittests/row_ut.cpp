#include "stdafx.h"

#include <core/misc/protobuf_helpers.h>

#include <ytlib/new_table_client/row.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NVersionedTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

bool AreRowsEqual(TRow lhs, TRow rhs)
{
    if (!lhs && rhs)
        return false;
    if (lhs && !rhs)
        return false;
    if (!lhs && !rhs)
        return true;

    if (lhs.GetValueCount() != rhs.GetValueCount())
        return false;
    if (lhs.GetDeleted() != rhs.GetDeleted())
        return false;
    if (lhs.GetTimestamp() != rhs.GetTimestamp())
        return false;

    for (int index = 0; index < lhs.GetValueCount(); ++index) {
        if (CompareRowValues(lhs[index], rhs[index]) != 0)
            return false;
    }
    
    return true;
}

void CheckSerialize(TRow row)
{
    TOwningRow owningRow(row);

    ASSERT_TRUE(AreRowsEqual(row, owningRow));

    auto protoRow = NYT::ToProto<Stroka>(owningRow);
    auto owningRow2 =  NYT::FromProto<TOwningRow>(protoRow);

    ASSERT_TRUE(AreRowsEqual(owningRow, owningRow2));
}

TEST(TRowTest, Serialize1)
{
    TRowBuilder builder;
    auto row = builder.GetRow();
    row.SetDeleted(true);
    row.SetTimestamp(123);
    CheckSerialize(row);

}

TEST(TRowTest, Serialize2)
{
    TRowBuilder builder;
    builder.AddValue(TRowValue::MakeSentinel(EColumnType::Null, 0));
    builder.AddValue(TRowValue::MakeInteger(42, 1));
    builder.AddValue(TRowValue::MakeDouble(0.25, 2));
    CheckSerialize(builder.GetRow());
}

TEST(TRowTest, Serialize3)
{
    // TODO(babenko): cannot test Any type at the moment since CompareRowValues does not work
    // for it.
    TRowBuilder builder;
    builder.AddValue(TRowValue::MakeString("string1", 10));
    builder.AddValue(TRowValue::MakeInteger(1234, 20));
    builder.AddValue(TRowValue::MakeString("string2", 30));
    builder.AddValue(TRowValue::MakeDouble(4321.0, 1000));
    builder.AddValue(TRowValue::MakeString("", 10000));
    CheckSerialize(builder.GetRow());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NVersionedTableClient
} // namespace NYT
