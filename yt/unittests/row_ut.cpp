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

class TRowBuilder
{
public:
    explicit TRowBuilder(int valueCount)
        : Data(new char[sizeof (TRowHeader) + sizeof (TRowValue) * valueCount])
    {
        auto* header = reinterpret_cast<TRowHeader*>(Data.get());
        header->ValueCount = valueCount;

        TRow row(*this);
        row.SetDeleted(false);
        row.SetTimestamp(NullTimestamp);
        for (int index = 0; index < valueCount; ++index) {
            row[index] = TRowValue::MakeNull(-1);
        }
    }

    operator TRow()
    {
        return TRow(reinterpret_cast<TRowHeader*>(Data.get()));
    }

private:
    std::unique_ptr<char[]> Data;

};

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
    TRowBuilder builder(0);
    TRow row(builder);
    row.SetDeleted(true);
    row.SetTimestamp(123);
    CheckSerialize(row);

}

TEST(TRowTest, Serialize2)
{
    TRowBuilder builder(3);
    TRow row(builder);
    row[0] = TRowValue::MakeNull(0);
    row[1] = TRowValue::MakeInteger(1, 42);
    row[2] = TRowValue::MakeDouble(2, 0.25);
    CheckSerialize(row);
}

TEST(TRowTest, Serialize3)
{
    // TODO(babenko): cannot test Any type at the moment since CompareRowValues does not work
    // for it.
    TRowBuilder builder(5);
    TRow row(builder);
    row[0] = TRowValue::MakeString(10, "string1");
    row[1] = TRowValue::MakeInteger(20, 1234);
    row[2] = TRowValue::MakeString(30, "string2");
    row[3] = TRowValue::MakeDouble(1000, 4321.0);
    row[4] = TRowValue::MakeString(10000, "");
    CheckSerialize(row);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NVersionedTableClient
} // namespace NYT
