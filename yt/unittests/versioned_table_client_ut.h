#include "stdafx.h"
#include "framework.h"

#include <yt/core/yson/public.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/node.h>

#include <yt/ytlib/new_table_client/public.h>
#include <yt/ytlib/new_table_client/unversioned_row.h>
#include <yt/ytlib/new_table_client/versioned_row.h>

namespace NYT {
namespace NVersionedTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;

inline TUnversionedOwningRow BuildKey(const Stroka& yson)
{
    TUnversionedOwningRowBuilder keyBuilder;
    auto keyParts = ConvertTo<std::vector<INodePtr>>(
        TYsonString(yson, EYsonType::ListFragment));

    for (auto keyPart : keyParts) {
        switch (keyPart->GetType()) {
            case ENodeType::Integer:
                keyBuilder.AddValue(MakeIntegerValue<TUnversionedValue>(
                    keyPart->GetValue<i64>()));
                break;
            case ENodeType::Double:
                keyBuilder.AddValue(MakeDoubleValue<TUnversionedValue>(
                    keyPart->GetValue<double>()));
                break;
            case ENodeType::String:
                keyBuilder.AddValue(MakeStringValue<TUnversionedValue>(
                    keyPart->GetValue<Stroka>()));
                break;
            case ENodeType::Entity:
                    keyBuilder.AddValue(MakeSentinelValue<TUnversionedValue>(
                        keyPart->Attributes().Get<EValueType>("type")));
                break;
            default:
                keyBuilder.AddValue(MakeAnyValue<TUnversionedValue>(
                    ConvertToYsonString(keyPart).Data()));
                break;
        }
    }

    return keyBuilder.GetRowAndReset();
}

inline Stroka KeyToYson(TKey key)
{
    return ConvertToYsonString(key, EYsonFormat::Text).Data();
}


class TVersionedTableClientTestBase
    : public ::testing::Test
{
protected:
    void ExpectRowsEqual(TVersionedRow expected, TVersionedRow actual)
    {
        if (!expected) {
            EXPECT_FALSE(actual);
            return;
        }

        EXPECT_EQ(0, CompareRows(expected.BeginKeys(), expected.EndKeys(), actual.BeginKeys(), actual.EndKeys()));
        EXPECT_EQ(expected.GetTimestampCount(), actual.GetTimestampCount());
        for (int i = 0; i < expected.GetTimestampCount(); ++i) {
            EXPECT_EQ(expected.BeginTimestamps()[i], actual.BeginTimestamps()[i]);
        }

        EXPECT_EQ(expected.GetValueCount(), actual.GetValueCount());
        for (int i = 0; i < expected.GetValueCount(); ++i) {
            EXPECT_EQ(CompareRowValues(expected.BeginValues()[i], actual.BeginValues()[i]), 0);
            EXPECT_EQ(expected.BeginValues()[i].Timestamp, actual.BeginValues()[i].Timestamp);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NVersionedTableClient
} // namespace NYT

