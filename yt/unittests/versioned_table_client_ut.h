#include "stdafx.h"

#include <yt/core/yson/public.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/node.h>

#include <yt/ytlib/new_table_client/public.h>
#include <yt/ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace {

using namespace NVersionedTableClient;
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

    return keyBuilder.Finish();
}

inline Stroka KeyToYson(TKey key)
{
    return ConvertToYsonString(key, EYsonFormat::Text).Data();
}

} // namespace
} // namespace NYT

