#include "dynamic_store_bits.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TUnversionedValue GetUnversionedKeyValue(TSortedDynamicRow row, int index, EValueType type)
{
    auto nullKeyMask = row.GetNullKeyMask();
    YT_ASSERT(index < static_cast<int>(sizeof(nullKeyMask) * 8));

    auto source = row.BeginKeys()[index];

    auto result = MakeUnversionedSentinelValue(EValueType::Null, index);
    if (!(nullKeyMask & (1 << index))) {
        result.Type = type;
        if (IsStringLikeType(type)) {
            result.Length = source.String->Length;
            result.Data.String = source.String->Data;
        } else {
            ::memcpy(&result.Data, &source, sizeof(TDynamicValueData));
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
