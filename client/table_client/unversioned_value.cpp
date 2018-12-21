#include "unversioned_value.h"

#include <yt/core/misc/farm_hash.h>


#ifndef YT_COMPILING_UDF

#include "unversioned_row.h"

#include <yt/core/misc/error.h>
#include <yt/core/misc/format.h>

#include <yt/core/ytree/convert.h>

#endif

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

ui64 GetHash(const TUnversionedValue& value)
{
    // NB: hash function may change in future. Use fingerprints for persistent hashing.
    return GetFarmFingerprint(value);
}

// Forever-fixed Google FarmHash fingerprint.
TFingerprint GetFarmFingerprint(const TUnversionedValue& value)
{
    auto type = value.Type;
    switch (type) {
        case EValueType::String:
            return FarmFingerprint(value.Data.String, value.Length);

        case EValueType::Int64:
        case EValueType::Uint64:
        case EValueType::Double:
            // These types are aliased.
            return FarmFingerprint(value.Data.Int64);

        case EValueType::Boolean:
            return FarmFingerprint(value.Data.Boolean);

        case EValueType::Null:
            return FarmFingerprint(0);

        default:

#ifdef YT_COMPILING_UDF
            Y_UNREACHABLE();
#else
            THROW_ERROR_EXCEPTION(
                EErrorCode::UnhashableType,
                "Cannot hash values of type %Qlv; only scalar types are allowed for key columns",
                type)
                << TErrorAttribute("value", value);
#endif

    }
}

// Forever-fixed Google FarmHash fingerprint.
TFingerprint GetFarmFingerprint(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    ui64 result = 0xdeadc0de;
    for (const auto* value = begin; value < end; ++value) {
        result = FarmFingerprint(result, GetFarmFingerprint(*value));
    }
    return result ^ (end - begin);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
