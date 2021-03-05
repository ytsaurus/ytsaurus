#include "unversioned_value.h"

#include <yt/yt/core/misc/farm_hash.h>


#ifndef YT_COMPILING_UDF

#include "unversioned_row.h"
#include "composite_compare.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/format.h>

#include <yt/yt/core/ytree/convert.h>

#endif

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

ui64 GetHash(const TUnversionedValue& value)
{
    if (value.Type == EValueType::Composite) {
        // NB: Composite types doesn't support FarmHash yet.
        return CompositeHash(TStringBuf(value.Data.String, value.Length));
    } else {
        // NB: hash function may change in future. Use fingerprints for persistent hashing.
        return GetFarmFingerprint(value);
    }
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
            YT_ABORT();
#else
            THROW_ERROR_EXCEPTION(
                EErrorCode::UnhashableType,
                "Cannot hash values of type %Qlv; only scalar types are allowed for key columns",
                type)
                << TErrorAttribute("value", value);
#endif

    }
}

TFingerprint GetHash(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    ui64 result = 0xdeadc0de;
    for (const auto* value = begin; value < end; ++value) {
        result = FarmFingerprint(result, GetHash(*value));
    }
    return result ^ (end - begin);
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

void PrintTo(const TUnversionedValue& value, ::std::ostream* os)
{
    *os << ToString(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
