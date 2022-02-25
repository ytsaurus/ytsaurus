#include "unversioned_value.h"


#ifndef YT_COMPILING_UDF

#include "unversioned_row.h"
#include "composite_compare.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/format.h>

#include <yt/yt/core/ytree/convert.h>

#endif

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TStringBuf TUnversionedValue::AsStringBuf() const
{
    return TStringBuf(Data.String, Length);
}

TString TUnversionedValue::AsString() const
{
    return TString(Data.String, Length);
}

ui64 GetHash(const TUnversionedValue& value)
{
    if (value.Type == EValueType::Composite) {
        // NB: Composite types doesn't support FarmHash yet.
        return CompositeHash(value.AsStringBuf());
    } else {
        // NB: hash function may change in future. Use fingerprints for persistent hashing.
        return GetFarmFingerprint(value);
    }
}

// Forever-fixed Google FarmHash fingerprint.
TFingerprint FarmFingerprint(const TUnversionedValue& value)
{
    auto type = value.Type;
    switch (type) {
        case EValueType::String:
            return NYT::FarmFingerprint(value.Data.String, value.Length);

        case EValueType::Int64:
        case EValueType::Uint64:
        case EValueType::Double:
            // These types are aliased.
            return NYT::FarmFingerprint(value.Data.Int64);

        case EValueType::Boolean:
            return NYT::FarmFingerprint(value.Data.Boolean);

        case EValueType::Null:
            return NYT::FarmFingerprint(0);

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

TFingerprint GetFarmFingerprint(const TUnversionedValue& value)
{
    return FarmFingerprint(value);
}

TFingerprint GetHash(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    ui64 result = 0xdeadc0de;
    for (const auto* value = begin; value < end; ++value) {
        result = NYT::FarmFingerprint(result, GetHash(*value));
    }
    return result ^ (end - begin);
}

TFingerprint GetFarmFingerprint(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    return NYT::FarmFingerprint<TUnversionedValue>(begin, end);
}

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TUnversionedValue& value, ::std::ostream* os)
{
    *os << ToString(value);
}

////////////////////////////////////////////////////////////////////////////////

void AppendWithCut(TStringBuilderBase* builder, TStringBuf string)
{
    constexpr auto Cutoff = 128;
    if (string.size() <= 2 * Cutoff + 3) {
        builder->AppendString(string);
    } else {
        builder->AppendString(string.substr(0, Cutoff));
        builder->AppendString("...");
        builder->AppendString(string.substr(string.size() - Cutoff, Cutoff));
    }
}

void FormatValue(TStringBuilderBase* builder, const TUnversionedValue& value, TStringBuf format)
{
    using NTableClient::EValueFlags;
    using NTableClient::EValueType;

    bool noFlags = false;
    for (char c : format) {
        noFlags |= c == 'k';
    }

    if (!noFlags) {
        if (Any(value.Flags & EValueFlags::Aggregate)) {
            builder->AppendChar('%');
        }
        if (Any(value.Flags & EValueFlags::Hunk)) {
            builder->AppendChar('&');
        }
        builder->AppendFormat("%v#", value.Id);
    }
    switch (value.Type) {
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            builder->AppendFormat("<%v>", value.Type);
            break;

        case EValueType::Int64:
            builder->AppendFormat("%v", value.Data.Int64);
            break;

        case EValueType::Uint64:
            builder->AppendFormat("%vu", value.Data.Uint64);
            break;

        case EValueType::Double:
            builder->AppendFormat("%v", value.Data.Double);
            break;

        case EValueType::Boolean:
            builder->AppendFormat("%v", value.Data.Boolean);
            break;

        case EValueType::String: {
            builder->AppendChar('"');
            AppendWithCut(builder, value.AsStringBuf());
            builder->AppendChar('"');
            break;
        }

        case EValueType::Any:
        case EValueType::Composite: {
            if (value.Type == EValueType::Composite) {
                // ermolovd@ says "composites" are comparable, in contrast to "any".
                builder->AppendString("><");
            }

            auto compositeString = ConvertToYsonString(
                NYson::TYsonString(value.AsString()),
                NYson::EYsonFormat::Text);

            AppendWithCut(builder, compositeString.AsStringBuf());
            break;
        }
    }
}

TString ToString(const TUnversionedValue& value, bool valueOnly)
{
    return ToStringViaBuilder(value, valueOnly ? "k" : "");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
