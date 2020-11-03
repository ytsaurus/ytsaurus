#include "key_bound.h"

namespace NYT::NTableClient {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

//! Used only for YT_LOG_FATAL below.
static const TLogger Logger("TableClientKey");

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::FromRow(const TRow& row, bool isInclusive, bool isUpper)
{
    ValidateValueTypes(row);
    TKeyBound result;
    result.Prefix = row;
    result.IsInclusive = isInclusive;
    result.IsUpper = isUpper;
    return result;
}

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::FromRow(TRow&& row, bool isInclusive, bool isUpper)
{
    ValidateValueTypes(row);
    TKeyBound result;
    result.Prefix = row;
    result.IsInclusive = isInclusive;
    result.IsUpper = isUpper;
    return result;
}

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::FromRowUnchecked(const TRow& row, bool isInclusive, bool isUpper)
{
#ifndef NDEBUG
    try {
        ValidateValueTypes(row);
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unexpected exception while building key bound from row");
    }
#endif

    TKeyBound result;
    result.Prefix = row;
    result.IsInclusive = isInclusive;
    result.IsUpper = isUpper;
    return result;
}

template <class TRow, class TKeyBound>
TKeyBound TKeyBoundImpl<TRow, TKeyBound>::FromRowUnchecked(TRow&& row, bool isInclusive, bool isUpper)
{
#ifndef NDEBUG
    try {
        ValidateValueTypes(row);
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Unexpected exception while building key bound from row");
    }
#endif

    TKeyBound result;
    result.Prefix = row;
    result.IsInclusive = isInclusive;
    result.IsUpper = isUpper;
    return result;
}

template <class TRow, class TKeyBound>
void TKeyBoundImpl<TRow, TKeyBound>::ValidateValueTypes(const TRow& row)
{
    for (const auto& value : row) {
        ValidateDataValueType(value.Type);
    }
}

template <class TRow, class TKeyBound>
template <class TKeyClass>
bool TKeyBoundImpl<TRow, TKeyBound>::TestKey(const TKeyClass& key) const
{
    YT_LOG_FATAL_IF(
        key.GetCount() < Prefix.GetCount(),
        "Key bound is tested against longer key (Key: %v, Prefix: %v)",
        key.AsRow(),
        Prefix);

    auto comparisonResult = CompareRows(key.AsRow(), Prefix, Prefix.GetCount());

    if (IsUpper && IsInclusive) {
        return comparisonResult <= 0;
    } else if (IsUpper && !IsInclusive) {
        return comparisonResult < 0;
    } else if (!IsUpper && IsInclusive) {
        return comparisonResult >= 0;
    } else /* if (!IsUpper && !IsInclusive) */ {
        return comparisonResult > 0;
    }
}

template <class TRow, class TKeyBound>
bool TKeyBoundImpl<TRow, TKeyBound>::operator==(const TKeyBoundImpl<TRow, TKeyBound>& other) const
{
    return Prefix == other.Prefix && IsInclusive == other.IsInclusive && IsUpper == other.IsUpper;
}

template <class TRow, class TKeyBound>
void TKeyBoundImpl<TRow, TKeyBound>::FormatValue(TStringBuilderBase* builder) const
{
    builder->AppendChar(IsUpper ? '<' : '>');
    if (IsInclusive) {
        builder->AppendChar('=');
    }
    builder->AppendFormat("%v", Prefix);
}

////////////////////////////////////////////////////////////////////////////////

template class TKeyBoundImpl<TUnversionedRow, TKeyBound>;
template class TKeyBoundImpl<TUnversionedOwningRow, TOwningKeyBound>;

template bool TKeyBoundImpl<TUnversionedRow, TKeyBound>::TestKey(const TKey& key) const;
template bool TKeyBoundImpl<TUnversionedRow, TKeyBound>::TestKey(const TOwningKey& key) const;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TOwningKeyBound::operator TKeyBound() const
{
    TKeyBound result;
    result.Prefix = Prefix;
    result.IsInclusive = IsInclusive;
    result.IsUpper = IsUpper;
    return result;
}

void FormatValue(TStringBuilderBase* builder, const TOwningKeyBound& keyBound, TStringBuf /*format*/)
{
    return keyBound.FormatValue(builder);
}

TString ToString(const TOwningKeyBound& keyBound)
{
    return ToStringViaBuilder(keyBound);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TKeyBound& keyBound, TStringBuf /*format*/)
{
    return keyBound.FormatValue(builder);
}

TString ToString(const TKeyBound& keyBound)
{
    return ToStringViaBuilder(keyBound);
}

////////////////////////////////////////////////////////////////////////////////

TOwningKeyBound KeyBoundFromLegacyRow(TUnversionedRow row, bool isUpper, int keyLength)
{
    // Flag indicating that row starts with #keyLength non-sentinel values followed by at least one arbitrary value.
    bool isLongRow = false;

    // If row contains at least one sentinel on first #keyLength positions, type of leftmost of them.
    std::optional<EValueType> leftmostSentinelType;

    // Builder for the longest prefix of row which is free of sentinels. Prefix length is limited by #keyLength.
    TUnversionedOwningRowBuilder prefixBuilder;
    for (int index = 0; index < row.GetCount() && index <= keyLength; ++index) {
        if (index == keyLength) {
            isLongRow = true;
            break;
        }
        if (row[index].Type != EValueType::Min && row[index].Type != EValueType::Max) {
            prefixBuilder.AddValue(row[index]);
        } else {
            leftmostSentinelType = row[index].Type;
            break;
        }
    }

    // When dealing with unversioned rows, upper limit is always exclusive and lower limit is always inclusive.
    // We will call this kind of inclusiveness standard. This implies following cases for key bounds.
    //
    // (A) If row is long, upper limit will be inclusive and lower limit will be exclusive, i.e. inclusiveness is toggled.
    // (B) Otherwise, if row has exactly length of #keyLength and does not contain sentinels, inclusiveness is standard.
    //
    // Suppose none of (A) and (B) happened. We know that prefix is strictly shorter than #keyLength. If may or may not be
    // followed by a sentinel. Actually there is no difference if prefix is followed by Min or if it is not followed by sentinel.
    // To prove this fact, consider row R = prefix + [Min], length(R) < #keyLength and key K, length(K) == #keyLength.
    // It is easy to see that R is compared to K in exactly the same way as prefix is compared to K; this case
    // corresponds to a key bound with standard inclusiveness.
    //
    // Similar argument shows that if prefix is followed by Max, key bound inclusiveness should be toggled.
    //
    // So, we have only two more cases:
    //
    // (C) Otherwise, if prefix is followed by Min or no sentinel, inclusiveness is standard.
    // (D) Otherwise (prefix is followed by Max), inclusiveness is toggled.

    // Cases (A) and (D).
    bool toggleInclusiveness = isLongRow || leftmostSentinelType == EValueType::Max;

    bool isInclusive = (isUpper && toggleInclusiveness) || (!isUpper && !toggleInclusiveness);

    TOwningKeyBound result;
    result.Prefix = prefixBuilder.FinishRow();
    result.IsUpper = isUpper;
    result.IsInclusive = isInclusive;
    return result;
}

TUnversionedOwningRow KeyBoundToLegacyRow(TKeyBound keyBound)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& value : keyBound.Prefix) {
        builder.AddValue(value);
    }
    if ((keyBound.IsUpper && keyBound.IsInclusive) || (!keyBound.IsUpper && !keyBound.IsInclusive)) {
        builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
    }
    return builder.FinishRow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
