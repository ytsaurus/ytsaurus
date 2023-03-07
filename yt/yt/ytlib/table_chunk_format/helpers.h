#pragma once

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/versioned_row.h>
#include <yt/ytlib/table_client/helpers.h>

#include <yt/core/misc/chunked_output_stream.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
typename std::enable_if<std::is_signed<TValue>::value, TValue>::type
GetValue(const NTableClient::TUnversionedValue& value)
{
    return value.Data.Int64;
}

template <class TValue>
typename std::enable_if<std::is_unsigned<TValue>::value, TValue>::type
GetValue(const NTableClient::TUnversionedValue& value)
{
    return value.Data.Uint64;
}

////////////////////////////////////////////////////////////////////////////////

inline const NTableClient::TUnversionedValue& GetUnversionedValue(NTableClient::TUnversionedRow row, int valueIndex)
{
    YT_ASSERT(valueIndex < row.GetCount());
    return row[valueIndex];
}

inline const NTableClient::TUnversionedValue& GetUnversionedValue(NTableClient::TVersionedRow row, int valueIndex)
{
    YT_ASSERT(valueIndex < row.GetKeyCount());
    return row.BeginKeys()[valueIndex];
}

inline NTableClient::TUnversionedValue& GetUnversionedValue(NTableClient::TMutableUnversionedRow row, int valueIndex)
{
    YT_ASSERT(valueIndex < row.GetCount());
    return row[valueIndex];
}

inline NTableClient::TUnversionedValue& GetUnversionedValue(NTableClient::TMutableVersionedRow row, int valueIndex)
{
    YT_ASSERT(valueIndex < row.GetKeyCount());
    return row.BeginKeys()[valueIndex];
}

////////////////////////////////////////////////////////////////////////////////

inline ui32 GetUnversionedValueCount(const NTableClient::TUnversionedRow row)
{
    return row.GetCount();
}

inline ui32 GetUnversionedValueCount(const NTableClient::TVersionedRow row)
{
    return row.GetKeyCount();
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
int CompareTypedValues(const NTableClient::TUnversionedValue& lhs, const NTableClient::TUnversionedValue& rhs)
{
    if (Y_UNLIKELY(lhs.Type != rhs.Type)) {
        return static_cast<int>(lhs.Type) - static_cast<int>(rhs.Type);
    }

    if (rhs.Type == NTableClient::EValueType::Null) {
        return 0;
    }

    TValue lhsValue;
    GetValue(&lhsValue, lhs);

    TValue rhsValue;
    GetValue(&rhsValue, rhs);

    if (lhsValue < rhsValue) {
        return -1;
    } else if (lhsValue > rhsValue) {
        return +1;
    } else {
        return 0;
    }
}

//! Compare two unversioned values of the same type (or null).
template <NTableClient::EValueType valueType>
int CompareValues(const NTableClient::TUnversionedValue& lhs, const NTableClient::TUnversionedValue& rhs)
{
    using namespace NTableClient;
    if constexpr (valueType == EValueType::Int64) {
        return CompareTypedValues<i64>(lhs, rhs);
    } else if constexpr (valueType == EValueType::Uint64) {
        return CompareTypedValues<ui64>(lhs, rhs);
    } else if constexpr (valueType == EValueType::Double) {
        return CompareTypedValues<double>(lhs, rhs);
    } else if constexpr (valueType == EValueType::String) {
        return CompareTypedValues<TStringBuf>(lhs, rhs);
    } else if constexpr (valueType == EValueType::Boolean) {
        return CompareTypedValues<bool>(lhs, rhs);
    } else if constexpr (valueType == EValueType::Any || valueType == EValueType::Composite) {
        return CompareRowValues(lhs, rhs);
    } else if constexpr (valueType == EValueType::Null) {
        // Nulls are always equal
        return 0;
    } else {
        // Poor man static_assert(false, ...).
        static_assert(valueType == EValueType::Int64, "Unexpected value type");
    }
}

////////////////////////////////////////////////////////////////////////////////

inline ui32 GetTimestampIndex(
    const NTableClient::TVersionedValue& value,
    const NTableClient::TVersionedRow row)
{
    const NTableClient::TTimestamp* beginIt;
    const NTableClient::TTimestamp* endIt;
    std::tie(beginIt, endIt) = std::equal_range(
        row.BeginWriteTimestamps(),
        row.EndWriteTimestamps(),
        value.Timestamp,
        [](const NTableClient::TTimestamp t1, const NTableClient::TTimestamp t2) {
            return t1 > t2;
        });

    YT_VERIFY(beginIt != endIt);
    YT_VERIFY(std::distance(beginIt, endIt) == 1);

    return std::distance(row.BeginWriteTimestamps(), beginIt);
}

////////////////////////////////////////////////////////////////////////////////

inline TRange<NTableClient::TVersionedValue> FindValues(const NTableClient::TVersionedRow row, int columnId)
{
    auto lower = std::lower_bound(
        row.BeginValues(),
        row.EndValues(),
        columnId,
        [](const NTableClient::TVersionedValue& value, int columnId) {
            return value.Id < columnId;
        });

    auto upper = std::upper_bound(
        row.BeginValues(),
        row.EndValues(),
        columnId,
        [](int columnId, const NTableClient::TVersionedValue& value) {
            return columnId < value.Id;
        });

    return MakeRange(lower, upper);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
