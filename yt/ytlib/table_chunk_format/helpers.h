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
    Y_ASSERT(valueIndex < row.GetCount());
    return row[valueIndex];
}

inline const NTableClient::TUnversionedValue& GetUnversionedValue(NTableClient::TVersionedRow row, int valueIndex)
{
    Y_ASSERT(valueIndex < row.GetKeyCount());
    return row.BeginKeys()[valueIndex];
}

inline NTableClient::TUnversionedValue& GetUnversionedValue(NTableClient::TMutableUnversionedRow row, int valueIndex)
{
    Y_ASSERT(valueIndex < row.GetCount());
    return row[valueIndex];
}

inline NTableClient::TUnversionedValue& GetUnversionedValue(NTableClient::TMutableVersionedRow row, int valueIndex)
{
    Y_ASSERT(valueIndex < row.GetKeyCount());
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
int CompareValues(const NTableClient::TUnversionedValue& lhs, const NTableClient::TUnversionedValue& rhs);

template <>
inline int CompareValues<NTableClient::EValueType::Int64>(
    const NTableClient::TUnversionedValue& lhs,
    const NTableClient::TUnversionedValue& rhs)
{
    return CompareTypedValues<i64>(lhs, rhs);
}

template <>
inline int CompareValues<NTableClient::EValueType::Uint64>(
    const NTableClient::TUnversionedValue& lhs,
    const NTableClient::TUnversionedValue& rhs)
{
    return CompareTypedValues<ui64>(lhs, rhs);
}

template <>
inline int CompareValues<NTableClient::EValueType::Double>(
    const NTableClient::TUnversionedValue& lhs,
    const NTableClient::TUnversionedValue& rhs)
{
    return CompareTypedValues<double>(lhs, rhs);
}

template <>
inline int CompareValues<NTableClient::EValueType::String>(
    const NTableClient::TUnversionedValue& lhs,
    const NTableClient::TUnversionedValue& rhs)
{
    return CompareTypedValues<TStringBuf>(lhs, rhs);
}

template <>
inline int CompareValues<NTableClient::EValueType::Boolean>(
    const NTableClient::TUnversionedValue& lhs,
    const NTableClient::TUnversionedValue& rhs)
{
    return CompareTypedValues<bool>(lhs, rhs);
}

template<>
inline int CompareValues<NTableClient::EValueType::Any>(
    const NTableClient::TUnversionedValue& lhs,
    const NTableClient::TUnversionedValue& rhs)
{
    return NTableClient::CompareRowValues(lhs, rhs);
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

    YCHECK(beginIt != endIt);
    YCHECK(std::distance(beginIt, endIt) == 1);

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
