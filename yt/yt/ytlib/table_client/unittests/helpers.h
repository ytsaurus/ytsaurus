#pragma once

#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/unversioned_row_batch.h>
#include <yt/client/table_client/versioned_row.h>

#include <yt/core/misc/algorithm_helpers.h>

#include <iostream>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void ExpectSchemafulRowsEqual(TUnversionedRow expected, TUnversionedRow actual);

void ExpectSchemalessRowsEqual(TUnversionedRow expected, TUnversionedRow actual, int keyColumnCount);

void ExpectSchemafulRowsEqual(TVersionedRow expected, TVersionedRow actual);

void CheckResult(std::vector<TVersionedRow>* expected, IVersionedReaderPtr reader);

template <class TExpectedRow, class TActualRow>
void CheckSchemafulResult(const std::vector<TExpectedRow>& expected, const std::vector<TActualRow>& actual)
{
    ASSERT_EQ(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); ++i) {
        ExpectSchemafulRowsEqual(expected[i], actual[i]);
    }
}

template <class TExpectedRow, class TActualRow>
void CheckSchemalessResult(
    TRange<TExpectedRow> expected,
    TRange<TActualRow> actual,
    int keyColumnCount)
{
    ASSERT_EQ(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); ++i) {
        ExpectSchemalessRowsEqual(expected[i], actual[i], keyColumnCount);
    }
}

template <class TRow, class TReader>
void CheckSchemalessResult(const std::vector<TRow>& expected, TIntrusivePtr<TReader> reader, int keyColumnCount)
{
    size_t offset = 0;
    while (auto batch = reader->Read()) {
        auto actual = batch->MaterializeRows();
        if (actual.empty()) {
            ASSERT_TRUE(reader->GetReadyEvent().Get().IsOK());
            continue;
        }

        CheckSchemalessResult(
            MakeRange(expected).Slice(offset, std::min(expected.size(), offset + actual.size())),
            actual,
            keyColumnCount);
        offset += actual.size();
    }
}

std::vector<std::pair<ui32, ui32>> GetTimestampIndexRanges(
    TRange<NTableClient::TVersionedRow> rows,
    NTableClient::TTimestamp timestamp);

template <class T>
void AppendVector(std::vector<T>* data, const std::vector<T> toAppend)
{
    data->insert(data->end(), toAppend.begin(), toAppend.end());
}

template <class T>
TRange<T> GetTypedData(const NTableClient::IUnversionedRowBatch::TValueBuffer& buffer)
{
    return MakeRange(
        reinterpret_cast<const T*>(buffer.Data.Begin()),
        reinterpret_cast<const T*>(buffer.Data.End()));
}

inline bool GetBit(const NTableClient::IUnversionedRowBatch::TValueBuffer& buffer, int index)
{
    return (buffer.Data[index / 8] & (1 << (index % 8))) != 0;
}

inline bool GetBit(const NTableClient::IUnversionedRowBatch::TBitmap& bitmap, int index)
{
    return (bitmap.Data[index / 8] & (1 << (index % 8))) != 0;
}

inline void ResolveRleEncoding(
    const NTableClient::IUnversionedRowBatch::TColumn*& column,
    i64& index)
{
    if (!column->Rle) {
        return;
    }
    
    YT_ASSERT(column->Values->BitWidth == 64);
    YT_ASSERT(!column->Values->ZigZagEncoded);
    auto rleIndexes = GetTypedData<ui64>(*column->Values);
    YT_ASSERT(rleIndexes[0] == 0);

    index = BinarySearch(
        static_cast<i64>(0),
        static_cast<i64>(rleIndexes.size()),
        [&] (i64 k) {
            return rleIndexes[k] <= index;
        }) - 1;
    column = column->Rle->ValueColumn;
}

inline void ResolveDictionaryEncoding(
    const NTableClient::IUnversionedRowBatch::TColumn*& column,
    i64& index)
{
    if (!column->Dictionary) {
        return;
    }

    const auto& dictionary = *column->Dictionary;
    YT_ASSERT(dictionary.ZeroMeansNull);
    YT_ASSERT(column->Values->BitWidth == 32);
    YT_ASSERT(!column->Values->ZigZagEncoded);
    index = static_cast<i64>(GetTypedData<ui32>(*column->Values)[index]) - 1;
    column = column->Dictionary->ValueColumn;
}

inline TStringBuf DecodeStringFromColumn(
    const NTableClient::IUnversionedRowBatch::TColumn& column,
    i64 index)
{
    const auto& strings = *column.Strings;
    YT_ASSERT(strings.AvgLength);
    YT_ASSERT(column.Values->BitWidth == 32);
    YT_ASSERT(column.Values->ZigZagEncoded);

    auto getOffset = [&] (i64 index) {
        return  (index == 0)
            ? 0
            : *strings.AvgLength * index + ZigZagDecode64(GetTypedData<ui32>(*column.Values)[index - 1]);
    };

    i64 offset = getOffset(index);
    i64 nextOffset = getOffset(index + 1);
    return TStringBuf(strings.Data.Begin() + offset, strings.Data.Begin() + nextOffset);
}

template <class T>
T DecodeIntegerFromColumn(
    const NTableClient::IUnversionedRowBatch::TColumn& column,
    i64 index)
{
    YT_ASSERT(column.Values->BitWidth == 64);
    auto value = GetTypedData<ui64>(*column.Values)[index];
    value += column.Values->BaseValue;
    if (column.Values->ZigZagEncoded) {
        value = static_cast<ui64>(ZigZagDecode64(value));
    }
    return static_cast<T>(value);
}

inline double DecodeDoubleFromColumn(
    const NTableClient::IUnversionedRowBatch::TColumn& column,
    i64 index)
{
    YT_ASSERT(column.Values->BitWidth == 64);
    return GetTypedData<double>(*column.Values)[index];
}

inline bool DecodeBoolFromColumn(
    const NTableClient::IUnversionedRowBatch::TColumn& column,
    i64 index)
{
    YT_ASSERT(column.Values->BitWidth == 1);
    return GetBit(*column.Values, index);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

