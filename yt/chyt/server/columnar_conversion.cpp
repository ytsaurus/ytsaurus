#include "columnar_conversion.h"

#include "config.h"

#include <yt/yt/client/table_client/columnar.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/clickhouse_functions/unescaped_yson.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNothing.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <EExtendedYsonFormat ysonFormat, class F>
DB::ColumnString::MutablePtr ConvertCHColumnToAnyByIndexImpl(const DB::IColumn& column, F func)
{
    TString ysonBuffer;
    TStringOutput ysonOutput(ysonBuffer);

    // Using IIFE for constexpr resolution of the writer (for binary YSON we use TBufferedBinaryYsonWriter,
    // for non-binary we use TExtendedYsonWriter).
    auto ysonWriter = [&] {
        if constexpr (ysonFormat == EExtendedYsonFormat::Binary) {
            return TBufferedBinaryYsonWriter(&ysonOutput);
        } else {
            return TExtendedYsonWriter(&ysonOutput, ysonFormat);
        }
    }();

    auto anyColumn = DB::ColumnString::create();
    auto& offsets = anyColumn->getOffsets();
    auto& chars = anyColumn->getChars();

    for (size_t index = 0; index < column.size(); ++index) {
        ysonBuffer.clear();
        func(index, &ysonWriter);
        ysonWriter.Flush();
        chars.insert(chars.end(), ysonBuffer.begin(), ysonBuffer.end());
        chars.push_back('\x0');
        offsets.push_back(chars.size());
    }

    return anyColumn;
}

template <EExtendedYsonFormat ysonFormat, class T, class F>
DB::ColumnString::MutablePtr ConvertCHVectorColumnToAnyImpl(const DB::IColumn& column, F func)
{
    const auto* typedColumnPtr = dynamic_cast<const DB::ColumnVector<T>*>(&column);
    YT_VERIFY(typedColumnPtr);
    const auto& typedValues = typedColumnPtr->getData();

    return ConvertCHColumnToAnyByIndexImpl<ysonFormat>(
        column,
        [&] (size_t index, auto* writer) {
            auto value = typedValues[index];
            func(value, writer);
        });
}

template <EExtendedYsonFormat ysonFormat, class F>
DB::ColumnString::MutablePtr ConvertCHStringColumnToAnyImpl(const DB::IColumn& column, F func)
{
    const auto* typedColumnPtr = dynamic_cast<const DB::ColumnString*>(&column);
    YT_VERIFY(typedColumnPtr);

    return ConvertCHColumnToAnyByIndexImpl<ysonFormat>(
        column,
        [&] (size_t index, auto* writer) {
            auto value = typedColumnPtr->getDataAt(index);
            func(TStringBuf(value.data, value.size), writer);
        });
}

template <EExtendedYsonFormat ysonFormat, class F>
DB::ColumnString::MutablePtr ConvertCHDateTime64ColumnToAnyImpl(const DB::IColumn& column, F func)
{
    const auto* typedColumnPtr = dynamic_cast<const DB::ColumnDecimal<DB::DateTime64>*>(&column);
    YT_VERIFY(typedColumnPtr);
    const auto& typedValues = typedColumnPtr->getData();

    return ConvertCHColumnToAnyByIndexImpl<ysonFormat>(
        column,
        [&] (size_t index, auto* writer) {
            auto value = typedValues[index];
            func(value, writer);
        });
}

DB::ColumnString::MutablePtr ConvertCHNothingColumnToAnyImpl(const DB::IColumn& column)
{
    auto valueCount = column.size();
    auto chColumn = DB::ColumnString::create();
    chColumn->insertManyDefaults(valueCount);
    return chColumn;
}

template <EExtendedYsonFormat ysonFormat>
DB::ColumnString::MutablePtr ConvertCHColumnToAnyImpl(const DB::IColumn& column, ESimpleLogicalValueType type)
{
    YT_LOG_TRACE("Converting column to any (Count: %v, Type: %v)",
        column.size(),
        type);

    switch (type) {
        #define XX(valueType, cppType, method) \
            case ESimpleLogicalValueType::valueType: \
                return ConvertCHVectorColumnToAnyImpl<ysonFormat, cppType>( \
                    column, \
                    [] (cppType value, auto* writer) { writer->method(value); });
        XX(Int8,        DB::Int8, OnInt64Scalar)
        XX(Int16,       i16, OnInt64Scalar)
        XX(Int32,       i32, OnInt64Scalar)
        XX(Date32,      i32, OnInt64Scalar)
        XX(Int64,       i64, OnInt64Scalar)
        XX(Interval,    i64, OnInt64Scalar)
        XX(Interval64,  i64, OnInt64Scalar)

        XX(Uint8,     DB::UInt8, OnUint64Scalar)
        XX(Uint16,    ui16, OnUint64Scalar)
        XX(Uint32,    ui32, OnUint64Scalar)
        XX(Uint64,    ui64, OnUint64Scalar)
        XX(Date,      ui16, OnUint64Scalar)
        XX(Datetime,  ui32, OnUint64Scalar)
        #undef XX

        #define XX(chType, cppType) \
            case ESimpleLogicalValueType::chType: \
                return ConvertCHVectorColumnToAnyImpl<ysonFormat, cppType>( \
                    column, \
                    [] (cppType value, auto* writer) { writer->OnDoubleScalar(value); });
        XX(Float,  float )
        XX(Double, double)
        #undef XX

        #define XX(valueType, cppType, method) \
            case ESimpleLogicalValueType::valueType: \
                return ConvertCHDateTime64ColumnToAnyImpl<ysonFormat>( \
                    column, \
                    [] (cppType value, auto* writer) { writer->method(value); });
        XX(Timestamp, ui64, OnUint64Scalar)
        XX(Datetime64, i64, OnInt64Scalar)
        XX(Timestamp64, i64, OnInt64Scalar)
        #undef XX

        case ESimpleLogicalValueType::Boolean:
            return ConvertCHVectorColumnToAnyImpl<ysonFormat, DB::UInt8>(
                column,
                [] (DB::UInt8 value, auto* writer) { writer->OnBooleanScalar(value != 0); });

        case ESimpleLogicalValueType::String:
            return ConvertCHStringColumnToAnyImpl<ysonFormat>(
                column,
                [] (TStringBuf value, auto* writer) { writer->OnStringScalar(value); });

        case ESimpleLogicalValueType::Null:
        case ESimpleLogicalValueType::Void:
            return ConvertCHNothingColumnToAnyImpl(column);

        default:
            THROW_ERROR_EXCEPTION("Cannot convert CH column to %Qlv type",
                ESimpleLogicalValueType::Any);
    }
}

template <class TColumn, class... Args>
DB::MutableColumnPtr ConvertIntegerYTColumnToCHColumnImpl(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn,
    const IUnversionedColumnarRowBatch::TColumn& ytValueColumn,
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    Args&&... args)
{
    auto chColumn = TColumn::create(ytColumn.ValueCount, std::forward<Args>(args)...);
    auto* currentOutput = chColumn->getData().data();

    auto values = ytValueColumn.GetTypedValues<ui64>();

    DecodeIntegerVector(
        ytColumn.StartIndex,
        ytColumn.StartIndex + ytColumn.ValueCount,
        ytValueColumn.Values->BaseValue,
        ytValueColumn.Values->ZigZagEncoded,
        dictionaryIndexes,
        rleIndexes,
        [&] (auto index) {
            return values[index];
        },
        [&] (auto value) {
            *currentOutput++ = value;
        });

    return chColumn;
}

auto AnalyzeColumnEncoding(const IUnversionedColumnarRowBatch::TColumn& ytColumn)
{
    TRange<ui64> rleIndexes;
    TRange<ui32> dictionaryIndexes;
    const IUnversionedColumnarRowBatch::TColumn* ytValueColumn = &ytColumn;

    if (ytValueColumn->Rle) {
        YT_VERIFY(ytValueColumn->Values);
        YT_VERIFY(ytValueColumn->Values->BaseValue == 0);
        YT_VERIFY(ytValueColumn->Values->BitWidth == 64);
        YT_VERIFY(!ytValueColumn->Values->ZigZagEncoded);
        rleIndexes = ytValueColumn->GetTypedValues<ui64>();
        ytValueColumn = ytValueColumn->Rle->ValueColumn;
    }

    if (ytValueColumn->Dictionary) {
        YT_VERIFY(ytValueColumn->Values);
        YT_VERIFY(ytValueColumn->Values->BaseValue == 0);
        YT_VERIFY(ytValueColumn->Values->BitWidth == 32);
        YT_VERIFY(!ytValueColumn->Values->ZigZagEncoded);
        dictionaryIndexes = ytValueColumn->GetTypedValues<ui32>();
        ytValueColumn = ytValueColumn->Dictionary->ValueColumn;
    }

    return std::tuple(
        ytValueColumn,
        rleIndexes,
        dictionaryIndexes);
}

template <class T>
    requires
        std::is_same_v<T, float> ||
        std::is_same_v<T, double>
DB::MutableColumnPtr ConvertFloatingPointYTColumnToCHColumn(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn)
{
    auto chColumn = DB::ColumnVector<T>::create(ytColumn.ValueCount);
    auto& chData = chColumn->getData();

    if (std::is_same_v<T, double> && ytColumn.Values->BitWidth == sizeof(float) * 8) {
        // Need to convert float to double.
        // TODO(dakovalkov): It's not optimal.
        auto relevantValues = ytColumn.GetRelevantTypedValues<float>();

        for (i64 index = 0; index < ytColumn.ValueCount; ++index) {
            chData[index] = static_cast<double>(relevantValues[index]);
        }
    } else {
        auto relevantValues = ytColumn.GetRelevantTypedValues<T>();

        ::memcpy(
            chData.data(),
            relevantValues.Begin(),
            sizeof(T) * ytColumn.ValueCount);
    }

    return chColumn;
}

i64 CountTotalStringLengthInRleDictionaryIndexesWithZeroNull(
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    TRange<i32> stringLengths,
    i64 startIndex,
    i64 endIndex)
{
    YT_VERIFY(startIndex >= 0 && startIndex <= endIndex);
    YT_VERIFY(rleIndexes[0] == 0);

    auto startRleIndex = TranslateRleStartIndex(rleIndexes, startIndex);
    const auto* currentInput = dictionaryIndexes.Begin() + startRleIndex;
    auto currentIndex = startIndex;
    auto currentRleIndex = startRleIndex;
    i64 result = 0;
    while (currentIndex < endIndex) {
        ++currentRleIndex;
        auto thresholdIndex = currentRleIndex < static_cast<i64>(rleIndexes.Size()) ? static_cast<i64>(rleIndexes[currentRleIndex]) : Max<i64>();
        auto currentDictionaryIndex = *currentInput++;
        auto newIndex = std::min(endIndex, thresholdIndex);
        if (currentDictionaryIndex != 0) {
            result += (newIndex - currentIndex) * stringLengths[currentDictionaryIndex - 1];
        }
        currentIndex = newIndex;
    }
    return result;
}

i64 CountTotalStringLengthWithFilterHint(
    TRange<ui32> ytOffsets,
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    ui32 avgLength,
    i64 startIndex,
    i64 endIndex,
    TRange<DB::UInt8> filterHint)
{
    i64 totalStringLength = 0;
    int rowIndex = 0;

    DecodeRawVector<i32>(
            startIndex,
            endIndex,
            dictionaryIndexes,
            rleIndexes,
            [&] (i64 offsetIndex) {
                auto [startOffset, endOffset] = DecodeStringRange(ytOffsets, avgLength, offsetIndex);
                return endOffset - startOffset;
            },
            [&] (i32 length) {
                if (filterHint[rowIndex++]) {
                    totalStringLength += length;
                }
            });

    return totalStringLength;
}

DB::ColumnString::MutablePtr ConvertStringLikeYTColumnToCHColumnImpl(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn,
    TRange<DB::UInt8> filterHint)
{
    auto [ytValueColumn, rleIndexes, dictionaryIndexes] = AnalyzeColumnEncoding(ytColumn);

    YT_LOG_TRACE("Converting string-like column (Count: %v, Dictionary: %v, Rle: %v)",
        ytColumn.ValueCount,
        static_cast<bool>(dictionaryIndexes),
        static_cast<bool>(rleIndexes));

    YT_VERIFY(ytValueColumn->Values);
    YT_VERIFY(ytValueColumn->Values->BitWidth == 32);
    YT_VERIFY(ytValueColumn->Values->BaseValue == 0);
    YT_VERIFY(ytValueColumn->Values->ZigZagEncoded);
    YT_VERIFY(ytValueColumn->Strings);
    YT_VERIFY(ytValueColumn->Strings->AvgLength);

    auto ytOffsets = ytValueColumn->GetTypedValues<ui32>();
    const auto* ytChars = ytValueColumn->Strings->Data.Begin();

    YT_VERIFY(ytValueColumn->Strings->AvgLength);
    auto avgLength = *ytValueColumn->Strings->AvgLength;

    auto chColumn = DB::ColumnString::create();

    auto& chOffsets = chColumn->getOffsets();
    chOffsets.resize(ytColumn.ValueCount);
    auto* currentCHOffset = chOffsets.data() - 1;

    auto& chChars = chColumn->getChars();
    ui64 currentCHCharsPosition = 0;
    DB::UInt8* currentCHChar;
    size_t remainingCHCharsCapacity;

    auto initCHCharsCursor = [&] {
        currentCHChar = chChars.data() + currentCHCharsPosition;
        remainingCHCharsCapacity = chChars.size()
            - currentCHCharsPosition;
    };

    auto resizeCHChars = [&] (i64 size) {
        chChars.resize(size);
        initCHCharsCursor();
        YT_LOG_TRACE("String buffer resized (Size: %v)",
            chChars.size());
    };

    auto uncheckedConsumer = [&] (auto pair) {
        auto [str, length] = pair;
        *currentCHOffset++ = currentCHCharsPosition;
        memcpy(currentCHChar, str, length);
        currentCHChar += length;
        *currentCHChar++ = '\x0';
        currentCHCharsPosition += length + 1;
    };

    auto checkedConsumer = [&] (auto pair) {
        auto [str, length] = pair;
        if (Y_UNLIKELY(remainingCHCharsCapacity <= static_cast<size_t>(length))) {
            resizeCHChars(
                std::max(chChars.size() * 2, chChars.size() + (static_cast<size_t>(length) + 1)));
        }
        uncheckedConsumer(pair);
        remainingCHCharsCapacity -= (length + 1);
    };

    auto estimateAndResizeCHChars = [&] {
        resizeCHChars(
            // +1 is due to zero-terminated strings, *2 is to reduce the number of reallocations
            (avgLength + 1) * ytColumn.ValueCount * 2 +
            // some additive footprint
            1_KB);
    };

    if (filterHint) {
        YT_VERIFY(std::ssize(filterHint) == ytColumn.ValueCount);

        resizeCHChars(
            CountTotalStringLengthWithFilterHint(
                ytOffsets,
                dictionaryIndexes,
                rleIndexes,
                avgLength,
                ytColumn.StartIndex,
                ytColumn.StartIndex + ytColumn.ValueCount,
                filterHint) +
            ytColumn.ValueCount);

        int rowIndex = 0;
        DecodeRawVector<std::pair<const char*, i32>>(
            ytColumn.StartIndex,
            ytColumn.StartIndex + ytColumn.ValueCount,
            dictionaryIndexes,
            rleIndexes,
            [&] (i64 index) {
                    auto [startOffset, endOffset] = DecodeStringRange(ytOffsets, avgLength, index);
                    return std::pair(ytChars + startOffset, endOffset - startOffset);
                },
            [&] (auto pair) {
                if (filterHint[rowIndex++]) {
                    uncheckedConsumer(pair);
                } else {
                    uncheckedConsumer(std::pair{pair.first, 0});
                }
            });
    } else if (dictionaryIndexes) {
        // Check for small dictionary case: at least #SmallDictionaryFactor
        // occurrences of dictionary entries on average.
        constexpr int SmallDictionaryFactor = 3;
        if (static_cast<i64>(ytOffsets.Size()) * SmallDictionaryFactor < ytColumn.ValueCount) {
            YT_LOG_TRACE("Converting string column with small dictionary (Count: %v, DictionarySize: %v, Rle: %v)",
                ytColumn.ValueCount,
                ytOffsets.size(),
                static_cast<bool>(rleIndexes));

            // Let's decode string offsets and lengths.
            std::vector<const char*> ytStrings(ytOffsets.size());
            std::vector<i32> ytStringLengths(ytOffsets.size());
            DecodeStringPointersAndLengths(
                ytOffsets,
                avgLength,
                ytValueColumn->Strings->Data,
                TMutableRange(ytStrings),
                TMutableRange(ytStringLengths));

            auto stringsFetcher = [&] (i64 index) {
                return std::pair(ytStrings[index], ytStringLengths[index]);
            };

            if (rleIndexes) {
                // For run-length encoded strings it makes sense to precompute the total needed
                // string capacity to avoid reallocation checks on fast path below.
                resizeCHChars(
                    CountTotalStringLengthInRleDictionaryIndexesWithZeroNull(
                        dictionaryIndexes,
                        rleIndexes,
                        ytStringLengths,
                        ytColumn.StartIndex,
                        ytColumn.StartIndex + ytColumn.ValueCount) +
                    ytColumn.ValueCount);

                DecodeRawVector<std::pair<const char*, i32>>(
                    ytColumn.StartIndex,
                    ytColumn.StartIndex + ytColumn.ValueCount,
                    dictionaryIndexes,
                    rleIndexes,
                    stringsFetcher,
                    uncheckedConsumer);
            } else {
                estimateAndResizeCHChars();

                DecodeRawVector<std::pair<const char*, i32>>(
                    ytColumn.StartIndex,
                    ytColumn.StartIndex + ytColumn.ValueCount,
                    dictionaryIndexes,
                    rleIndexes,
                    stringsFetcher,
                    checkedConsumer);
            }
        } else {
            // Large dictionary (or, more likely, small read range): will decode each
            // dictionary reference separately.
            YT_LOG_TRACE("Converting string column with large dictionary (Count: %v, DictionarySize: %v, Rle: %v)",
                ytColumn.ValueCount,
                ytOffsets.size(),
                static_cast<bool>(rleIndexes));

            estimateAndResizeCHChars();

            DecodeRawVector<std::pair<const char*, i32>>(
                ytColumn.StartIndex,
                ytColumn.StartIndex + ytColumn.ValueCount,
                dictionaryIndexes,
                rleIndexes,
                [&] (i64 index) {
                    auto [startOffset, endOffset] = DecodeStringRange(ytOffsets, avgLength, index);
                    return std::pair(ytChars + startOffset, endOffset - startOffset);
                },
                checkedConsumer);
        }
    } else {
        YT_LOG_TRACE("Converting string column without dictionary (Count: %v, Rle: %v)",
            ytColumn.ValueCount,
            static_cast<bool>(rleIndexes));

        estimateAndResizeCHChars();

        // No dictionary encoding (but possibly RLE); we still avoid expensive multiplication on
        // each access by maintaining #avgLengthTimesIndex.
        i64 avgLengthTimesIndex = ytValueColumn->StartIndex * avgLength;
        i64 currentOffset = DecodeStringOffset(ytOffsets, avgLength, ytValueColumn->StartIndex);;
        DecodeRawVector<std::pair<const char*, i32>>(
            ytColumn.StartIndex,
            ytColumn.StartIndex + ytColumn.ValueCount,
            {},
            rleIndexes,
            [&] (i64 index) {
                auto startOffset = currentOffset;
                avgLengthTimesIndex += avgLength;
                auto endOffset = avgLengthTimesIndex + ZigZagDecode64(ytOffsets[index]);
                i32 length = endOffset - startOffset;
                currentOffset = endOffset;
                return std::pair(ytChars + startOffset, length);
            },
            checkedConsumer);
    }

    // Put the final offset.
    *currentCHOffset++ = currentCHCharsPosition;
    YT_VERIFY(currentCHOffset == chOffsets.end());

    // Trim chars.
    chChars.resize(currentCHCharsPosition);

    return chColumn;
}

} // namespace

DB::MutableColumnPtr ConvertDoubleYTColumnToCHColumn(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn)
{
    YT_LOG_TRACE("Converting double column (Count: %v)",
        ytColumn.ValueCount);

    return ConvertFloatingPointYTColumnToCHColumn<double>(ytColumn);
}

DB::MutableColumnPtr ConvertFloatYTColumnToCHColumn(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn)
{
    YT_LOG_TRACE("Converting float column (Count: %v)",
        ytColumn.ValueCount);

    return ConvertFloatingPointYTColumnToCHColumn<float>(ytColumn);
}

DB::ColumnString::MutablePtr ConvertStringLikeYTColumnToCHColumn(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn,
    TRange<DB::UInt8> filterHint)
{
    return ConvertStringLikeYTColumnToCHColumnImpl(ytColumn, filterHint);
}

DB::MutableColumnPtr ConvertBooleanYTColumnToCHColumn(const IUnversionedColumnarRowBatch::TColumn& ytColumn)
{
    YT_LOG_TRACE("Converting boolean column (Count: %v)",
        ytColumn.ValueCount);

    auto chColumn = DB::ColumnUInt8::create(ytColumn.ValueCount);

    DecodeBytemapFromBitmap(
        ytColumn.GetBitmapValues(),
        ytColumn.StartIndex,
        ytColumn.StartIndex + ytColumn.ValueCount,
        TMutableRange(chColumn->getData().data(), ytColumn.ValueCount));

    return chColumn;
}

DB::MutableColumnPtr ConvertNullYTColumnToCHColumn(const IUnversionedColumnarRowBatch::TColumn& ytColumn)
{
    YT_LOG_TRACE("Converting null column (Count: %v)",
        ytColumn.ValueCount);

    auto chColumn = DB::ColumnNothing::create(ytColumn.ValueCount);

    return chColumn;
}

DB::ColumnUInt8::MutablePtr BuildNullBytemapForCHColumn(const IUnversionedColumnarRowBatch::TColumn& ytColumn)
{
    auto chColumn = DB::ColumnUInt8::create(ytColumn.ValueCount);

    auto nullBytemap = TMutableRange(chColumn->getData().data(), ytColumn.ValueCount);

    auto [ytValueColumn, rleIndexes, dictionaryIndexes] = AnalyzeColumnEncoding(ytColumn);

    YT_LOG_TRACE("Building null bytemap (ValueCount: %v, Rle: %v, Dictionary: %v, NullBitmap: %v, Values: %v)",
        ytColumn.ValueCount,
        static_cast<bool>(rleIndexes),
        static_cast<bool>(dictionaryIndexes),
        static_cast<bool>(ytColumn.NullBitmap),
        static_cast<bool>(ytColumn.Values));

    if (rleIndexes && dictionaryIndexes) {
        BuildNullBytemapFromRleDictionaryIndexesWithZeroNull(
            dictionaryIndexes,
            rleIndexes,
            ytColumn.StartIndex,
            ytColumn.StartIndex + ytColumn.ValueCount,
            nullBytemap);
    } else if (rleIndexes && !dictionaryIndexes) {
        YT_VERIFY(ytValueColumn->NullBitmap);
        BuildNullBytemapFromRleNullBitmap(
            ytValueColumn->NullBitmap->Data,
            rleIndexes,
            ytColumn.StartIndex,
            ytColumn.StartIndex + ytColumn.ValueCount,
            nullBytemap);
    } else if (!rleIndexes && dictionaryIndexes) {
        BuildNullBytemapFromDictionaryIndexesWithZeroNull(
            dictionaryIndexes.Slice(ytColumn.StartIndex, ytColumn.StartIndex + ytColumn.ValueCount),
            nullBytemap);
    } else if (!ytColumn.NullBitmap) {
        // Refer to a comment around IUnversionedColumnarRowBatch::TColumn::NullBitmap.
        if (ytColumn.Values) {
            ::memset(nullBytemap.begin(), 0, nullBytemap.size());
        } else {
            ::memset(nullBytemap.begin(), 1, nullBytemap.size());
        }
    } else {
        YT_VERIFY(ytColumn.NullBitmap);
        DecodeBytemapFromBitmap(
            ytColumn.NullBitmap->Data,
            ytColumn.StartIndex,
            ytColumn.StartIndex + ytColumn.ValueCount,
            nullBytemap);
    }

    return chColumn;
}

DB::MutableColumnPtr ConvertIntegerYTColumnToCHColumn(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn,
    ESimpleLogicalValueType type)
{
    auto [ytValueColumn, rleIndexes, dictionaryIndexes] = AnalyzeColumnEncoding(ytColumn);

    YT_LOG_TRACE("Converting integer column (Count: %v, Rle: %v, Dictionary: %v)",
        ytColumn.ValueCount,
        static_cast<bool>(rleIndexes),
        static_cast<bool>(dictionaryIndexes));

    switch (type) {
        #define XX(ytType, columnType, ...) \
            case ESimpleLogicalValueType::ytType: { \
                return ConvertIntegerYTColumnToCHColumnImpl<columnType>(__VA_ARGS__); \
            }
        #define XX_ARGS ytColumn, *ytValueColumn, dictionaryIndexes, rleIndexes
        #define XX_VECTOR_COLUMN(ytType, chType) XX(ytType, DB::ColumnVector<chType>, XX_ARGS)
        #define XX_DATETIME_COLUMN(ytType, decimalScale) XX(ytType, DB::ColumnDecimal<DB::DateTime64>, XX_ARGS, decimalScale)

        XX_VECTOR_COLUMN(Int8,        Int8)
        XX_VECTOR_COLUMN(Int16,       Int16)
        XX_VECTOR_COLUMN(Int32,       Int32)
        XX_VECTOR_COLUMN(Int64,       Int64)

        XX_VECTOR_COLUMN(Uint8,       UInt8)
        XX_VECTOR_COLUMN(Uint16,      UInt16)
        XX_VECTOR_COLUMN(Uint32,      UInt32)
        XX_VECTOR_COLUMN(Uint64,      UInt64)

        XX_VECTOR_COLUMN(Date,        UInt16)
        XX_VECTOR_COLUMN(Date32,      Int32)
        XX_VECTOR_COLUMN(Datetime,    UInt32)
        XX_VECTOR_COLUMN(Interval,    Int64)
        XX_VECTOR_COLUMN(Interval64,  Int64)

        XX_DATETIME_COLUMN(Datetime64, 0)
        XX_DATETIME_COLUMN(Timestamp, 6)
        XX_DATETIME_COLUMN(Timestamp64, 6)

        #undef XX
        #undef XX_ARGS
        #undef XX_VECTOR_COLUMN
        #undef XX_DATETIME_COLUMN

        default:
            YT_ABORT();
    }
}

DB::ColumnString::MutablePtr ConvertCHColumnToAny(
    const DB::IColumn& column,
    ESimpleLogicalValueType type,
    EExtendedYsonFormat ysonFormat)
{
    switch (ysonFormat) {
        #define XX(format) \
            case EExtendedYsonFormat::format: \
                return ConvertCHColumnToAnyImpl<EExtendedYsonFormat::format>(column, type);

        XX(Binary)
        XX(Text)
        XX(Pretty)
        XX(UnescapedText)
        XX(UnescapedPretty)

        #undef XX
    }

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
