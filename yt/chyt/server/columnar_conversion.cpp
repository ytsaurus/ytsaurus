#include "columnar_conversion.h"

#include "config.h"
#include "helpers.h"

#include <yt/yt/client/table_client/columnar.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/clickhouse_functions/unescaped_yson.h>
#include <yt/yt/library/tz_types/tz_types.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnUnique.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ClickHouseYtLogger;

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
    TRef nullBitmap,
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
        nullBitmap,
        [&] (auto index) {
            return values[index];
        },
        [&] (auto value) {
            *currentOutput++ = value;
        });

    return chColumn;
}

template <class T>
DB::MutableColumnPtr ConvertIntegerYTColumnToLowCardinalityCHColumnImpl(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn,
    const IUnversionedColumnarRowBatch::TColumn& ytValueColumn,
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    TRef nullBitmap,
    bool insideOptional)
{
    DB::DataTypePtr dictionaryType = std::make_shared<DB::DataTypeNumber<T>>();
    if (insideOptional) {
        dictionaryType = std::make_shared<DB::DataTypeNullable>(dictionaryType);
    }
    auto dictionaryColumn = DB::DataTypeLowCardinality::createColumnUnique(*dictionaryType);

    auto indexesColumn = DB::DataTypeUInt64().createColumn();

    auto lowCardinalityColumn = DB::ColumnLowCardinality::create(
        std::move(dictionaryColumn),
        std::move(indexesColumn),
        /*is_shared*/ false);

    auto values = ytValueColumn.GetTypedValues<ui64>();

    // DecodeIntegerVector don't really consume nulls, so we need to iterate bitmap manually.
    auto nullBitMapIndex = 0;

    DecodeIntegerVector(
        ytColumn.StartIndex,
        ytColumn.StartIndex + ytColumn.ValueCount,
        ytValueColumn.Values->BaseValue,
        ytValueColumn.Values->ZigZagEncoded,
        dictionaryIndexes,
        rleIndexes,
        nullBitmap,
        [&] (auto index) {
            return values[index];
        },
        [&] (auto value) {
            if (nullBitmap && GetBit(nullBitmap, nullBitMapIndex++)) {
                lowCardinalityColumn->insertDefault();
            } else {
                lowCardinalityColumn->insert(value);
            }
        });

    return lowCardinalityColumn;
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

TRef GetNullBitmap(const IUnversionedColumnarRowBatch::TColumn& ytColumn)
{
    const auto* valueColumn = ytColumn.Rle
        ? ytColumn.Rle->ValueColumn
        : &ytColumn;
    if (valueColumn->NullBitmap) {
        return valueColumn->NullBitmap->Data;
    }
    return {};
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

    // We can get empty column (example: dictionary encoded null column with simple distinct optimization).
    if (ytColumn.ValueCount == 0) {
        return chColumn;
    }

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

template <ESimpleLogicalValueType LogicalType, class TColumn, class... Args>
DB::MutableColumnPtr ConvertTzYTColumnToCHColumnImpl(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn,
    TRange<DB::UInt8> filterHint,
    Args&&... args)
{
    auto [ytValueColumn, rleIndexes, dictionaryIndexes] = AnalyzeColumnEncoding(ytColumn);

    YT_LOG_TRACE("Converting tz column (Count: %v, Dictionary: %v, Rle: %v)",
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

    auto chColumn = TColumn::create(ytColumn.ValueCount, std::forward<Args>(args)...);

    auto* currentOutput = chColumn->getData().data();

    using dateInt = TTzIntegerType<LogicalType>;

    auto consumer = [&] (dateInt timestamp) {
        *currentOutput++ = timestamp;
    };

    if (filterHint) {
        int rowIndex = 0;
        DecodeRawVector<dateInt>(
            ytColumn.StartIndex,
            ytColumn.StartIndex + ytColumn.ValueCount,
            dictionaryIndexes,
            rleIndexes,
            [&] (i64 index) {
                    if (filterHint[rowIndex]) {
                        auto [startOffset, endOffset] = DecodeStringRange(ytOffsets, avgLength, index);
                        return GetTimestampFromTzString<LogicalType>(std::string_view(ytChars + startOffset, endOffset - startOffset));
                    } else {
                        return static_cast<TTzIntegerType<LogicalType>>(0);
                    }
                },
            [&] (auto timestamp) {
                if (filterHint[rowIndex++]) {
                    consumer(timestamp);
                } else {
                    consumer(0);
                }
            });
    } else if (dictionaryIndexes) {
        constexpr int SmallDictionaryFactor = 3;
        if (static_cast<i64>(ytOffsets.Size()) * SmallDictionaryFactor < ytColumn.ValueCount) {
            YT_LOG_TRACE("Converting tz date column with small dictionary (Count: %v, DictionarySize: %v, Rle: %v)",
                ytColumn.ValueCount,
                ytOffsets.size(),
                static_cast<bool>(rleIndexes));

            std::vector<const char*> ytStrings(ytOffsets.size());
            std::vector<i32> ytStringLengths(ytOffsets.size());
            DecodeStringPointersAndLengths(
                ytOffsets,
                avgLength,
                ytValueColumn->Strings->Data,
                TMutableRange(ytStrings),
                TMutableRange(ytStringLengths));

            auto stringsFetcher = [&] (i64 index) {
                return GetTimestampFromTzString<LogicalType>(std::string_view(ytStrings[index], ytStringLengths[index]));
            };

            DecodeRawVector<dateInt>(
                ytColumn.StartIndex,
                ytColumn.StartIndex + ytColumn.ValueCount,
                dictionaryIndexes,
                rleIndexes,
                stringsFetcher,
                consumer);
        } else {
            YT_LOG_TRACE("Converting tz column with large dictionary (Count: %v, DictionarySize: %v, Rle: %v)",
                ytColumn.ValueCount,
                ytOffsets.size(),
                static_cast<bool>(rleIndexes));

            DecodeRawVector<dateInt>(
                ytColumn.StartIndex,
                ytColumn.StartIndex + ytColumn.ValueCount,
                dictionaryIndexes,
                rleIndexes,
                [&] (i64 index) {
                    auto [startOffset, endOffset] = DecodeStringRange(ytOffsets, avgLength, index);
                    return GetTimestampFromTzString<LogicalType>(std::string_view(ytChars + startOffset, endOffset - startOffset));
                },
                consumer);
        }
    } else {
        YT_LOG_TRACE("Converting tz column without dictionary (Count: %v, Rle: %v)",
            ytColumn.ValueCount,
            static_cast<bool>(rleIndexes));

        i64 avgLengthTimesIndex = ytValueColumn->StartIndex * avgLength;
        i64 currentOffset = DecodeStringOffset(ytOffsets, avgLength, ytValueColumn->StartIndex);
        DecodeRawVector<dateInt>(
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
                return GetTimestampFromTzString<LogicalType>(std::string_view(ytChars + startOffset, length));
            },
            consumer);
    }

    return chColumn;
}

DB::MutableColumnPtr ConvertStringLikeYTColumnToLowCardinalityCHColumnImpl(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn,
    TRange<DB::UInt8> filterHint,
    bool insideOptional)
{
    auto [ytValueColumn, rleIndexes, dictionaryIndexes] = AnalyzeColumnEncoding(ytColumn);

    YT_LOG_TRACE("Converting string-like column to LowCardinality (Count: %v, Dictionary: %v, Rle: %v, InsideOptional: %v)",
        ytColumn.ValueCount,
        static_cast<bool>(dictionaryIndexes),
        static_cast<bool>(rleIndexes),
        insideOptional);

    YT_VERIFY(ytValueColumn->Values);
    YT_VERIFY(ytValueColumn->Values->BitWidth == 32);
    YT_VERIFY(ytValueColumn->Values->BaseValue == 0);
    YT_VERIFY(ytValueColumn->Values->ZigZagEncoded);
    YT_VERIFY(ytValueColumn->Strings);
    YT_VERIFY(ytValueColumn->Strings->AvgLength);

    auto ytOffsets = ytValueColumn->GetTypedValues<ui32>();
    const auto* ytChars = ytValueColumn->Strings->Data.Begin();

    auto avgLength = *ytValueColumn->Strings->AvgLength;
    DB::DataTypePtr dictionaryType = std::make_shared<DB::DataTypeString>();
    if (insideOptional) {
        dictionaryType = std::make_shared<DB::DataTypeNullable>(dictionaryType);
    }
    auto dictionaryColumn = DB::DataTypeLowCardinality::createColumnUnique(*dictionaryType);

    auto indexesColumn = DB::DataTypeUInt64().createColumn();

    auto bytemap = insideOptional ? BuildNullBytemapForCHColumn(ytColumn) : nullptr;
    auto bytemapIndex = ytValueColumn->StartIndex;

    DB::MutableColumnPtr lowCardinalityColumn;

    if (filterHint) {
        YT_LOG_TRACE("Converting string column to LowCardinality with filter hint (Count: %v, DictionarySize: %v)",
            ytColumn.ValueCount,
            ytOffsets.size());
        YT_VERIFY(std::ssize(filterHint) == ytColumn.ValueCount);
        lowCardinalityColumn = DB::ColumnLowCardinality::create(
            std::move(dictionaryColumn),
            std::move(indexesColumn),
            /*is_shared*/ false);

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
                if (filterHint[rowIndex] || (!bytemap || bytemap->getData()[bytemapIndex] != 0)) {
                    lowCardinalityColumn->insertData(pair.first, pair.second);
                } else {
                    lowCardinalityColumn->insertDefault();
                }
                rowIndex++;
                bytemapIndex++;
            });
    } else if (dictionaryIndexes || rleIndexes) {
        YT_LOG_TRACE("Converting string column to LowCardinality (Count: %v, DictionarySize: %v)",
            ytColumn.ValueCount,
            ytOffsets.size());

        std::vector<const char*> ytStrings(ytOffsets.size());
        std::vector<i32> ytStringLengths(ytOffsets.size());
        std::vector<size_t> positions;
        positions.reserve(ytOffsets.size());
        positions.push_back(dictionaryColumn->getDefaultValueIndex());
        DecodeStringPointersAndLengths(
            ytOffsets,
            avgLength,
            ytValueColumn->Strings->Data,
            TMutableRange(ytStrings),
            TMutableRange(ytStringLengths));

        for (auto index = ytValueColumn->StartIndex; index < ytValueColumn->StartIndex + ytValueColumn->ValueCount; ++index) {
            positions.push_back(dictionaryColumn->uniqueInsertData(ytStrings[index], ytStringLengths[index]));
        }

        DecodeRawVector<ui64>(
            ytColumn.StartIndex,
            ytColumn.StartIndex + ytColumn.ValueCount,
            dictionaryIndexes,
            rleIndexes,
            [&] (ui64 index) {
                return index + 1;
            },
            [&] (ui64 index) {
                if (bytemap && bytemap->getData()[bytemapIndex++]) {
                    index = 0;
                }
                indexesColumn->insert(positions[index]);
            }
        );

        lowCardinalityColumn = DB::ColumnLowCardinality::create(
            std::move(dictionaryColumn),
            std::move(indexesColumn),
            /*is_shared*/ false);
    } else {
        YT_LOG_TRACE("Converting string column without dictionary to LowCardinality (Count: %v, Rle: %v)",
            ytColumn.ValueCount,
            static_cast<bool>(rleIndexes));
        lowCardinalityColumn = DB::ColumnLowCardinality::create(
            std::move(dictionaryColumn),
            std::move(indexesColumn),
            /*is_shared*/ false);

        i64 avgLengthTimesIndex = ytValueColumn->StartIndex * avgLength;
        i64 currentOffset = DecodeStringOffset(ytOffsets, avgLength, ytValueColumn->StartIndex);
        DecodeRawVector<std::pair<const char*, i32>>(
            ytColumn.StartIndex,
            ytColumn.StartIndex + ytColumn.ValueCount,
            dictionaryIndexes,
            rleIndexes,
            [&] (i64 index) {
                auto startOffset = currentOffset;
                avgLengthTimesIndex += avgLength;
                auto endOffset = avgLengthTimesIndex + ZigZagDecode64(ytOffsets[index]);
                i32 length = endOffset - startOffset;
                currentOffset = endOffset;
                return std::make_pair(ytChars + startOffset, length);
            },
            [&] (auto pair) {
                if (!bytemap || bytemap->getData()[bytemapIndex++] == 0) {
                    lowCardinalityColumn->insertData(pair.first, pair.second);
                } else {
                    lowCardinalityColumn->insertDefault();
                }
            });
    }
    return lowCardinalityColumn;
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

DB::MutableColumnPtr ConvertStringLikeYTColumnToLowCardinalityCHColumn(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn,
    TRange<DB::UInt8> filterHint,
    bool insideOptional)
{
    return ConvertStringLikeYTColumnToLowCardinalityCHColumnImpl(ytColumn, filterHint, insideOptional);
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
    TRef nullBitmap = GetNullBitmap(ytColumn);

    YT_LOG_TRACE("Converting integer column (Count: %v, Rle: %v, Dictionary: %v)",
        ytColumn.ValueCount,
        static_cast<bool>(rleIndexes),
        static_cast<bool>(dictionaryIndexes));

    switch (type) {
        #define XX(ytType, columnType, ...) \
            case ESimpleLogicalValueType::ytType: { \
                return ConvertIntegerYTColumnToCHColumnImpl<columnType>(__VA_ARGS__); \
            }
        #define XX_ARGS ytColumn, *ytValueColumn, dictionaryIndexes, rleIndexes, nullBitmap
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

DB::MutableColumnPtr ConvertTzYTColumnToCHColumn(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn,
    TRange<DB::UInt8> filterHint,
    NTableClient::ESimpleLogicalValueType type)
{
    switch(type) {
        #define XX(ytType, columnType, ...) \
                case ESimpleLogicalValueType::ytType: { \
                    return ConvertTzYTColumnToCHColumnImpl<ESimpleLogicalValueType::ytType, columnType>(__VA_ARGS__); \
                }
        #define XX_ARGS ytColumn, filterHint
        #define XX_VECTOR_COLUMN(ytType, chType) XX(ytType, DB::ColumnVector<chType>, XX_ARGS)
        #define XX_DECIMAL_COLUMN(ytType, decimalScale) XX(ytType, DB::ColumnDecimal<DB::DateTime64>, XX_ARGS, decimalScale)

        XX_VECTOR_COLUMN(TzDate,        UInt16)
        XX_VECTOR_COLUMN(TzDate32,      Int32)
        XX_VECTOR_COLUMN(TzDatetime,    UInt32)

        XX_DECIMAL_COLUMN(TzDatetime64, 0)
        XX_DECIMAL_COLUMN(TzTimestamp, 6)
        XX_DECIMAL_COLUMN(TzTimestamp64, 6)

        #undef XX
        #undef XX_ARGS
        #undef XX_VECTOR_COLUMN
        #undef XX_DECIMAL_COLUMN

        default:
            YT_ABORT();
    }
}

DB::MutableColumnPtr ConvertIntegerYTColumnToLowCardinalityCHColumn(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn,
    ESimpleLogicalValueType type,
    bool insideOptional)
{
    auto [ytValueColumn, rleIndexes, dictionaryIndexes] = AnalyzeColumnEncoding(ytColumn);
    TRef nullBitmap = GetNullBitmap(ytColumn);

    YT_LOG_TRACE("Converting integer column (Count: %v, Rle: %v, Dictionary: %v)",
        ytColumn.ValueCount,
        static_cast<bool>(rleIndexes),
        static_cast<bool>(dictionaryIndexes));

    switch (type) {
        #define XX(ytType, chType, ...) \
            case ESimpleLogicalValueType::ytType: { \
                return ConvertIntegerYTColumnToLowCardinalityCHColumnImpl<chType>(ytColumn, *ytValueColumn, dictionaryIndexes, rleIndexes, nullBitmap, insideOptional); \
            }

        XX(Int8,        Int8)
        XX(Int16,       Int16)
        XX(Int32,       Int32)
        XX(Int64,       Int64)

        XX(Uint8,       UInt8)
        XX(Uint16,      UInt16)
        XX(Uint32,      UInt32)
        XX(Uint64,      UInt64)

        XX(Date,        UInt16)
        XX(Date32,      Int32)
        XX(Datetime,    UInt32)
        XX(Interval,    Int64)
        XX(Interval64,  Int64)

        #undef XX

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

void ReduceFilterToDistinct(
    DB::IColumn::Filter& filter,
    const IUnversionedColumnarRowBatch::TColumn& ytColumn)
{
    auto [ytValueColumn, rleIndexes, dictionaryIndexes] = AnalyzeColumnEncoding(ytColumn);

    if ((dictionaryIndexes.empty() && rleIndexes.empty()) || filter.empty()) {
        return;
    }

    DB::IColumn::Filter newFilter;
    newFilter.resize_fill(ytValueColumn->ValueCount + 1);

    int rowIndex = 0;
    bool hasNull = false;
    DecodeRawVector<i64>(
        ytColumn.StartIndex,
        ytColumn.StartIndex + ytColumn.ValueCount,
        dictionaryIndexes,
        rleIndexes,
        [&] (i64 index) {
            return index - ytValueColumn->StartIndex + 1;
        },
        [&] (i64 index) {
            hasNull |= (index == 0);
            if (filter[rowIndex++]) {
                index = (index == 0) ? newFilter.size() : index;
                newFilter.data()[index - 1] = 1;
            }
        });
    if (!hasNull) {
        newFilter.pop_back();
    }
    filter = std::move(newFilter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
