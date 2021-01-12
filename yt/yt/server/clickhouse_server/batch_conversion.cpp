#include "batch_conversion.h"

#include "helpers.h"
#include "composite.h"
#include "config.h"

#include <yt/ytlib/table_client/columnar.h>

#include <yt/client/table_client/unversioned_row_batch.h>
#include <yt/client/table_client/logical_type.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/helpers.h>

#include <yt/core/misc/memory_ops.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <EYsonFormat ysonFormat, class F>
DB::ColumnString::MutablePtr ConvertCHColumnToAnyByIndexImpl(const DB::IColumn& column, F func)
{
    TString ysonBuffer;
    TStringOutput ysonOutput(ysonBuffer);

    // Using IIFE for constexpr resolution of the writer (for binary YSON we use TBufferedBinaryYsonWriter,
    // for non-binary we use TYsonWriter).
    auto ysonWriter = [&] {
        if constexpr (ysonFormat == EYsonFormat::Binary) {
            return TBufferedBinaryYsonWriter(&ysonOutput);
        } else {
            return TYsonWriter(&ysonOutput, ysonFormat);
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

template <EYsonFormat ysonFormat, class T, class F>
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

template <EYsonFormat ysonFormat, class F>
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

template <EYsonFormat ysonFormat>
DB::ColumnString::MutablePtr ConvertCHColumnToAnyImpl(const DB::IColumn& column, ESimpleLogicalValueType type)
{
    YT_LOG_TRACE("Converting column to any (Count: %v, Type: %v)",
        column.size(),
        type);

    switch (type) {
        #define XX(valueType, cppType) \
            case ESimpleLogicalValueType::valueType: \
                return ConvertCHVectorColumnToAnyImpl<ysonFormat, cppType>( \
                    column, \
                    [] (cppType value, auto* writer) { writer->OnInt64Scalar(value); });
        XX(Int8,      i8 )
        XX(Int16,     i16)
        XX(Int32,     i32)
        XX(Int64,     i64)
        XX(Interval,  i64)
        #undef XX

        #define XX(valueType, cppType) \
            case ESimpleLogicalValueType::valueType: \
                return ConvertCHVectorColumnToAnyImpl<ysonFormat, cppType>( \
                    column, \
                    [] (cppType value, auto* writer) { writer->OnUint64Scalar(value); });
        XX(Uint8,     DB::UInt8)
        XX(Uint16,    ui16)
        XX(Uint32,    ui32)
        XX(Uint64,    ui64)
        XX(Date,      ui16)
        XX(Datetime,  ui32)
        XX(Timestamp, ui64)
        #undef XX

        #define XX(chType, cppType) \
            case ESimpleLogicalValueType::chType: \
                return ConvertCHVectorColumnToAnyImpl<ysonFormat, cppType>( \
                    column, \
                    [] (cppType value, auto* writer) { writer->OnDoubleScalar(value); });
        XX(Float,  float )
        XX(Double, double)
        #undef XX

        case ESimpleLogicalValueType::Boolean:
            return ConvertCHVectorColumnToAnyImpl<ysonFormat, DB::UInt8>(
                column,
                [] (DB::UInt8 value, auto* writer) { writer->OnBooleanScalar(value != 0); });

        case ESimpleLogicalValueType::String:
            return ConvertCHStringColumnToAnyImpl<ysonFormat>(
                column,
                [] (TStringBuf value, auto* writer) { writer->OnStringScalar(value); });

        default:
            THROW_ERROR_EXCEPTION("Cannot convert CH column to %Qlv type",
                ESimpleLogicalValueType::Any);
    }
}

template <class T>
DB::MutableColumnPtr ConvertIntegerYTColumnToCHColumnImpl(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn,
    const IUnversionedColumnarRowBatch::TColumn& ytValueColumn,
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes)
{
    auto chColumn = DB::ColumnVector<T>::create(ytColumn.ValueCount);
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

    return std::make_tuple(
        ytValueColumn,
        rleIndexes,
        dictionaryIndexes);
}

template <class T>
DB::MutableColumnPtr ConvertFloatingPointYTColumnToCHColumn(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn)
{
    auto relevantValues = ytColumn.GetRelevantTypedValues<T>();

    auto chColumn = DB::ColumnVector<T>::create(ytColumn.ValueCount);

    ::memcpy(
        chColumn->getData().data(),
        relevantValues.Begin(),
        sizeof (T) * ytColumn.ValueCount);

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

DB::MutableColumnPtr ConvertStringLikeYTColumnToCHColumnImpl(const IUnversionedColumnarRowBatch::TColumn& ytColumn)
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
            - currentCHCharsPosition
            - MemcpySmallUnsafePadding;
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
        MemcpySmallUnsafe(currentCHChar, str, length);
        currentCHChar += length;
        *currentCHChar++ = '\x0';
        currentCHCharsPosition += length + 1;
    };

    auto checkedConsumer = [&] (auto pair) {
        auto [str, length] = pair;
        if (Y_UNLIKELY(remainingCHCharsCapacity <= static_cast<size_t>(length))) {
            resizeCHChars(
                std::max(chChars.size() * 2, chChars.size() + (static_cast<size_t>(length) + 1) +
                MemcpySmallUnsafePadding));
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

    if (dictionaryIndexes) {
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
                MakeMutableRange(ytStrings),
                MakeMutableRange(ytStringLengths));

            auto stringsFetcher = [&] (i64 index) {
                return std::make_pair(ytStrings[index], ytStringLengths[index]);
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
                    ytColumn.ValueCount +
                    MemcpySmallUnsafePadding);

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
                    return std::make_pair(ytChars + startOffset, endOffset - startOffset);
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
                return std::make_pair(ytChars + startOffset, length);
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

bool IsIntegerLikeType(ESimpleLogicalValueType type)
{
    return
        IsIntegralType(type) ||
        type == ESimpleLogicalValueType::Date ||
        type == ESimpleLogicalValueType::Datetime ||
        type == ESimpleLogicalValueType::Interval ||
        type == ESimpleLogicalValueType::Timestamp;
}

DB::ColumnPtr ConvertCHColumnToComposite(
    const DB::IColumn& column,
    const DB::IColumn* nullColumn,
    const TColumnSchema& schema,
    const TCompositeSettingsPtr& settings)
{
    auto* typedColumn = DB::checkAndGetColumn<DB::ColumnString>(column);
    auto* typedNullColumn = (nullColumn) ? DB::checkAndGetColumn<DB::ColumnUInt8>(nullColumn) : nullptr;

    TCompositeValueToClickHouseColumnConverter converter(TComplexTypeFieldDescriptor(schema), settings);
    for (int index = 0; index < typedColumn->size(); ++index) {
        if (typedNullColumn && typedNullColumn->getBool(index)) {
            converter.ConsumeNulls(1);
        } else {
            auto yson = typedColumn->getDataAt(index);
            converter.ConsumeYson(TYsonStringBuf(yson.data, yson.size));
        }
    }

    return converter.FlushColumn();
}


DB::ColumnPtr ConvertYTColumnToCHColumn(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn,
    const TColumnSchema& chSchema,
    const TCompositeSettingsPtr& compositeSettings)
{
    DB::ColumnPtr chColumn;

    auto ytType = CastToV1Type(ytColumn.Type).first;
    auto chType = chSchema.CastToV1Type();

    auto throwOnIncompatibleType = [&] (bool ok) {
        if (!ok) {
            THROW_ERROR_EXCEPTION("Cannot convert %Qlv column to %Qlv type",
                ytType,
                chType);
        }
    };

    bool anyUpcast = false;
    if (ytType != ESimpleLogicalValueType::Any && chType == ESimpleLogicalValueType::Any) {
        chType = ytType;
        anyUpcast = true;
    }

    if (IsIntegerLikeType(chType)) {
        throwOnIncompatibleType(IsIntegerLikeType(ytType));
        chColumn = ConvertIntegerYTColumnToCHColumn(ytColumn, chType);
    } else if (chType == ESimpleLogicalValueType::Double) {
        throwOnIncompatibleType(ytType == ESimpleLogicalValueType::Double);
        chColumn = ConvertDoubleYTColumnToCHColumn(ytColumn);
    } else if (chType == ESimpleLogicalValueType::Float) {
        throwOnIncompatibleType(ytType == ESimpleLogicalValueType::Float);
        chColumn = ConvertFloatYTColumnToCHColumn(ytColumn);
    } else if (IsStringLikeType(chType)) {
        throwOnIncompatibleType(ytType == chType);
        chColumn = ConvertStringLikeYTColumnToCHColumn(ytColumn);
    } else if (chType == ESimpleLogicalValueType::Boolean) {
        throwOnIncompatibleType(ytType == ESimpleLogicalValueType::Boolean);
        chColumn = ConvertBooleanYTColumnToCHColumn(ytColumn);
    } else {
        THROW_ERROR_EXCEPTION("%Qlv type is not supported",
            chType);
    }

    if (anyUpcast) {
        chColumn = ConvertCHColumnToAny(*chColumn, ytType);
    }

    DB::ColumnPtr nullMapCHColumn;
    if (chSchema.LogicalType()->GetMetatype() == ELogicalMetatype::Optional) {
        nullMapCHColumn = BuildNullBytemapForCHColumn(ytColumn);
    }

    if (chSchema.GetPhysicalType() == EValueType::Any && compositeSettings->EnableConversion) {
        // Composite type converter wraps resulting column into nullable internally,
        // so just provide information about physical-level nulls to it.
        chColumn = ConvertCHColumnToComposite(*chColumn, nullMapCHColumn.get(), chSchema, compositeSettings);
    } else {
        // Wrap with nullable manually.
        if (nullMapCHColumn) {
            chColumn = DB::ColumnNullable::create(std::move(chColumn), std::move(nullMapCHColumn));
        }
    }

    return chColumn;
}

DB::Block ConvertColumnarRowBatchToBlock(
    const IUnversionedColumnarRowBatchPtr& batch,
    const TTableSchema& readSchema,
    const std::vector<int>& idToColumnIndex,
    const DB::Block& headerBlock,
    const TCompositeSettingsPtr& compositeSettings)
{
    // NB(max42): CHYT-256.
    // If chunk schema contains not all of the requested columns (which may happen
    // when a non-required column was introduced after chunk creation), we are not
    // going to receive some of the columns from reader. We still need
    // to provide them to CH, though, so we keep track of columns coming from the reader.
    std::vector<bool> presentColumnMask(readSchema.GetColumnCount(), false);

    auto block = headerBlock.cloneEmpty();

    std::vector<TCompositeValueToClickHouseColumnConverter> compositeValueConverters;
    compositeValueConverters.reserve(readSchema.GetColumnCount());

    for (int columnIndex = 0; columnIndex < readSchema.GetColumnCount(); ++columnIndex) {
        const auto& columnSchema = readSchema.Columns()[columnIndex];
        TComplexTypeFieldDescriptor descriptor(columnSchema);
        compositeValueConverters.emplace_back(descriptor, compositeSettings);
    }

    auto batchColumns = batch->MaterializeColumns();
    for (const auto* ytColumn : batchColumns) {
        auto columnIndex = idToColumnIndex[ytColumn->Id];
        YT_VERIFY(columnIndex != -1);

        compositeValueConverters[columnIndex].ConsumeYtColumn(*ytColumn);

        presentColumnMask[columnIndex] = true;
    }

    for (int columnIndex = 0; columnIndex < static_cast<int>(readSchema.Columns().size()); ++columnIndex) {
        if (!presentColumnMask[columnIndex]) {
            YT_VERIFY(!readSchema.Columns()[columnIndex].Required());
            compositeValueConverters[columnIndex].ConsumeNulls(batch->GetRowCount());
        }
        auto column = compositeValueConverters[columnIndex].FlushColumn();
        YT_VERIFY(column->size() == batch->GetRowCount());
        block.getByPosition(columnIndex).column = std::move(column);
    }

    return block;
}

DB::Block ConvertNonColumnarRowBatchToBlock(
    const IUnversionedRowBatchPtr& batch,
    const TTableSchema& readSchema,
    const std::vector<int>& idToColumnIndex,
    const TRowBufferPtr& rowBuffer,
    const DB::Block& headerBlock,
    const TCompositeSettingsPtr& compositeSettings)
{
    // NB(max42): CHYT-256.
    // If chunk schema contains not all of the requested columns (which may happen
    // when a non-required column was introduced after chunk creation), we are not
    // going to receive some of the unversioned values with nulls. We still need
    // to provide them to CH, though, so we keep track of present columns for each
    // row we get and add nulls for all unpresent columns.
    std::vector<bool> presentValueMask;

    auto block = headerBlock.cloneEmpty();

    // Indexed by column indices.
    std::vector<TCompositeValueToClickHouseColumnConverter> compositeValueConverters;
    compositeValueConverters.reserve(readSchema.GetColumnCount());

    for (int columnIndex = 0; columnIndex < readSchema.GetColumnCount(); ++columnIndex) {
        const auto& columnSchema = readSchema.Columns()[columnIndex];
        TComplexTypeFieldDescriptor descriptor(columnSchema);
        compositeValueConverters.emplace_back(descriptor, compositeSettings);
    }

    auto rowBatch = batch->MaterializeRows();

    // We transpose rows by writing down contiguous range of values for each column.
    // This is done to reduce the number of converter virtual calls.
    std::vector<std::vector<TUnversionedValue>> columnIndexToUnversionedValues(readSchema.GetColumnCount());
    for (auto& unversionedValues : columnIndexToUnversionedValues) {
        unversionedValues.reserve(rowBatch.size());
    }

    auto nullValue = MakeUnversionedNullValue();

    for (auto row : rowBatch) {
        presentValueMask.assign(readSchema.GetColumnCount(), false);
        for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
            auto value = row[index];
            auto id = value.Id;
            int columnIndex = (id < idToColumnIndex.size()) ? idToColumnIndex[id] : -1;
            YT_VERIFY(columnIndex != -1);
            YT_VERIFY(!presentValueMask[columnIndex]);
            presentValueMask[columnIndex] = true;
            columnIndexToUnversionedValues[columnIndex].emplace_back(value);
        }
        for (int columnIndex = 0; columnIndex < readSchema.GetColumnCount(); ++columnIndex) {
            if (!presentValueMask[columnIndex]) {
                YT_VERIFY(readSchema.Columns()[columnIndex].LogicalType()->IsNullable());
                // NB: converter does not care about value ids.
                columnIndexToUnversionedValues[columnIndex].emplace_back(nullValue);
            }
        }
    }

    for (int columnIndex = 0; columnIndex < readSchema.GetColumnCount(); ++columnIndex) {
        const auto& unversionedValues = columnIndexToUnversionedValues[columnIndex];
        YT_VERIFY(unversionedValues.size() == rowBatch.size());
        auto& converter = compositeValueConverters[columnIndex];
        converter.ConsumeUnversionedValues(unversionedValues);
        auto column = converter.FlushColumn();
        YT_VERIFY(column->size() == rowBatch.size());
        block.getByPosition(columnIndex).column = std::move(column);
    }

    return block;
}

} // namespace

DB::Block ConvertRowBatchToBlock(
    const IUnversionedRowBatchPtr& batch,
    const TTableSchema& readSchema,
    const std::vector<int>& idToColumnIndex,
    const TRowBufferPtr& rowBuffer,
    const DB::Block& headerBlock,
    const TCompositeSettingsPtr& compositeSettings)
{
    if (auto columnarBatch = batch->TryAsColumnar()) {
        return ConvertColumnarRowBatchToBlock(
            columnarBatch,
            readSchema,
            idToColumnIndex,
            headerBlock,
            compositeSettings);
    } else {
        return ConvertNonColumnarRowBatchToBlock(
            batch,
            readSchema,
            idToColumnIndex,
            rowBuffer,
            headerBlock,
            compositeSettings);
    }
}

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

DB::MutableColumnPtr ConvertStringLikeYTColumnToCHColumn(
    const IUnversionedColumnarRowBatch::TColumn& ytColumn)
{
    return ConvertStringLikeYTColumnToCHColumnImpl(ytColumn);
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
        MakeMutableRange(chColumn->getData().data(), ytColumn.ValueCount));

    return chColumn;
}

DB::ColumnUInt8::MutablePtr BuildNullBytemapForCHColumn(const IUnversionedColumnarRowBatch::TColumn& ytColumn)
{
    auto chColumn = DB::ColumnUInt8::create(ytColumn.ValueCount);

    auto nullBytemap = MakeMutableRange(chColumn->getData().data(), ytColumn.ValueCount);

    auto [ytValueColumn, rleIndexes, dictionaryIndexes] = AnalyzeColumnEncoding(ytColumn);

    YT_LOG_TRACE("Buliding null bytemap (Value: %v, Rle: %v, Dictionary: %v)",
        ytColumn.ValueCount,
        static_cast<bool>(rleIndexes),
        static_cast<bool>(dictionaryIndexes));

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
    } else if (!rleIndexes && dictionaryIndexes)  {
        BuildNullBytemapFromDictionaryIndexesWithZeroNull(
            dictionaryIndexes.Slice(ytColumn.StartIndex, ytColumn.StartIndex + ytColumn.ValueCount),
            nullBytemap);
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
        #define XX(ytType, chType) \
            case ESimpleLogicalValueType::ytType: { \
                return ConvertIntegerYTColumnToCHColumnImpl<chType>( \
                    ytColumn, \
                    *ytValueColumn, \
                    dictionaryIndexes, \
                    rleIndexes); \
            }

        XX(Int8,      Int8)
        XX(Int16,     Int16)
        XX(Int32,     Int32)
        XX(Int64,     Int64)

        XX(Uint8,     UInt8)
        XX(Uint16,    UInt16)
        XX(Uint32,    UInt32)
        XX(Uint64,    UInt64)

        XX(Date,      UInt16)
        XX(Datetime,  UInt32)
        XX(Interval,  Int64)
        XX(Timestamp, UInt64)

        #undef XX

        default:
            YT_ABORT();
    }
}

DB::ColumnString::MutablePtr ConvertCHColumnToAny(
    const DB::IColumn& column,
    ESimpleLogicalValueType type,
    EYsonFormat ysonFormat)
{
    switch (ysonFormat) {
        #define XX(format) \
            case EYsonFormat::format: \
                return ConvertCHColumnToAnyImpl<EYsonFormat::format>(column, type);

        XX(Binary)
        XX(Pretty)
        XX(Text)

        #undef XX
    }

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
