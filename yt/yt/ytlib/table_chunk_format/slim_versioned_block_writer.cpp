#include "slim_versioned_block_writer.h"

#include "private.h"

#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/client/table_client/schema.h>

#include <library/cpp/yt/memory/chunked_input_stream.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

struct TSlimVersionedBlockWriterTag
{ };

namespace {

size_t EstimateValueSize(const TUnversionedValue& value)
{
    if (IsStringLikeType(value.Type)) {
        return MaxVarUint32Size + value.Length;
    } else {
        return sizeof(TUnversionedValueData);
    }
}

void WriteNonNullUnversionedValue(
    char*& ptr,
    const TUnversionedValue& value,
    ESimpleLogicalValueType type)
{
    YT_ASSERT(value.Type != EValueType::Null);
    YT_ASSERT(value.Type == type);

    switch (type) {
        case ESimpleLogicalValueType::Void:
            break;

        case ESimpleLogicalValueType::Int64:
        case ESimpleLogicalValueType::Interval:
            YT_ASSERT(value.Type == EValueType::Int64);
            WritePod(ptr, value.Data.Int64);
            break;

        case ESimpleLogicalValueType::Uint64:
        case ESimpleLogicalValueType::Date:
        case ESimpleLogicalValueType::Datetime:
        case ESimpleLogicalValueType::Timestamp:
            YT_ASSERT(value.Type == EValueType::Uint64);
            WritePod(ptr, value.Data.Uint64);
            break;

        case ESimpleLogicalValueType::Double:
            YT_ASSERT(value.Type == EValueType::Double);
            WritePod(ptr, value.Data.Double);
            break;

        case ESimpleLogicalValueType::Float:
            YT_ASSERT(value.Type == EValueType::Double);
            WritePod(ptr, static_cast<float>(value.Data.Double));
            break;

        case ESimpleLogicalValueType::Boolean:
            YT_ASSERT(value.Type == EValueType::Boolean);
            WritePod(ptr, value.Data.Boolean);
            break;

        case ESimpleLogicalValueType::String:
        case ESimpleLogicalValueType::Any:
        case ESimpleLogicalValueType::Json:
        case ESimpleLogicalValueType::Utf8:
        case ESimpleLogicalValueType::Uuid:
            YT_ASSERT(IsStringLikeType(value.Type));
            ptr += WriteVarUint32(ptr, value.Length);
            WriteRef(ptr, TRef(value.Data.String, value.Length));
            break;

        case ESimpleLogicalValueType::Int8:
            YT_ASSERT(value.Type == EValueType::Int64);
            WritePod(ptr, static_cast<i8>(value.Data.Int64));
            break;

        case ESimpleLogicalValueType::Uint8:
            YT_ASSERT(value.Type == EValueType::Uint64);
            WritePod(ptr, static_cast<ui8>(value.Data.Uint64));
            break;

        case ESimpleLogicalValueType::Int16:
            YT_ASSERT(value.Type == EValueType::Int64);
            WritePod(ptr, static_cast<i16>(value.Data.Int64));
            break;

        case ESimpleLogicalValueType::Uint16:
            YT_ASSERT(value.Type == EValueType::Uint64);
            WritePod(ptr, static_cast<ui16>(value.Data.Uint64));
            break;

        case ESimpleLogicalValueType::Int32:
            YT_ASSERT(value.Type == EValueType::Int64);
            WritePod(ptr, static_cast<i32>(value.Data.Int64));
            break;

        case ESimpleLogicalValueType::Uint32:
            YT_ASSERT(value.Type == EValueType::Uint64);
            WritePod(ptr, static_cast<ui32>(value.Data.Uint64));
            break;

        default:
            THROW_ERROR_EXCEPTION("Unsupported simple logical type %Qlv",
                type);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TSlimVersionedBlockWriter::TDictionary
{
public:
    ui32 GetIndex(TUnversionedValueRange valueRange)
    {
        auto it = ValueRangeToIndex_.find(valueRange);
        if (it != ValueRangeToIndex_.end()) {
            return it->second;
        }

        valueRange = ValueRangePool_.Capture(valueRange);

        auto index = static_cast<ui32>(ValueRangeToIndex_.size());
        EmplaceOrCrash(ValueRangeToIndex_, valueRange, index);
        IndexToValueRange_.push_back(valueRange);

        return index;
    }

    ui32 GetIndex(const TUnversionedValue& value)
    {
        return GetIndex(MakeRange(&value, 1));
    }

    void Write(
        TChunkedOutputStream& stream,
        TChunkedOutputStream& offsetStream,
        const TCompactVector<ESimpleLogicalValueType, TypicalColumnCount>& logicalColumnTypes)
    {
        for (auto valueRange : IndexToValueRange_) {
            WritePod(offsetStream, static_cast<ui32>(stream.GetSize()));

            size_t estimatedSize = 0;
            for (const auto& value : valueRange) {
                estimatedSize +=
                    // Tag
                    sizeof(ui32) +
                    // Value
                    EstimateValueSize(value);
            }

            char* beginPtr = stream.Preallocate(estimatedSize);
            char* ptr = beginPtr;

            for (const auto& value : valueRange) {
                ui32 valueTag =
                    (value.Id << SlimVersionedIdValueTagShift) |
                    (value.Type == EValueType::Null ? SlimVersionedValueTagNull : 0) |
                    (Any(value.Flags & EValueFlags::Aggregate) ? SlimVersionedValueTagAggregate : 0) |
                    (value.Type != EValueType::TheBottom ? SlimVersionedValueTagDictionaryPayload : 0);
                ptr += WriteVarUint32(ptr, valueTag);
                if (value.Type != EValueType::Null && value.Type != EValueType::TheBottom) {
                    WriteNonNullUnversionedValue(ptr, value, logicalColumnTypes[value.Id]);
                }
            }

            auto actualSize = static_cast<size_t>(ptr - beginPtr);
            YT_VERIFY(actualSize <= estimatedSize);
            stream.Advance(actualSize);
        }

        // Sentinel
        WritePod(offsetStream, static_cast<ui32>(stream.GetSize()));
    }

private:
    THashMap<TUnversionedValueRange, ui32, TBitwiseUnversionedValueRangeHash, TBitwiseUnversionedValueRangeEqual> ValueRangeToIndex_;
    std::vector<TUnversionedValueRange> IndexToValueRange_;
    TChunkedMemoryPool ValueRangePool_{GetRefCountedTypeCookie<TSlimVersionedBlockWriterTag>()};
};

////////////////////////////////////////////////////////////////////////////////

class TSlimVersionedBlockWriter::TValueFrequencyCollector
{
public:
    TValueFrequencyCollector(
        TSlimVersionedWriterConfigPtr config,
        ESimpleLogicalValueType type)
        : Config_(std::move(config))
        , FixedSizeDictionaryType_(
            type == ESimpleLogicalValueType::Int64 ||
            type == ESimpleLogicalValueType::Uint64 ||
            type == ESimpleLogicalValueType::Interval ||
            type == ESimpleLogicalValueType::Date ||
            type == ESimpleLogicalValueType::Datetime ||
            type == ESimpleLogicalValueType::Timestamp|
            type == ESimpleLogicalValueType::Double)
        , StringLikeDictionaryType_(
            type == ESimpleLogicalValueType::String ||
            type == ESimpleLogicalValueType::Any ||
            type == ESimpleLogicalValueType::Json ||
            type == ESimpleLogicalValueType::Utf8 ||
            type == ESimpleLogicalValueType::Uuid)
    { }

    void CollectValue(const TUnversionedValue& value)
    {
        ++TotalCount_;
        ++ValueToCount_[value];
    }

    void Finish()
    {
        TopCountThreshold_ = static_cast<int>(Config_->TopValueQuantile * TotalCount_);
    }

    enum class EValueCategory
    {
        Regular,
        Top,
        Frequent,
    };

    EValueCategory GetValueCategory(const TUnversionedValue& value) const
    {
        auto type = value.Type;
        if (type == EValueType::Null) {
            return EValueCategory::Top;
        }

        auto count = GetOrDefault(ValueToCount_, value, 0);
        if (count >= TopCountThreshold_) {
            return EValueCategory::Top;
        }

        if (Config_->EnablePerValueDictionaryEncoding) {
            if ((FixedSizeDictionaryType_ || StringLikeDictionaryType_) && count >= DictionaryCountThreshold) {
                return EValueCategory::Frequent;
            }
            if (StringLikeDictionaryType_ && count >= 2 && value.Length >= DictionaryStringLengthThreshold) {
                return EValueCategory::Frequent;
            }
        }

        return EValueCategory::Regular;
    }

private:
    const TSlimVersionedWriterConfigPtr Config_;
    const bool FixedSizeDictionaryType_;
    const bool StringLikeDictionaryType_;

    THashMap<TUnversionedValue, int, TBitwiseUnversionedValueHash, TBitwiseUnversionedValueEqual> ValueToCount_;
    int TotalCount_ = 0;
    int TopCountThreshold_ = 0;

    static constexpr int DictionaryCountThreshold = 10;
    static constexpr int DictionaryStringLengthThreshold = 100;
};

////////////////////////////////////////////////////////////////////////////////

TSlimVersionedBlockWriter::TSlimVersionedBlockWriter(
    NTableClient::TSlimVersionedWriterConfigPtr config,
    TTableSchemaPtr schema,
    TMemoryUsageTrackerGuard guard)
    : TVersionedBlockWriterBase(
        std::move(schema),
        std::move(guard))
    , TimestampDataStream_(GetRefCountedTypeCookie<TSlimVersionedBlockWriterTag>())
    , RowBufferStream_(GetRefCountedTypeCookie<TSlimVersionedBlockWriterTag>())
    , StringPool_(GetRefCountedTypeCookie<TSlimVersionedBlockWriterTag>())
    , RowOffsetStream_(GetRefCountedTypeCookie<TSlimVersionedBlockWriterTag>())
    , RowDataStream_(GetRefCountedTypeCookie<TSlimVersionedBlockWriterTag>())
    , KeyDictionaryOffsetStream_(GetRefCountedTypeCookie<TSlimVersionedBlockWriterTag>())
    , KeyDictionaryDataStream_(GetRefCountedTypeCookie<TSlimVersionedBlockWriterTag>())
    , ValueDictionaryOffsetStream_(GetRefCountedTypeCookie<TSlimVersionedBlockWriterTag>())
    , ValueDictionaryDataStream_(GetRefCountedTypeCookie<TSlimVersionedBlockWriterTag>())
{
    LogicalColumnTypes_.reserve(Schema_->GetColumnCount());
    for (const auto& columnSchema : Schema_->Columns()) {
        LogicalColumnTypes_.push_back(columnSchema.CastToV1Type());
    }

    ValueFrequencyCollectors_.reserve(Schema_->GetColumnCount());
    for (int index = 0; index < Schema_->GetColumnCount(); ++index) {
        ValueFrequencyCollectors_.push_back(std::make_unique<TValueFrequencyCollector>(
            config,
            LogicalColumnTypes_[index]));
    }
}

TSlimVersionedBlockWriter::~TSlimVersionedBlockWriter() = default;

void TSlimVersionedBlockWriter::WriteRow(TVersionedRow row)
{
    ++RowCount_;
    ValueCount_ += row.GetValueCount();

    size_t bufferedRowSize =
        // Estimated values size
        sizeof(size_t) +
        // Delete timestamp count
        sizeof(int) +
        // Value count
        sizeof(int) +
        // Key values
        sizeof(TUnversionedValue) * Schema_->GetKeyColumnCount() +
        // Delete timestamp indices
        sizeof(ui32) * row.GetDeleteTimestampCount() +
        // Versioned values (timestamp index, unversioned value)
        (sizeof(ui32) + sizeof(TUnversionedValue)) * row.GetValueCount();

    char* ptr = RowBufferStream_.Preallocate(bufferedRowSize);

    size_t estimatedValuesSize = 0;
    for (const auto& value : row.Keys()) {
        estimatedValuesSize += EstimateValueSize(value);
    }
    for (const auto& value : row.Values()) {
        estimatedValuesSize += EstimateValueSize(value);
    }

    WritePod<size_t>(ptr, estimatedValuesSize);
    WritePod<int>(ptr, row.GetDeleteTimestampCount());
    WritePod<int>(ptr, row.GetValueCount());

    for (int index = 0; index < Schema_->GetKeyColumnCount(); ++index) {
        auto capturedValue = CaptureValue(row.Keys()[index]);
        ValueFrequencyCollectors_[index]->CollectValue(capturedValue);
        WritePod(ptr, capturedValue);
    }

    for (auto timestamp : row.DeleteTimestamps()) {
        WritePod<ui32>(ptr, GetTimestampIndex(timestamp));
        UpdateMinMaxTimestamp(timestamp);
    }

    for (const auto& value : row.Values()) {
        WritePod<ui32>(ptr, GetTimestampIndex(value.Timestamp));
        UpdateMinMaxTimestamp(value.Timestamp);
    }

    for (const auto& value : row.Values()) {
        auto capturedValue = CaptureValue(value);
        ValueFrequencyCollectors_[capturedValue.Id]->CollectValue(capturedValue);
        WritePod<TUnversionedValue>(ptr, capturedValue);
    }

    RowBufferStream_.Advance(bufferedRowSize);

    if (MemoryGuard_) {
        MemoryGuard_.SetSize(GetBlockSize());
    }
}

TBlock TSlimVersionedBlockWriter::FlushBlock()
{
    for (int index = 0; index < Schema_->GetColumnCount(); ++index) {
        ValueFrequencyCollectors_[index]->Finish();
    }

    TChunkedInputStream rowBufferInput(RowBufferStream_.Finish());

    std::vector<ui32> timestampIndicesScratch;
    std::vector<TUnversionedValue> valuesScratch;
    std::vector<TUnversionedValue> dictionaryValuesScratch;
    std::vector<TUnversionedValue> payloadValuesScratch;

    TDictionary keyDictionary;
    TDictionary valueDictionary;

    auto writeValues = [&] (
        char*& ptr,
        TUnversionedValueRange valueRange,
        TDictionary& dictionary)
    {
        dictionaryValuesScratch.clear();
        payloadValuesScratch.clear();

        for (const auto& value : valueRange) {
            const auto& frequencyCollector = ValueFrequencyCollectors_[value.Id];
            auto category = frequencyCollector->GetValueCategory(value);
            switch (category) {
                case TValueFrequencyCollector::EValueCategory::Regular:
                    dictionaryValuesScratch.push_back(value);
                    dictionaryValuesScratch.back().Type = EValueType::TheBottom;
                    payloadValuesScratch.push_back(value);
                    break;

                case TValueFrequencyCollector::EValueCategory::Top:
                    dictionaryValuesScratch.push_back(value);
                    break;

                case TValueFrequencyCollector::EValueCategory::Frequent: {
                    auto valueDictionaryIndex = dictionary.GetIndex(value);
                    auto valueDictionaryTag = valueDictionaryIndex << SlimVersionedDictionaryTagIndexShift;
                    ptr += WriteVarUint32(ptr, valueDictionaryTag);
                    break;
                }

                default:
                    YT_ABORT();
            }
        }

        {
            auto rowDictionaryIndex = dictionary.GetIndex(MakeRange(dictionaryValuesScratch));
            auto rowDictionaryTag = (rowDictionaryIndex << SlimVersionedDictionaryTagIndexShift) | SlimVersionedDictionaryTagEos;
            ptr += WriteVarUint32(ptr, rowDictionaryTag);
        }

        for (const auto& value : payloadValuesScratch) {
            WriteNonNullUnversionedValue(ptr, value, LogicalColumnTypes_[value.Id]);
        }
    };

    for (int rowIndex = 0; rowIndex < RowCount_; ++rowIndex) {
        WritePod(RowOffsetStream_, static_cast<ui32>(RowDataStream_.GetSize()));

        size_t estimatedValuesSize;
        ReadPod(rowBufferInput, estimatedValuesSize);

        int deleteTimestampCount;
        ReadPod(rowBufferInput, deleteTimestampCount);

        int valueCount;
        ReadPod(rowBufferInput, valueCount);

        size_t estimatedRowSize =
            estimatedValuesSize +
            // Key dictionary tag
            MaxVarUint32Size +
            // Delete timestamp count
            MaxVarUint32Size +
            // Delete timestamps
            MaxVarUint32Size * deleteTimestampCount +
            // Value timestamps
            MaxVarUint32Size * valueCount +
            // Value dictionary tags
            MaxVarUint32Size * valueCount;

        char* beginPtr = RowDataStream_.Preallocate(estimatedRowSize);
        char* ptr = beginPtr;

        // Key
        {
            EnsureVectorSize(valuesScratch, KeyColumnCount_);
            ReadRef(rowBufferInput, TMutableRef(valuesScratch.data(), sizeof(TUnversionedValue) * KeyColumnCount_));

            writeValues(
                ptr,
                MakeRange(valuesScratch.data(), KeyColumnCount_),
                keyDictionary);
        }

        // Delete timestamps
        {
            ptr += WriteVarUint32(ptr, deleteTimestampCount);
            EnsureVectorSize(timestampIndicesScratch, static_cast<size_t>(deleteTimestampCount));
            ReadRef(rowBufferInput, TMutableRef(timestampIndicesScratch.data(), sizeof(ui32) * deleteTimestampCount));
            for (int index = 0; index < deleteTimestampCount; ++index) {
                ptr += WriteVarUint32(ptr, timestampIndicesScratch[index]);
            }
        }

        // Values
        {
            EnsureVectorSize(timestampIndicesScratch, valueCount);
            ReadRef(rowBufferInput, TMutableRef(timestampIndicesScratch.data(), sizeof(ui32) * valueCount));

            EnsureVectorSize(valuesScratch, valueCount);
            ReadRef(rowBufferInput, TMutableRef(valuesScratch.data(), sizeof(TUnversionedValue) * valueCount));

            int currentIndex = 0;
            while (currentIndex < valueCount) {
                int beginIndex = currentIndex;
                int endIndex = currentIndex;

                auto timestampIndex = timestampIndicesScratch[currentIndex];
                ptr += WriteVarUint32(ptr, timestampIndex);

                while (endIndex < valueCount && timestampIndicesScratch[endIndex] == timestampIndex) {
                    ++endIndex;
                }

                writeValues(
                    ptr,
                    MakeRange(valuesScratch.data() + beginIndex, valuesScratch.data() + endIndex),
                    valueDictionary);

                currentIndex = endIndex;
            }
        }

        auto actualRowSize = static_cast<size_t>(ptr - beginPtr);
        YT_VERIFY(actualRowSize <= estimatedRowSize);
        RowDataStream_.Advance(actualRowSize);
    }

    // Sentinel
    WritePod(RowOffsetStream_, static_cast<ui32>(RowDataStream_.GetSize()));

    keyDictionary.Write(KeyDictionaryDataStream_, KeyDictionaryOffsetStream_, LogicalColumnTypes_);
    valueDictionary.Write(ValueDictionaryDataStream_, ValueDictionaryOffsetStream_, LogicalColumnTypes_);

    TSlimVersionedBlockHeader header{
        .RowOffsetsSize = static_cast<i32>(RowOffsetStream_.GetSize()),
        .RowDataSize = static_cast<i32>(RowDataStream_.GetSize()),
        .TimestampDataSize = static_cast<i32>(TimestampDataStream_.GetSize()),
        .KeyDictionaryOffsetsSize = static_cast<i32>(KeyDictionaryOffsetStream_.GetSize()),
        .KeyDictionaryDataSize = static_cast<i32>(KeyDictionaryDataStream_.GetSize()),
        .ValueDictionaryOffsetsSize = static_cast<i32>(ValueDictionaryOffsetStream_.GetSize()),
        .ValueDictionaryDataSize = static_cast<i32>(ValueDictionaryDataStream_.GetSize()),
        .RowCount = RowCount_,
        .ValueCount = ValueCount_,
        .ValueCountPerRowEstimate = std::max(2 * ValueCount_ / RowCount_, 1),
        .TimestampCount = static_cast<i32>(TimestampToIndex_.size()),
    };

    TChunkedOutputStream headerStream(GetRefCountedTypeCookie<TSlimVersionedBlockWriterTag>());
    WritePod(headerStream, header);

    std::vector<TSharedRef> blockParts;
    auto addStream = [&] (auto& stream) {
        WritePadding(stream, stream.GetSize());
        auto streamParts = stream.Finish();
        blockParts.insert(blockParts.end(), streamParts.begin(), streamParts.end());
    };
    addStream(headerStream);
    addStream(RowOffsetStream_);
    addStream(TimestampDataStream_);
    addStream(KeyDictionaryOffsetStream_);
    addStream(KeyDictionaryDataStream_);
    addStream(ValueDictionaryOffsetStream_);
    addStream(ValueDictionaryDataStream_);
    addStream(RowDataStream_);

    return TVersionedBlockWriterBase::FlushBlock(std::move(blockParts), {});
}

i64 TSlimVersionedBlockWriter::GetBlockSize() const
{
    return
        TimestampDataStream_.GetSize() +
        RowBufferStream_.GetSize() +
        StringPool_.GetSize();
}

TUnversionedValue TSlimVersionedBlockWriter::CaptureValue(TUnversionedValue value)
{
    if (!IsStringLikeType(value.Type)) {
        return value;
    }

    bool inlineHunk = IsInlineHunkValue(value);
    auto length = value.Length;
    char* beginPtr = StringPool_.AllocateUnaligned(length + (inlineHunk ? 1 : 0));
    char* ptr = beginPtr;
    if (inlineHunk) {
        *ptr++ = static_cast<char>(EHunkValueTag::Inline);
        ++value.Length;
    }
    ::memcpy(ptr, value.Data.String, length);
    value.Data.String = beginPtr;
    return value;
}

int TSlimVersionedBlockWriter::GetTimestampIndex(TTimestamp timestamp)
{
    if (auto it = TimestampToIndex_.find(timestamp); it != TimestampToIndex_.end()) {
        return it->second;
    }
    int index = std::ssize(TimestampToIndex_);
    WritePod(TimestampDataStream_, timestamp);
    EmplaceOrCrash(TimestampToIndex_, timestamp, index);
    return index;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
