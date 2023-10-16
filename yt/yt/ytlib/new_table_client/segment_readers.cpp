#include "segment_readers.h"

#define UNROLL_LOOPS

namespace NYT::NNewTableClient {

struct TWriteIdsTag { };
struct TRowToValueTag { };
struct TRowIndexTag { };

constexpr int UnpackSizeFactor = 2;

TCompressedVectorView CreateCompressedView(const ui64* ptr, ui32 size, ui8 width, bool newMeta)
{
    if (newMeta) {
        // TODO(lukyan): Remove these checks after some time.
        YT_VERIFY(GetCompressedVectorSize(ptr) == size);
        YT_VERIFY(GetCompressedVectorWidth(ptr) == width);
        return {ptr, size, width};
    } else {
        return TCompressedVectorView(ptr);
    }
}

////////////////////////////////////////////////////////////////////////////////

void CheckBatchSize(TRange<TReadSpan> spans, ui32 expectedBatchSize)
{
    Y_UNUSED(spans);
    Y_UNUSED(expectedBatchSize);
#ifndef NDEBUG
    ui32 batchSize = 0;
    for (auto [lower, upper] : spans) {
        batchSize += upper - lower;
    }

    YT_VERIFY(expectedBatchSize == batchSize);
#endif
}

////////////////////////////////////////////////////////////////////////////////

void DiffsToOffsets(TMutableRange<ui32> values, ui32 expectedPerItem, ui32 startOffset = 0)
{
    auto pivot = startOffset;
    for (auto& value : values) {
        pivot += expectedPerItem;
        value = pivot + ZigZagDecode32(value);
    }
}

#ifdef FULL_UNPACK
void TScanTimestampExtractor::InitSegment(const TTimestampMeta* meta, const char* data, TTmpBuffers* tmpBuffers, bool newMeta)
{
    RowOffset_ = meta->ChunkRowCount - meta->RowCount;
    SegmentRowLimit_ = meta->ChunkRowCount;

    const ui64* ptr = reinterpret_cast<const ui64*>(data);

    auto timestampsDictView = CreateCompressedView(
        ptr,
        meta->TimestampsDictSize,
        meta->TimestampsDictWidth,
        newMeta);
    ptr += timestampsDictView.GetSizeInWords();

    auto writeTimestampIdsView = CreateCompressedView(
        ptr,
        meta->WriteTimestampSize,
        meta->WriteTimestampWidth,
        newMeta);
    ptr += writeTimestampIdsView.GetSizeInWords();

    auto deleteTimestampIdsView = CreateCompressedView(
        ptr,
        meta->DeleteTimestampSize,
        meta->DeleteTimestampWidth,
        newMeta);
    ptr += deleteTimestampIdsView.GetSizeInWords();

    auto writeTimestampPerRowDiffsView = CreateCompressedView(
        ptr,
        meta->WriteOffsetDiffsSize,
        meta->WriteOffsetDiffsWidth,
        newMeta);
    ptr += writeTimestampPerRowDiffsView.GetSizeInWords();

    auto deleteTimestampPerRowDiffsView = CreateCompressedView(
        ptr,
        meta->DeleteOffsetDiffsSize,
        meta->DeleteOffsetDiffsWidth,
        newMeta);
    ptr += deleteTimestampPerRowDiffsView.GetSizeInWords();

    auto& timestampsDict = tmpBuffers->Values;
    auto& ids = tmpBuffers->Ids;

    timestampsDict.resize(timestampsDictView.GetSize());
    timestampsDictView.UnpackTo(timestampsDict.data());

    YT_VERIFY(writeTimestampPerRowDiffsView.GetSize() == meta->RowCount);
    YT_VERIFY(deleteTimestampPerRowDiffsView.GetSize() == meta->RowCount);

    {
        auto* writeTimestampOffsets = WriteTimestampOffsets_.Resize(writeTimestampPerRowDiffsView.GetSize() + 1);

        writeTimestampOffsets[0] = 0;
        writeTimestampPerRowDiffsView.UnpackTo(writeTimestampOffsets + 1);

        auto expectedCount = meta->ExpectedWritesPerRow;
        DiffsToOffsets(MakeMutableRange(writeTimestampOffsets + 1, meta->RowCount), expectedCount);

#ifndef NDEBUG
        for (size_t index = 0; index < writeTimestampPerRowDiffsView.GetSize(); ++index) {
            auto expected = GetOffset(writeTimestampPerRowDiffsView, expectedCount, index);
            YT_VERIFY(writeTimestampOffsets[index] == expected);
        }
#endif
    }

    WriteTimestamps_.Resize(writeTimestampIdsView.GetSize());
    {
        ids.resize(writeTimestampIdsView.GetSize());
        writeTimestampIdsView.UnpackTo(ids.data());

        for (size_t index = 0; index < ids.size(); ++index) {
            WriteTimestamps_[index] = meta->BaseTimestamp + timestampsDict[ids[index]];
        }
    }

    {
        auto* deleteTimestampOffsets = DeleteTimestampOffsets_.Resize(deleteTimestampPerRowDiffsView.GetSize() + 1);

        deleteTimestampOffsets[0] = 0;
        deleteTimestampPerRowDiffsView.UnpackTo(deleteTimestampOffsets + 1);

        auto expectedCount = meta->ExpectedDeletesPerRow;
        DiffsToOffsets(MakeMutableRange(deleteTimestampOffsets + 1, meta->RowCount), expectedCount);

#ifndef NDEBUG
        for (size_t index = 0; index < deleteTimestampPerRowDiffsView.GetSize(); ++index) {
            auto expected = GetOffset(deleteTimestampPerRowDiffsView, expectedCount, index);
            YT_VERIFY(deleteTimestampOffsets[index] == expected);
        }
#endif
    }

    DeleteTimestamps_.Resize(deleteTimestampIdsView.GetSize());
    {
        ids.resize(deleteTimestampIdsView.GetSize());
        deleteTimestampIdsView.UnpackTo(ids.data());

        for (size_t index = 0; index < ids.size(); ++index) {
            DeleteTimestamps_[index] = meta->BaseTimestamp + timestampsDict[ids[index]];
        }
    }
}
#endif

class TSpansSlice
{
public:
    TSpansSlice(const TSpansSlice&) = delete;
    TSpansSlice(TSpansSlice&&) = delete;

    TSpansSlice(
        TReadSpan* const spanItStart,
        TReadSpan* const spanItEnd,
        const ui32 batchSize,
        const ui32 savedUpperBound)
        : SpanItStart_(spanItStart)
        , SpanItEnd_(spanItEnd)
        , BatchSize_(batchSize)
        , SavedUpperBound_(savedUpperBound)
    { }

    ~TSpansSlice()
    {
        if (SavedUpperBound_ > 0) {
            SpanItEnd_->Upper = SavedUpperBound_;
        }
    }

    ui32 GetBatchSize() const
    {
        return BatchSize_;
    }

    TRange<TReadSpan> GetSpans() const
    {
        return {SpanItStart_, SpanItEnd_ + (SavedUpperBound_ > 0)};
    }

    ui32 GetSize() const
    {
        return SpanItEnd_ - SpanItStart_ + (SavedUpperBound_ > 0);
    }

private:
    TReadSpan* const SpanItStart_;
    TReadSpan* const SpanItEnd_;
    const ui32 BatchSize_;
    const ui32 SavedUpperBound_;
};

TSpansSlice GetBatchSlice(TMutableRange<TReadSpan> spans, ui32 rowLimit)
{
    auto spanIt = spans.begin();
    ui32 batchSize = 0;
    ui32 savedUpperBound = 0;
    while (spanIt != spans.end()) {
        auto [lower, upper] = *spanIt;

        if (upper <= rowLimit) {
            batchSize += upper - lower;
            ++spanIt;
            continue;
        } else if (lower < rowLimit) {
            batchSize += rowLimit - lower;
            savedUpperBound = spanIt->Upper;
            spanIt->Upper = rowLimit;
        }
        break;
    }

    return {spans.begin(), spanIt, batchSize, savedUpperBound};
}

template <class T, class TDict>
void DoInitDictValues(
    T* output,
    T baseValue,
    TDict&& dict,
    TCompressedVectorView ids,
    TRange<TReadSpan> offsetSpans)
{
    for (auto [lower, upper] : offsetSpans) {
        ids.UnpackTo(output, lower, upper);
        auto outputEnd = output + upper - lower;
        while (output != outputEnd) {
            *output = baseValue + dict[*output];
            ++output;
        }
    }
}

template <class T>
void InitDictValues(
    TMutableRange<T> output,
    T baseValue,
    TCompressedVectorView dictView,
    std::vector<T>* dict,
    TCompressedVectorView ids,
    TRange<TReadSpan> offsetSpans,
    size_t /*segmentSize*/)
{
    auto batchSize = output.size();

    if (dict->empty() && batchSize * UnpackSizeFactor > dictView.GetSize()) {
        dict->resize(dictView.GetSize());
        dictView.UnpackTo(dict->data());
    }

    if (!dict->empty()) {
        DoInitDictValues(output.Begin(), baseValue, *dict, ids, offsetSpans);
    } else {
        DoInitDictValues(output.Begin(), baseValue, dictView, ids, offsetSpans);
    }
}

ui32 DoInitTimestampOffsets(
    ui32 segmentRowOffset,
    ui32 expectedPerRow,
    TCompressedVectorView perRowDiffsView,
    ui32* output,
    TReadSpan* offsetsSpans,
    TRange<TReadSpan> spans)
{
    // First offset is zero.
    *output++ = 0;

    ui32 offset = 0;
    for (auto [lower, upper] : spans) {
        auto segmentLower = lower - segmentRowOffset;
        auto segmentUpper = upper - segmentRowOffset;

        auto startSegmentOffset = GetOffset(perRowDiffsView, expectedPerRow, segmentLower);

        perRowDiffsView.UnpackTo(output, segmentLower, segmentUpper);

        auto count = segmentUpper - segmentLower;
        DiffsToOffsets(
            MakeMutableRange(output, count),
            expectedPerRow,
            offset + expectedPerRow * segmentLower - startSegmentOffset);

#ifndef NDEBUG
        for (ui32 index = 0; index < count; ++index) {
            auto expected = GetOffset(perRowDiffsView, expectedPerRow, segmentLower + index + 1) -
                startSegmentOffset + offset;
            YT_VERIFY(output[index] == expected);
        }
#endif

        output += count;

        auto nextOffset = output[-1];

        auto endSegmentOffset = startSegmentOffset + nextOffset - offset;
        auto endSegmentOffsetExpected = GetOffset(perRowDiffsView, expectedPerRow, segmentUpper);
        YT_VERIFY(endSegmentOffset == endSegmentOffsetExpected);

        *offsetsSpans++ = {startSegmentOffset, endSegmentOffset};

        offset = nextOffset;
    }

    return offset;
}

void TScanTimestampExtractor::InitSegment(
    const TTimestampMeta* meta,
    const char* data,
    ui32 rowOffset,
    TMutableRange<TReadSpan> spans,
    TTmpBuffers* tmpBuffers,
    bool newMeta)
{
    RowOffset_ = rowOffset;

    auto& timestampsDict = tmpBuffers->Values;
    timestampsDict.clear();

    const ui64* ptr = reinterpret_cast<const ui64*>(data);

    auto timestampsDictView = CreateCompressedView(
        ptr,
        meta->TimestampsDictSize,
        meta->TimestampsDictWidth,
        newMeta);
    ptr += timestampsDictView.GetSizeInWords();

    auto writeTimestampIdsView = CreateCompressedView(
        ptr,
        meta->WriteTimestampSize,
        meta->WriteTimestampWidth,
        newMeta);
    ptr += writeTimestampIdsView.GetSizeInWords();

    auto deleteTimestampIdsView = CreateCompressedView(
        ptr,
        meta->DeleteTimestampSize,
        meta->DeleteTimestampWidth,
        newMeta);
    ptr += deleteTimestampIdsView.GetSizeInWords();

    auto writeTimestampPerRowDiffsView = CreateCompressedView(
        ptr,
        meta->WriteOffsetDiffsSize,
        meta->WriteOffsetDiffsWidth,
        newMeta);
    ptr += writeTimestampPerRowDiffsView.GetSizeInWords();

    auto deleteTimestampPerRowDiffsView = CreateCompressedView(
        ptr,
        meta->DeleteOffsetDiffsSize,
        meta->DeleteOffsetDiffsWidth,
        newMeta);
    ptr += deleteTimestampPerRowDiffsView.GetSizeInWords();

    YT_VERIFY(writeTimestampPerRowDiffsView.GetSize() == meta->RowCount);
    YT_VERIFY(deleteTimestampPerRowDiffsView.GetSize() == meta->RowCount);

    auto slice = GetBatchSlice(spans, meta->ChunkRowCount);
    auto batchSize = slice.GetBatchSize();
    // Segment can be initialized multiple times if block bound (of other columns) crosses segment.
    YT_VERIFY(!slice.GetSpans().empty());
    SegmentRowLimit_ = slice.GetSpans().end()[-1].Upper;

    tmpBuffers->DataSpans.resize(slice.GetSize());

    WriteTimestampOffsets_.Resize(batchSize + 1);

    // Unpack offsets according to spans.
    // Build offset spans from initial spans to unpack data pointed by offsets.
    auto writeTimestampCount = DoInitTimestampOffsets(
        meta->ChunkRowCount - meta->RowCount,
        meta->ExpectedWritesPerRow,
        writeTimestampPerRowDiffsView,
        WriteTimestampOffsets_.GetData(),
        tmpBuffers->DataSpans.data(),
        slice.GetSpans());

    WriteTimestamps_.Resize(writeTimestampCount);
    auto writeTimestamps = MakeMutableRange(WriteTimestamps_.GetData(), writeTimestampCount);

    InitDictValues(
        writeTimestamps,
        meta->BaseTimestamp,
        timestampsDictView,
        &timestampsDict,
        writeTimestampIdsView,
        tmpBuffers->DataSpans,
        meta->RowCount);

    DeleteTimestampOffsets_.Resize(batchSize + 1);

    auto deleteTimestampCount = DoInitTimestampOffsets(
        meta->ChunkRowCount - meta->RowCount,
        meta->ExpectedDeletesPerRow,
        deleteTimestampPerRowDiffsView,
        DeleteTimestampOffsets_.GetData(),
        tmpBuffers->DataSpans.data(),
        slice.GetSpans());

    DeleteTimestamps_.Resize(deleteTimestampCount);
    auto deleteTimestamps = MakeMutableRange(DeleteTimestamps_.GetData(), deleteTimestampCount);

    InitDictValues(
        deleteTimestamps,
        meta->BaseTimestamp,
        timestampsDictView,
        &timestampsDict,
        deleteTimestampIdsView,
        tmpBuffers->DataSpans,
        meta->RowCount);
}

template <class T>
const ui64* TScanIntegerExtractor<T>::GetEndPtr(const TIntegerMeta* meta, const ui64* ptr, bool newMeta)
{
    if (meta->Direct) {
        auto valuesView = CreateCompressedView(ptr, meta->ValuesSize, meta->ValuesWidth, newMeta);
        ptr += valuesView.GetSizeInWords();
        ptr += GetBitmapSize(valuesView.GetSize());
    } else {
        auto valuesView = CreateCompressedView(ptr, meta->ValuesSize, meta->ValuesWidth, newMeta);
        ptr += valuesView.GetSizeInWords();
        auto idsView = CreateCompressedView(ptr, meta->IdsSize, meta->IdsWidth, newMeta);
        ptr += idsView.GetSizeInWords();
    }

    return ptr;
}

#ifdef FULL_UNPACK
template <class T>
const ui64* TScanIntegerExtractor<T>::InitData(const TIntegerMeta* meta, const ui64* ptr, TTmpBuffers* tmpBuffers, bool newMeta)
{
    auto& values = tmpBuffers->Values;
    auto& ids = tmpBuffers->Ids;

    const auto* integerMeta = static_cast<const TIntegerMeta*>(meta);
    auto baseValue = integerMeta->BaseValue;

    if (meta->Direct) {
        auto valuesView = CreateCompressedView(ptr, meta->ValuesSize, meta->ValuesWidth, newMeta);
        ptr += valuesView.GetSizeInWords();

        size_t itemCount = valuesView.GetSize();

        NullBits_ = TBitmap(ptr);
        ptr += GetBitmapSize(itemCount);

        auto [items] = AllocateCombined<T>(&Items_, itemCount);

        valuesView.UnpackTo(items);

#ifdef UNROLL_LOOPS
        auto tailCount = itemCount % 8;
        auto itemsEnd = items + itemCount - tailCount;

#define ITERATION \
        *items = ConvertInt<T>(baseValue + *items); \
        ++items;

        while (items < itemsEnd) {
            for (int index = 0; index < 8; ++index) {
                ITERATION
            }
        }

        for (int index = 0; index < static_cast<int>(tailCount); ++index) {
            ITERATION
        }
#undef ITERATION

#else
        for (size_t index = 0; index < itemCount; ++index) {
            items[index] = ConvertInt<T>(baseValue + items[index]);
        }
#endif

    } else {
        auto valuesView = CreateCompressedView(ptr, meta->ValuesSize, meta->ValuesWidth, newMeta);
        ptr += valuesView.GetSizeInWords();

        auto idsView = CreateCompressedView(ptr, meta->IdsSize, meta->IdsWidth, newMeta);
        ptr += idsView.GetSizeInWords();
        auto itemCount = idsView.GetSize();

        auto [items, nullBits] = AllocateCombined<T, TBit>(&Items_, itemCount, itemCount);

        NullBits_ = nullBits;

        values.resize(1 + valuesView.GetSize());
        // Zero id denotes null value and allows to eliminate extra branch.
        values[0] = 0;
        valuesView.UnpackTo(values.data() + 1);

        ids.resize(idsView.GetSize());
        idsView.UnpackTo(ids.data());

#ifdef UNROLL_LOOPS
        auto tailCount = itemCount % 8;
        auto itemsEnd = items + itemCount - tailCount;

        ui8* nullBitsData = nullBits.GetData();
        auto idsPtr = ids.data();

#define ITERATION(count) { \
            ui8 word = 0; \
            for (int index = 0; index < (count); ++index) { \
                auto id = *idsPtr++; \
                word |= ui8(!id) << index; \
                *items++ = ConvertInt<T>(baseValue + values[id]); \
            } \
            *nullBitsData++ = word; \
        }

        while (items < itemsEnd) {
            ITERATION(8)
        }

        ITERATION(static_cast<int>(tailCount))

#undef ITERATION

#else
        for (size_t index = 0; index < itemCount; ++index) {
            auto id = ids[index];
            nullBits.Set(index, id == 0);
            items[index] = ConvertInt<T>(baseValue + values[id]);
        }
#endif

    }

    return ptr;
}
#endif

template <class T, class TFunctor>
void UnpackDict(
    T* idsBuffer,
    TMutableBitmap nullBits,
    TCompressedVectorView idsView,
    TRange<TReadSpan> spans,
    TFunctor&& functor)
{
    ui32 offset = 0;
    for (auto [lower, upper] : spans) {
        idsView.UnpackTo(idsBuffer + offset, lower, upper);

        auto count = upper - lower;
        const auto offsetEnd = offset + count;

#ifdef UNROLL_LOOPS
        const auto alignedStart = AlignUp<ui32>(offset, 8);
        const auto alignedEnd = AlignDown<ui32>(offsetEnd, 8);

        if (alignedStart < alignedEnd) {
            while (offset != alignedStart) {
                auto id = idsBuffer[offset];
                nullBits.Set(offset, id == 0);
                functor(offset, id);
                ++offset;
            }

#define ITERATION(count) { \
                ui8 word = 0; \
                for (int index = 0; index < (count); ++index) { \
                    auto id = idsBuffer[offset]; \
                    word |= ui8(!id) << index; \
                    functor(offset, id); \
                    ++offset; \
                } \
                *nullBitsData++ = word; \
            }

            ui8* nullBitsData = nullBits.GetData() + offset / 8;

            do {
                ITERATION(8)
            } while (offset < alignedEnd);

            auto tailCount = static_cast<int>(offsetEnd - offset);
            if (tailCount) {
                ITERATION(tailCount);
            }
        } else {
            while (offset != offsetEnd) {
                auto id = idsBuffer[offset];
                nullBits.Set(offset, id == 0);
                functor(offset, id);
                ++offset;
            }
        }

#undef ITERATION

#else
        while (offset != offsetEnd) {
            auto id = idsBuffer[offset];
            nullBits.Set(offset, id == 0);
            functor(offset, id);
            ++offset;
        }
#endif

    }
}

template <class T>
const ui64* TScanIntegerExtractor<T>::InitData(
    const TIntegerMeta* meta,
    const ui64* ptr,
    TRange<TReadSpan> spans,
    ui32 batchSize,
    TTmpBuffers* /*tmpBuffers*/,
    ui32 segmentChunkRowCount,
    bool newMeta)
{
    CheckBatchSize(spans, batchSize);

    const auto* integerMeta = static_cast<const TIntegerMeta*>(meta);
    auto baseValue = integerMeta->BaseValue;

    if (meta->Direct) {
        auto valuesView = CreateCompressedView(ptr, meta->ValuesSize, meta->ValuesWidth, newMeta);
        ptr += valuesView.GetSizeInWords();

        TBitmap nullBitsView(ptr);
        ptr += GetBitmapSize(valuesView.GetSize());

        auto [items, nullBits] = AllocateCombined<T, TBit>(&Items_, batchSize, batchSize);
        NullBits_ = nullBits;

        ui32 offset = 0;
        for (auto [lower, upper] : spans) {
            valuesView.UnpackTo(items + offset, lower, upper);

            for (size_t index = 0; index < upper - lower; ++index) {
                items[offset + index] = ConvertInt<T>(baseValue + items[offset + index]);
            }

            CopyBitmap(
                nullBits.GetData(),
                offset,
                nullBitsView.GetData(),
                lower,
                upper - lower);

            offset += upper - lower;
        }
    } else {
        auto valuesView = CreateCompressedView(ptr, meta->ValuesSize, meta->ValuesWidth, newMeta);
        ptr += valuesView.GetSizeInWords();

        auto idsView = CreateCompressedView(ptr, meta->IdsSize, meta->IdsWidth, newMeta);
        ptr += idsView.GetSizeInWords();

        auto [items, nullBits] = AllocateCombined<T, TBit>(&Items_, batchSize, batchSize);
        NullBits_ = nullBits;

        // Even if segment is read completely it can be initialized multiple times.
        if (SegmentChunkRowCount_ != segmentChunkRowCount) {
            ValuesDict_.clear();
            SegmentChunkRowCount_ = segmentChunkRowCount;
        }

        if (ValuesDict_.empty() && batchSize * UnpackSizeFactor > valuesView.GetSize()) {
            ValuesDict_.resize(valuesView.GetSize() + 1);
            // Zero id denotes null value and allows to eliminate extra branch.
            ValuesDict_[0] = 0;
            valuesView.UnpackTo(ValuesDict_.data() + 1);
        }

        if (!ValuesDict_.empty()) {
            const auto* valuesDict = ValuesDict_.data();

            UnpackDict(items, nullBits, idsView, spans, [&, items = items] (ui32 index, ui32 id) {
                items[index] = ConvertInt<T>(baseValue + valuesDict[id]);
            });
        } else {
            UnpackDict(items, nullBits, idsView, spans, [&, items = items] (ui32 index, ui32 id) {
                if (id > 0) {
                    items[index] = ConvertInt<T>(baseValue + valuesView[id - 1]);
                }
            });

        }
    }

    return ptr;
}

template <class T>
void TScanIntegerExtractor<T>::InitNullData()
{
    auto [items, nullBits] = AllocateCombined<T, TBit>(&Items_, 1, 1);
    NullBits_ = nullBits;

    items[0] = 0;
    nullBits.Set(0, true);
}

template
class TScanIntegerExtractor<i64>;

template
class TScanIntegerExtractor<ui64>;

const ui64* TScanDataExtractor<EValueType::Double>::GetEndPtr(
    const TEmptyMeta* /*meta*/,
    const ui64* ptr,
    bool /*newMeta*/)
{
    ui64 count = *ptr++;
    ptr += count;
    ptr += GetBitmapSize(count);
    return ptr;
}

#ifdef FULL_UNPACK
const ui64* TScanDataExtractor<EValueType::Double>::InitData(
    const TEmptyMeta* /*meta*/,
    const ui64* ptr,
    TTmpBuffers* /*tmpBuffers*/,
    bool /*newMeta*/)
{
    // No dictionary mode for double.
    ui64 count = *ptr++;
    Items_ = reinterpret_cast<const double*>(ptr);
    ptr += count;

    NullBits_ = TBitmap(ptr);
    ptr += GetBitmapSize(count);

    return ptr;
}
#endif

const ui64* TScanDataExtractor<EValueType::Double>::InitData(
    const TEmptyMeta* /*meta*/,
    const ui64* ptr,
    TRange<TReadSpan> spans,
    ui32 batchSize,
    TTmpBuffers* /*tmpBuffers*/,
    ui32 /*segmentChunkRowCount*/,
    bool /*newMeta*/)
{
    CheckBatchSize(spans, batchSize);

    // No dictionary mode for double.
    ui64 count = *ptr++;
    auto* itemsData  = reinterpret_cast<const double*>(ptr);
    ptr += count;

    TBitmap nullBitsView(ptr);
    ptr += GetBitmapSize(count);

    auto [items, nullBits] = AllocateCombined<double, TBit>(&Holder_, batchSize, batchSize);
    Items_ = items;
    NullBits_ = nullBits;

    int offset = 0;
    for (auto [lower, upper] : spans) {
        for (ui32 index = 0; index < upper - lower; ++index) {
            items[offset + index] = itemsData[lower + index];
        }

        CopyBitmap(
            nullBits.GetData(),
            offset,
            nullBitsView.GetData(),
            lower,
            upper - lower);


        offset += upper - lower;
    }

    return ptr;
}

void TScanDataExtractor<EValueType::Double>::InitNullData()
{
    auto [items, nullBits] = AllocateCombined<double, TBit>(&Holder_, 1, 1);

    items[0] = 0;
    nullBits.Set(0, true);

    Items_ = items;
    NullBits_ = nullBits;
}

ui64 TScanDataExtractor<EValueType::Boolean>::NullBooleanSegmentData;

const ui64* TScanDataExtractor<EValueType::Boolean>::GetEndPtr(const TEmptyMeta* /*meta*/, const ui64* ptr, bool /*newMeta*/)
{
    ui64 count = *ptr++;
    ptr += GetBitmapSize(count);
    ptr += GetBitmapSize(count);

    return ptr;
}

#ifdef FULL_UNPACK
const ui64* TScanDataExtractor<EValueType::Boolean>::InitData(
    const TEmptyMeta* /*meta*/,
    const ui64* ptr,
    TTmpBuffers* /*tmpBuffers*/,
    bool /*newMeta*/)
{
    ui64 count = *ptr++;
    Items_ = TBitmap(ptr);
    ptr += GetBitmapSize(count);

    NullBits_ = TBitmap(ptr);
    ptr += GetBitmapSize(count);

    return ptr;
}
#endif

const ui64* TScanDataExtractor<EValueType::Boolean>::InitData(
    const TEmptyMeta* /*meta*/,
    const ui64* ptr,
    TRange<TReadSpan> spans,
    ui32 batchSize,
    TTmpBuffers* /*tmpBuffers*/,
    ui32 /*segmentChunkRowCount*/,
    bool /*newMeta*/)
{
    CheckBatchSize(spans, batchSize);

    ui64 count = *ptr++;
    TBitmap itemsData(ptr);
    ptr += GetBitmapSize(count);

    TBitmap nullBitsView(ptr);
    ptr += GetBitmapSize(count);

    auto [items, nullBits] = AllocateCombined<TBit, TBit>(&Holder_, batchSize, batchSize);

    Items_ = items;
    NullBits_ = nullBits;

    int offset = 0;
    for (auto [lower, upper] : spans) {
        CopyBitmap(
            items.GetData(),
            offset,
            itemsData.GetData(),
            lower,
            upper - lower);

        CopyBitmap(
            nullBits.GetData(),
            offset,
            nullBitsView.GetData(),
            lower,
            upper - lower);

        offset += upper - lower;
    }

    return ptr;
}

void TScanDataExtractor<EValueType::Boolean>::InitNullData()
{
    TMutableBitmap bitmap(&NullBooleanSegmentData);
    bitmap.Set(0, true);

    Items_ = bitmap;
    NullBits_ = bitmap;
}

template <class T>
size_t UnpackBitVector(const ui64* ptr, ui32 size, ui8 width, std::vector<T>* container, bool newMeta)
{
    TCompressedVectorView view = CreateCompressedView(ptr, size, width, newMeta);
    container->resize(view.GetSize());
    view.UnpackTo(container->data());
    return view.GetSizeInWords();
}

#ifdef FULL_UNPACK
void TScanBlobExtractor::InitData(const TBlobMeta* meta, const ui64* ptr, TTmpBuffers* tmpBuffers, bool newMeta)
{
    const auto expectedLength = meta->ExpectedLength;

    auto& offsets = tmpBuffers->Offsets;

    if (meta->Direct) {
        ptr += UnpackBitVector(ptr, meta->OffsetsSize, meta->OffsetsWidth, &offsets, newMeta);
        auto valueCount = offsets.size();

        auto [items] = AllocateCombined<TItem>(&Items_, valueCount);

        ui32 begin = 0;
        for (size_t index = 0; index < valueCount; ++index) {
            ui32 end = GetOffsetNonZero(offsets, expectedLength, index + 1);
            items[index] = {begin, end};
            begin = end;
        }

        NullBits_ = TBitmap(ptr);
        ptr += GetBitmapSize(valueCount);

    } else {
        auto& ids = tmpBuffers->Ids;

        ptr += UnpackBitVector(ptr, meta->IdsSize, meta->IdsWidth, &ids, newMeta);
        ptr += UnpackBitVector(ptr, meta->OffsetsSize, meta->OffsetsWidth, &offsets, newMeta);

        auto valueCount = ids.size();

        auto [items, nullBits] = AllocateCombined<TItem, TBit>(&Items_, valueCount, valueCount);

        for (size_t index = 0; index < valueCount; ++index) {
            auto id = ids[index];
            nullBits.Set(index, id == 0);

            if (id > 0) {
                items[index] = {
                    GetOffset(offsets, expectedLength, id - 1),
                    GetOffsetNonZero(offsets, expectedLength, id)};
            } else {
                items[index] = {0, 0};
            }
        }

        NullBits_ = nullBits;
    }

    Data_ = reinterpret_cast<const char*>(ptr);
}
#endif

void TScanBlobExtractor::InitData(
    const TBlobMeta* meta,
    const ui64* ptr,
    TRange<TReadSpan> spans,
    ui32 batchSize,
    TTmpBuffers* tmpBuffers,
    ui32 segmentChunkRowCount,
    bool newMeta)
{
    const auto expectedLength = meta->ExpectedLength;

    CheckBatchSize(spans, batchSize);

    auto [items, nullBits] = AllocateCombined<TItem, TBit>(&Items_, batchSize, batchSize);

    if (meta->Direct) {
        auto offsetsView = CreateCompressedView(ptr, meta->OffsetsSize, meta->OffsetsWidth, newMeta);
        ptr += offsetsView.GetSizeInWords();
        TBitmap nullBitsView(ptr);
        ptr += GetBitmapSize(offsetsView.GetSize());

        int offset = 0;
        for (auto [lower, upper] : spans) {
            ui32 begin = GetOffset(offsetsView, expectedLength, lower);
            for (ui32 index = 0; index < upper - lower; ++index) {
                ui32 end = GetOffsetNonZero(offsetsView, expectedLength, lower + index + 1);
                items[offset + index] = {begin, end};
                begin = end;

                nullBits.Set(offset + index, nullBitsView[lower + index]);
            }

            offset += upper - lower;
        }
    } else {
        auto idsView = CreateCompressedView(ptr, meta->IdsSize, meta->IdsWidth, newMeta);
        ptr += idsView.GetSizeInWords();
        auto offsetsView = CreateCompressedView(ptr, meta->OffsetsSize, meta->OffsetsWidth, newMeta);;
        ptr += offsetsView.GetSizeInWords();

        auto& ids = tmpBuffers->Ids;
        ids.resize(batchSize);

        // Even if segment is read completely it can be initialized multiple times.
        if (SegmentChunkRowCount_ != segmentChunkRowCount) {
            OffsetsDict_.clear();
            SegmentChunkRowCount_ = segmentChunkRowCount;
        }

        if (OffsetsDict_.empty() && batchSize * UnpackSizeFactor > offsetsView.GetSize()) {
            OffsetsDict_.resize(offsetsView.GetSize() + 1);
            // Zero id denotes null value and allows to eliminate extra branch.
            OffsetsDict_[0] = 0;
            offsetsView.UnpackTo(OffsetsDict_.data() + 1);
        }

        if (!OffsetsDict_.empty()) {
            const auto* offsets = OffsetsDict_.data() + 1;

            UnpackDict(ids.data(), nullBits, idsView, spans, [&, items = items] (ui32 index, ui32 id) {
                // FIXME(lukyan): Cannot remove extra branch because of ui32 index.
                if (id > 0) {
                    items[index] = {
                        GetOffset(offsets, expectedLength, id - 1),
                        GetOffsetNonZero(offsets, expectedLength, id)};
                } else {
                    items[index] = {0, 0};
                }
            });
        } else {
            UnpackDict(ids.data(), nullBits, idsView, spans, [&, items = items] (ui32 index, ui32 id) {
                if (id > 0) {
                    items[index] = {
                        GetOffset(offsetsView, expectedLength, id - 1),
                        GetOffsetNonZero(offsetsView, expectedLength, id)};
                } else {
                    items[index] = {0, 0};
                }
            });
        }
    }

    NullBits_ = nullBits;
    Data_ = reinterpret_cast<const char*>(ptr);
}

void TScanBlobExtractor::InitNullData()
{
    auto [items, nullBits] = AllocateCombined<TItem, TBit>(&Items_, 1, 1);

    items[0] = TItem{0, 0};
    nullBits.Set(0, true);

    NullBits_ = nullBits;
    Data_ = nullptr;
}

#ifdef FULL_UNPACK
const ui64* TScanKeyIndexExtractor::InitIndex(const TKeyIndexMeta* meta, const ui64* ptr, bool newMeta)
{
    SegmentRowLimit_ = meta->ChunkRowCount;
    ui32 rowOffset = meta->ChunkRowCount - meta->RowCount;

    if (meta->Dense) {
        Count_ = meta->RowCount;
        auto rowIndexData = RowIndexes_.Resize(Count_ + 1, GetRefCountedTypeCookie<TRowIndexTag>());
        auto rowIndexDataEnd = rowIndexData + Count_;

#define ITERATION *rowIndexData++ = rowOffset++;
        while (rowIndexData + 4 < rowIndexDataEnd) {
            ITERATION
            ITERATION
            ITERATION
            ITERATION
        }

        while (rowIndexData < rowIndexDataEnd) {
            ITERATION
        }
#undef ITERATION
    } else {
        auto rowIndexView = CreateCompressedView(ptr, meta->RowIndexesSize, meta->RowIndexesWidth, newMeta);
        ptr += rowIndexView.GetSizeInWords();

        Count_ = rowIndexView.GetSize();
        auto rowIndexData = RowIndexes_.Resize(Count_ + 1, GetRefCountedTypeCookie<TRowIndexTag>());
        auto rowIndexDataEnd = rowIndexData + Count_;

        rowIndexView.UnpackTo(rowIndexData);

#define ITERATION *rowIndexData++ += rowOffset;
        while (rowIndexData + 4 < rowIndexDataEnd) {
            ITERATION
            ITERATION
            ITERATION
            ITERATION
        }

        while (rowIndexData < rowIndexDataEnd) {
            ITERATION
        }
#undef ITERATION
    }

    RowIndexes_[Count_] = meta->ChunkRowCount;

    return ptr;
}
#endif

const ui64* TScanKeyIndexExtractor::InitIndex(
    const TKeyIndexMeta* meta,
    const ui64* ptr,
    ui32 rowOffset,
    TMutableRange<TReadSpan> spans,
    TTmpBuffers* tmpBuffers,
    bool newMeta)
{
    ui32 segmentRowOffset = meta->ChunkRowCount - meta->RowCount;

    auto slice = GetBatchSlice(spans, meta->ChunkRowCount);
    ui32 batchSize = slice.GetBatchSize();
    // Segment can be initialized multiple times if block bound (of other columns) crosses segment.
    YT_VERIFY(!slice.GetSpans().empty());
    SegmentRowLimit_ = slice.GetSpans().end()[-1].Upper;

    tmpBuffers->DataSpans.resize(slice.GetSize());
    auto* offsetsSpans = tmpBuffers->DataSpans.data();

    if (meta->Dense) {
        auto rowIndexes = RowIndexes_.Resize(batchSize + 1, GetRefCountedTypeCookie<TRowIndexTag>());
        auto rowIndexesEnd = rowIndexes + batchSize;

#define ITERATION *rowIndexes++ = rowOffset++;
        while (rowIndexes + 4 < rowIndexesEnd) {
            ITERATION
            ITERATION
            ITERATION
            ITERATION
        }

        while (rowIndexes < rowIndexesEnd) {
            ITERATION
        }
#undef ITERATION

        *rowIndexes = rowOffset;
        Count_ = rowIndexes - RowIndexes_.GetData();
        YT_ASSERT(Count_ == batchSize);

        for (auto [lower, upper] : slice.GetSpans()) {
            auto segmentLower = lower - segmentRowOffset;
            auto segmentUpper = upper - segmentRowOffset;
            *offsetsSpans++ = {segmentLower, segmentUpper};
        }
    } else {
        auto rowIndexesView = CreateCompressedView(ptr, meta->RowIndexesSize, meta->RowIndexesWidth, newMeta);
        ptr += rowIndexesView.GetSizeInWords();

        ui32 segmentItemCount = rowIndexesView.GetSize();
        auto bufferSize = std::min(segmentItemCount, batchSize) + 1;

        auto* rowIndexes = RowIndexes_.Resize(bufferSize, GetRefCountedTypeCookie<TRowIndexTag>());
        auto* rowIndexesBufferEnd = rowIndexes + bufferSize;

        // Source spans can be clashed if they are in one RLE range.
        // So offsetsSpans.size() will be less than or equal to slice.GetSpans().size().
        ui32 lastSegmentRowIndex = 0;

        // First item is always zero.
        YT_VERIFY(rowIndexesView[0] == 0);
        YT_VERIFY(segmentItemCount > 0);
        ui32 valueOffset = 1;

        for (auto [lower, upper] : slice.GetSpans()) {
            auto segmentLower = lower - segmentRowOffset;
            auto segmentUpper = upper - segmentRowOffset;


            if (segmentUpper <= lastSegmentRowIndex) {
                rowOffset += upper - lower;
                continue;
            } else if (segmentLower < lastSegmentRowIndex) {
                rowOffset += lastSegmentRowIndex - segmentLower;
                segmentLower = lastSegmentRowIndex;
            }

            valueOffset = ExponentialSearch(valueOffset, segmentItemCount, [&] (ui32 valueOffset) {
                return rowIndexesView[valueOffset] <= segmentLower;
            });

            if (offsetsSpans > tmpBuffers->DataSpans.data()) {
                YT_ASSERT(offsetsSpans[-1].Upper <= valueOffset - 1);
            }

            ui32 valueOffsetEnd = ExponentialSearch(valueOffset, segmentItemCount, [&] (ui32 valueOffset) {
                return rowIndexesView[valueOffset] < segmentUpper;
            });

            if (valueOffsetEnd != segmentItemCount) {
                lastSegmentRowIndex = rowIndexesView[valueOffsetEnd];
            } else {
                lastSegmentRowIndex = meta->RowCount;
            }

            YT_ASSERT(rowIndexes < rowIndexesBufferEnd);
            *rowIndexes++ = rowOffset;

            rowIndexesView.UnpackTo(rowIndexes, valueOffset, valueOffsetEnd);

            auto* rowIndexesEnd = rowIndexes + valueOffsetEnd - valueOffset;
            while (rowIndexes != rowIndexesEnd) {
                YT_ASSERT(rowIndexes < rowIndexesBufferEnd);
                YT_ASSERT(*rowIndexes + rowOffset >= segmentLower);

                *rowIndexes++ += rowOffset - segmentLower;
            }

            *offsetsSpans++ = {valueOffset - 1, valueOffsetEnd};

            rowOffset += segmentUpper - segmentLower;
            valueOffset = valueOffsetEnd;
        }

        tmpBuffers->DataSpans.resize(offsetsSpans - tmpBuffers->DataSpans.data());

        YT_VERIFY(rowIndexes > RowIndexes_.GetData());
        YT_VERIFY(rowIndexes[-1] < rowOffset);
        YT_VERIFY(rowIndexes < rowIndexesBufferEnd);
        *rowIndexes = rowOffset;
        Count_ = rowIndexes - RowIndexes_.GetData();

#ifndef NDEBUG
        ui32 dataBatchSize = 0;
        for (auto [lower, upper] : tmpBuffers->DataSpans) {
            dataBatchSize += upper - lower;
        }
        YT_VERIFY(dataBatchSize == Count_);
#endif
    };

    return ptr;
}

void TScanKeyIndexExtractor::InitNullIndex()
{
    Count_ = 1;
    RowIndexes_.Resize(2);
    RowIndexes_[0] = 0;
    RowIndexes_[1] = std::numeric_limits<ui32>::max();
    SegmentRowLimit_ = std::numeric_limits<ui32>::max();
}

#ifdef FULL_UNPACK
template <bool Aggregate>
const ui64* TScanVersionExtractor<Aggregate>::InitVersion(const TMultiValueIndexMeta* meta, const ui64* ptr, bool newMeta)
{
    auto writeTimestampIdsView = CreateCompressedView(ptr, meta->WriteTimestampIdsSize, meta->WriteTimestampIdsWidth, newMeta);
    ptr += writeTimestampIdsView.GetSizeInWords();

    auto count = writeTimestampIdsView.GetSize();
    WriteTimestampIds_.Resize(count, GetRefCountedTypeCookie<TWriteIdsTag>());
    writeTimestampIdsView.UnpackTo(WriteTimestampIds_.GetData());

    if constexpr (Aggregate) {
        this->AggregateBits_ = TBitmap(ptr);
        ptr += GetBitmapSize(count);
    }

    return ptr;
}
#endif

template <bool Aggregate>
const ui64* TScanVersionExtractor<Aggregate>::InitVersion(
    const TMultiValueIndexMeta* meta,
    const ui64* ptr,
    TRange<TReadSpan> spans,
    ui32 batchSize,
    bool newMeta)
{
    CheckBatchSize(spans, batchSize);

    auto writeTimestampIdsView = CreateCompressedView(ptr, meta->WriteTimestampIdsSize, meta->WriteTimestampIdsWidth, newMeta);
    ptr += writeTimestampIdsView.GetSizeInWords();
    TBitmap aggregateBitsView;

    ui32* writeTimestampIds = nullptr;
    TMutableBitmap aggregateBits;

    if constexpr (Aggregate) {
        aggregateBitsView = TBitmap(ptr);
        ptr += GetBitmapSize(writeTimestampIdsView.GetSize());

        std::tie(writeTimestampIds, aggregateBits) = AllocateCombined<ui32, TBit>(
            &WriteTimestampIds_,
            batchSize,
            batchSize);
        this->AggregateBits_ = aggregateBits;
    } else {
        writeTimestampIds = WriteTimestampIds_.Resize(batchSize, GetRefCountedTypeCookie<TWriteIdsTag>());
    }

    ui32 offset = 0;
    for (auto [lower, upper] : spans) {
        writeTimestampIdsView.UnpackTo(writeTimestampIds, lower, upper);
        writeTimestampIds += upper - lower;

        if constexpr (Aggregate) {
            CopyBitmap(
                aggregateBits.GetData(),
                offset,
                aggregateBitsView.GetData(),
                lower,
                upper - lower);

            offset += upper - lower;
        }
    }

    return ptr;
}

template class TScanVersionExtractor<false>;
template class TScanVersionExtractor<true>;

#ifdef FULL_UNPACK
const ui64* TScanMultiValueIndexExtractor::InitIndex(
    const TMultiValueIndexMeta* meta,
    const ui64* ptr,
    TTmpBuffers* tmpBuffers,
    bool newMeta)
{
    SegmentRowLimit_ = meta->ChunkRowCount;

    ui32 rowOffset = meta->ChunkRowCount - meta->RowCount;

    auto& offsets = tmpBuffers->Offsets;

    ptr += UnpackBitVector(ptr, meta->OffsetsSize, meta->OffsetsWidth, &offsets, newMeta);

    if (meta->IsDense()) {
        ui32 expectedPerRow = meta->ExpectedPerRow;

        auto perRowDiff = offsets.data();
        ui32 rowCount = offsets.size();
        ui32 valueCount = GetOffsetNonZero(perRowDiff, expectedPerRow, rowCount);

        auto rowToValue = RowToValue_.Resize(valueCount + 1, GetRefCountedTypeCookie<TRowToValueTag>());

        ui32 rowIndex = 0;
        ui32 valueOffset = 0;
#define ITERATION { \
            ui32 nextOffset = GetOffsetNonZero(perRowDiff, expectedPerRow, rowIndex + 1); \
            if (nextOffset - valueOffset) { \
                *rowToValue++ = {rowOffset + rowIndex, valueOffset}; \
            }   \
            valueOffset = nextOffset; \
            ++rowIndex; \
        }

#ifdef UNROLL_LOOPS
        while (rowIndex + 4 < rowCount) {
            ITERATION
            ITERATION
            ITERATION
            ITERATION
        }
#endif
        while (rowIndex < rowCount) {
            ITERATION
        }

#undef ITERATION

        YT_VERIFY(meta->ChunkRowCount == rowOffset + rowCount);
        YT_VERIFY(valueOffset == valueCount);
        // Extra ValueIndex is used in ReadRows.
        *rowToValue = {SegmentRowLimit_, valueCount};

        IndexCount_ = rowToValue - RowToValue_.GetData();
    } else {
        auto rowIndexes = offsets.data();
        ui32 count = offsets.size();

        auto rowToValue = RowToValue_.Resize(count + 1, GetRefCountedTypeCookie<TRowToValueTag>());

        // Init with sentinel row index.
        auto rowIndex = SegmentRowLimit_;
        for (ui32 valueOffset = 0; valueOffset < count; ++valueOffset) {
            if (rowIndexes[valueOffset] != rowIndex) {
                rowIndex = rowIndexes[valueOffset];
                *rowToValue++ = {rowOffset + rowIndex, valueOffset};
            }
        }

        // Extra ValueIndex is used in ReadRows.
        *rowToValue = {SegmentRowLimit_, count};

        IndexCount_ = rowToValue - RowToValue_.GetData();
    }

    return ptr;
}
#endif

// Init only pro spans of row indexes.
const ui64* TScanMultiValueIndexExtractor::InitIndex(
    const TMultiValueIndexMeta* meta,
    const ui64* ptr,
    ui32 rowOffset,
    TMutableRange<TReadSpan> spans,
    TTmpBuffers* tmpBuffers,
    bool newMeta)
{
    ui32 segmentRowOffset = meta->ChunkRowCount - meta->RowCount;

    auto slice = GetBatchSlice(spans, meta->ChunkRowCount);
    ui32 batchSize = slice.GetBatchSize();

    // Segment can be initialized multiple times if block bound (of other columns) crosses segment.
    YT_VERIFY(!slice.GetSpans().empty());
    SegmentRowLimit_ = slice.GetSpans().end()[-1].Upper;

    tmpBuffers->DataSpans.resize(slice.GetSize());
    auto* offsetsSpans = tmpBuffers->DataSpans.data();

    if (meta->IsDense()) {
        // For dense unpack only span ranges.
        auto perRowDiffsView = CreateCompressedView(ptr, meta->OffsetsSize, meta->OffsetsWidth, newMeta);
        ptr += perRowDiffsView.GetSizeInWords();

        auto rowToValue = RowToValue_.Resize(batchSize + 1, GetRefCountedTypeCookie<TRowToValueTag>());

        ui32 expectedPerRow = meta->ExpectedPerRow;

        ui32 valueOffset = 0;

        // Upper part of rowToValue buffer is used for perRowDiffs.
        auto perRowDiffs = reinterpret_cast<ui32*>(rowToValue + batchSize + 1) - (batchSize + 1);

        for (auto [lower, upper] : slice.GetSpans()) {
            auto segmentLower = lower - segmentRowOffset;
            auto segmentUpper = upper - segmentRowOffset;

            auto startSegmentOffset = GetOffset(perRowDiffsView, expectedPerRow, segmentLower);
            perRowDiffsView.UnpackTo(perRowDiffs, segmentLower, segmentUpper);

            ui32 count = segmentUpper - segmentLower;

            ui32 startValueOffset = valueOffset;
            ui32 pivot = valueOffset + expectedPerRow * segmentLower - startSegmentOffset;

#ifndef NDEBUG
#define CHECK_EXPECTED_OFFSET \
                auto expectedNextOffset = GetOffset( \
                    perRowDiffsView, \
                    expectedPerRow, \
                    segmentLower + position + 1) - startSegmentOffset + startValueOffset; \
                YT_VERIFY(nextValueOffset == expectedNextOffset);
#else
#define CHECK_EXPECTED_OFFSET
#endif

#define ITERATION { \
                pivot += expectedPerRow; \
                auto nextValueOffset = pivot + ZigZagDecode32(perRowDiffs[position]); \
                CHECK_EXPECTED_OFFSET \
                YT_ASSERT(nextValueOffset >= valueOffset); \
                if (nextValueOffset > valueOffset) { \
                    YT_VERIFY(reinterpret_cast<ui32*>(rowToValue) < perRowDiffs + position); \
                    *rowToValue++ = {rowOffset + position, valueOffset}; \
                } \
                valueOffset = nextValueOffset; \
                ++position; \
            }

            ui32 position = 0;
#ifdef UNROLL_LOOPS
            while (position + 4 < count) {
                ITERATION
                ITERATION
                ITERATION
                ITERATION
            }
#endif

            while (position < count) {
                ITERATION
            }

#undef ITERATION
#undef CHECK_EXPECTED_OFFSET

            rowOffset += count;
            perRowDiffs += count;

            auto endSegmentOffset = startSegmentOffset + valueOffset - startValueOffset;
            auto endSegmentOffset0 = GetOffset(perRowDiffsView, expectedPerRow, segmentUpper);
            YT_VERIFY(endSegmentOffset == endSegmentOffset0);

            // TODO(lukyan): Skip empty data spans (if startSegmentOffset == endSegmentOffset)?
            *offsetsSpans++ = {startSegmentOffset, endSegmentOffset};

            YT_VERIFY(valueOffset - startValueOffset == endSegmentOffset - startSegmentOffset);
        }

        IndexCount_ = rowToValue - RowToValue_.GetData();

        // Extra ValueIndex is used in ReadRows.
        *rowToValue = {rowOffset, valueOffset};
    } else {
        auto rowIndexesView = CreateCompressedView(ptr, meta->OffsetsSize, meta->OffsetsWidth, newMeta);
        ptr += rowIndexesView.GetSizeInWords();

        auto rowToValue = RowToValue_.Resize(batchSize + 1, GetRefCountedTypeCookie<TRowToValueTag>());

        ui32 count = rowIndexesView.GetSize();

        // Value offset in segment.
        ui32 valueOffset = 0;
        ui32 valueCount = 0;

        for (auto [lower, upper] : slice.GetSpans()) {
            auto segmentLower = lower - segmentRowOffset;
            auto segmentUpper = upper - segmentRowOffset;

            valueOffset = ExponentialSearch(valueOffset, count, [&] (ui32 valueOffset) {
                return rowIndexesView[valueOffset] < segmentLower;
            });

            ui32 valueOffsetEnd = ExponentialSearch(valueOffset, count, [&] (ui32 valueOffset) {
                return rowIndexesView[valueOffset] < segmentUpper;
            });

            // Size of rowIndexesView may be much greater than batchSize if there are few rows but many values in row.
            // We unpack values to tmpBuffers->Offsets buffer.
            auto& rowIndexes = tmpBuffers->Offsets;
            rowIndexes.resize(valueOffsetEnd - valueOffset);
            rowIndexesView.UnpackTo(rowIndexes.data(), valueOffset, valueOffsetEnd);

            // Init with sentinel row index.
            ui32 rowIndex = -1;

#define ITERATION \
            if (rowIndexes[position] != rowIndex) { \
                rowIndex = rowIndexes[position]; \
                *rowToValue++ = {rowIndex - segmentLower + rowOffset, position + valueCount}; \
            } \
            ++position;

            ui32 position = 0;
            ui32 count = valueOffsetEnd - valueOffset;

#ifdef UNROLL_LOOPS
            while (position + 4 < count) {
                ITERATION
                ITERATION
                ITERATION
                ITERATION
            }
#endif
            while (position < count) {
                ITERATION
            }

#undef ITERATION

            rowOffset += segmentUpper - segmentLower;

            // TODO(lukyan): Skip empty data spans (if valueOffset == valueOffsetEnd)?
            *offsetsSpans++ = {valueOffset, valueOffsetEnd};
            valueCount += valueOffsetEnd - valueOffset;

            valueOffset = valueOffsetEnd;
        }

        IndexCount_ = rowToValue - RowToValue_.GetData();

        // Extra ValueIndex is used in ReadRows.
        *rowToValue = {rowOffset, valueCount};
    }

    return ptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
