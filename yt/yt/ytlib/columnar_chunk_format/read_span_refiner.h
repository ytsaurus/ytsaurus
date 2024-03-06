#pragma once

#include "public.h"
#include "segment_readers.h"

#include <yt/yt/ytlib/table_chunk_format/helpers.h>

namespace NYT::NColumnarChunkFormat {

////////////////////////////////////////////////////////////////////////////////

template <EValueType Type, bool Unpack>
class TKeySegmentReader;

template <EValueType Type>
class TKeySegmentReader<Type, true>
    : public TScanKeyIndexExtractor
    , public TScanDataExtractor<Type>
{
public:
    TKeySegmentReader();
};

template <EValueType Type>
class TKeySegmentReader<Type, false>
    : public TLookupKeyIndexExtractor
    , public TLookupDataExtractor<Type>
{
public:
    TKeySegmentReader();
};

template <EValueType Type>
class TColumnIterator
    : public TKeySegmentReader<Type, false>
{
public:
    using TBase = TKeySegmentReader<Type, false>;

    using TBase::LowerRowBound;
    using TBase::UpperRowBound;
    using TBase::GetSegmentRowLimit;
    using TBase::GetCount;

    void SetBlock(TRef block, TRange<TKeyMeta<Type>> segmentsMeta);

    void SetReadSpan(TReadSpan rowSpan);

    Y_FORCE_INLINE TUnversionedValue GetValue();

    Y_FORCE_INLINE TReadSpan SkipTillEnd() const;

    template <class TPredicate>
    Y_FORCE_INLINE TReadSpan SkipWhile(TPredicate pred);

    bool IsExhausted() const;

private:
    TRef Block_;
    TRange<TKeyMeta<Type>> SegmentsMeta_;

    ui32 SegmentIndex_ = 0;
    ui32 Position_ = 0;

    TReadSpan Span_;
    TTmpBuffers LocalBuffers_;

    Y_FORCE_INLINE TUnversionedValue GetValue(ui32 position) const;

    void UpdateSegment();

    Y_NO_INLINE void SkipToSegment(ui32 rowIndex);

    Y_FORCE_INLINE void SkipTo(ui32 rowIndex);

    // For pred(index) no need to know value, helper can be moved to rle base.
    template <class TPredicate>
    Y_FORCE_INLINE ui32 SkipInSegment(ui32 upperRowBound, TPredicate pred, ui32 position);

    template <class TPredicate>
    Y_FORCE_INLINE ui32 SkipWhileImpl(ui32 upperRowBound, TPredicate pred);
};

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
struct TBoundsIteratorBase
{
    ui32 ColumnId = 0;
    ui32 BoundIndex = 0;
    ui32 Limit = 0;

    Y_FORCE_INLINE TUnversionedRow GetBound(ui32 boundIndex) const;

    Y_FORCE_INLINE TUnversionedValue GetItem(ui32 boundIndex) const;

    void SetReadSpan(TReadSpan span);

    Y_FORCE_INLINE TUnversionedValue GetValue();

    template <EValueType Type>
    Y_FORCE_INLINE TReadSpan SkipWhileEqual(TUnversionedValue value);

    // Returns exhausted flag.
    template <EValueType Type>
    Y_FORCE_INLINE bool SkipTo(TUnversionedValue value);
};

template <class T>
class TBoundsIterator;

template <>
class TBoundsIterator<TLegacyKey>
    : public TBoundsIteratorBase<TBoundsIterator<TLegacyKey>>
{
public:
    using TBase = TBoundsIteratorBase<TBoundsIterator<TLegacyKey>>;

    TRange<TLegacyKey> Keys;

    Y_FORCE_INLINE TUnversionedRow GetBound(ui32 boundIndex) const;

    Y_FORCE_INLINE std::optional<TReadSpan> CoversIntervalBeforeBound() const;

    template <EValueType Type>
    Y_FORCE_INLINE std::optional<TReadSpan> SkipWhileEqual(TUnversionedValue value);
};

template <>
class TBoundsIterator<TRowRange>
    : public TBoundsIteratorBase<TBoundsIterator<TRowRange>>
{
public:
    using TBase = TBoundsIteratorBase<TBoundsIterator<TRowRange>>;

    TRange<TRowRange> Ranges;
    bool LastColumn = false;

    Y_FORCE_INLINE TUnversionedRow GetBound(ui32 boundIndex) const;

    Y_FORCE_INLINE std::optional<TReadSpan> CoversIntervalBeforeBound() const;

    template <EValueType Type>
    Y_FORCE_INLINE std::optional<TReadSpan> SkipWhileEqual(TUnversionedValue value);

private:
    Y_FORCE_INLINE bool CoversIntervalBeforeBound(ui32 boundIndex) const;

    Y_FORCE_INLINE EValueType NextColumnItemType(ui32 boundIndex);

    Y_FORCE_INLINE TReadSpan ShrinkRange(TReadSpan span);
};

using TRangeSliceAdapter =  TBoundsIterator<TRowRange>;

template <EValueType Type, class TSliceAdapter>
void BuildReadRowRanges(
    TColumnIterator<Type>* chunk,
    TSliceAdapter* keys,
    std::vector<TSpanMatching>* nextMatchings);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnarChunkFormat

#define READ_SPAN_REFINER_INL_H_
#include "read_span_refiner-inl.h"
#undef READ_SPAN_REFINER_INL_H_
