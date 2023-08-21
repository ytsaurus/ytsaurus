#ifndef READ_SPAN_REFINER_INL_H_
#error "Direct inclusion of this file is not allowed, include read_span_refiner.h"
// For the sake of sane code completion.
#include "read_span_refiner.h"
#endif
#undef READ_SPAN_REFINER_INL_H_

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

using ESortOrder = NTableClient::ESortOrder;

template <EValueType valueType>
int CompareValues(
    const TUnversionedValue& lhs,
    const TUnversionedValue& rhs,
    NTableClient::ESortOrder sortOrder = ESortOrder::Ascending)
{
    return NTableChunkFormat::CompareValues<valueType>(lhs, rhs, sortOrder);
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType Type>
TKeySegmentReader<Type, true>::TKeySegmentReader()
{
    TScanKeyIndexExtractor::InitNullIndex();
    TScanDataExtractor<Type>::InitNullData();
}

template <EValueType Type>
TKeySegmentReader<Type, false>::TKeySegmentReader()
{
    TLookupKeyIndexExtractor::InitNullIndex();
    TLookupDataExtractor<Type>::InitNullData();
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType Type>
void TColumnIterator<Type>::SetBlock(TRef block, TRange<TKeyMeta<Type>> segmentsMeta)
{
    if (Block_.Begin() == block.Begin() && Block_.End() == block.End()) {
        return;
    }
    Block_ = block;
    SegmentsMeta_ = segmentsMeta;
    SegmentIndex_ = 0;
    TBase::Reset();
}

template <EValueType Type>
void TColumnIterator<Type>::SetReadSpan(TReadSpan rowSpan)
{
    YT_ASSERT(Span_.Upper <= rowSpan.Lower);
    Span_ = rowSpan;
    YT_ASSERT(rowSpan.Lower <= rowSpan.Upper);

    // Skip if not exhausted.
    if (rowSpan.Lower < rowSpan.Upper) {
        SkipTo(rowSpan.Lower);
    }
}

template <EValueType Type>
TUnversionedValue TColumnIterator<Type>::GetValue()
{
    return GetValue(Position_);
}

template <EValueType Type>
TReadSpan TColumnIterator<Type>::SkipTillEnd() const
{
    YT_ASSERT(LowerRowBound(Position_) <= Span_.Lower);

    return Span_;
}

template <EValueType Type>
template <class TPredicate>
TReadSpan TColumnIterator<Type>::SkipWhile(TPredicate pred)
{
    YT_ASSERT(LowerRowBound(Position_) <= Span_.Lower);

    auto lower = Span_.Lower;
    auto upper = SkipWhileImpl(Span_.Upper, [&] (ui32 position) {
        return pred(GetValue(position));
    });

    Span_.Lower = upper;
    return TReadSpan{lower, upper};
}

template <EValueType Type>
bool TColumnIterator<Type>::IsExhausted() const
{
    YT_ASSERT(Span_.Lower <= Span_.Upper);
    return Span_.Lower == Span_.Upper;
}

template <EValueType Type>
TUnversionedValue TColumnIterator<Type>::GetValue(ui32 position) const
{
    YT_ASSERT(position < GetCount());
    TUnversionedValue result;
    TBase::Extract(&result, position);
    return result;
}

template <EValueType Type>
void TColumnIterator<Type>::UpdateSegment()
{
    const auto& segmentMeta = SegmentsMeta_[SegmentIndex_];

    DoInitLookupKeySegment</*NewMeta*/ false>(
        this,
        &segmentMeta,
        reinterpret_cast<const ui64*>(Block_.Begin() + segmentMeta.DataOffset));
}

template <EValueType Type>
void TColumnIterator<Type>::SkipToSegment(ui32 rowIndex)
{
    // Search segment
    auto segmentIt = ExponentialSearch(
        SegmentsMeta_.begin() + SegmentIndex_,
        SegmentsMeta_.end(),
        [&] (auto segmentMetaIt) {
            return segmentMetaIt->ChunkRowCount <= rowIndex;
        });

    YT_VERIFY(segmentIt != SegmentsMeta_.end());
    SegmentIndex_ = std::distance(SegmentsMeta_.begin(), segmentIt);
    UpdateSegment();
    Position_ = 0;
}

template <EValueType Type>
void TColumnIterator<Type>::SkipTo(ui32 rowIndex)
{
    if (Y_UNLIKELY(rowIndex >= GetSegmentRowLimit())) {
        SkipToSegment(rowIndex);
    }

    Position_ = ExponentialSearch(Position_, GetCount(), [&] (auto position) {
        return UpperRowBound(position) <= rowIndex;
    });

    YT_ASSERT(LowerRowBound(Position_) <= rowIndex);
    YT_ASSERT(Position_ < GetCount());
}

template <EValueType Type>
template <class TPredicate>
ui32 TColumnIterator<Type>::SkipInSegment(ui32 upperRowBound, TPredicate pred, ui32 position)
{
    return ExponentialSearch(position, GetCount(), [&] (auto position) {
        return UpperRowBound(position) < upperRowBound && pred(position);
    });
}

template <EValueType Type>
template <class TPredicate>
ui32 TColumnIterator<Type>::SkipWhileImpl(ui32 upperRowBound, TPredicate pred)
{
    if (upperRowBound <= UpperRowBound(Position_)) {
        // No need to call predicate.
        // TODO(lukyan): Increment position if equal?
        return upperRowBound;
    }

    // Predicate is true at current position so start search from next position.
    Position_ = SkipInSegment(upperRowBound, pred, Position_ + 1);

    // TODO(lukyan): Move this branch into noinline function?
    if (Y_UNLIKELY(Position_ == GetCount())) {
        // TODO(lukyan): Use lookup segment readers here.
        YT_ASSERT(UpperRowBound(GetCount() - 1) < upperRowBound && pred(GetCount() - 1));

        do {
            ++SegmentIndex_;
            YT_VERIFY(SegmentIndex_ != SegmentsMeta_.size());
            UpdateSegment();
        } while (UpperRowBound(GetCount() - 1) < upperRowBound && pred(GetCount() - 1));

        Position_ = SkipInSegment(upperRowBound, pred, 0);
    }

    YT_VERIFY(Position_ != GetCount());
    return pred(Position_) ? upperRowBound : LowerRowBound(Position_);
}

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
TUnversionedRow TBoundsIteratorBase<TDerived>::GetBound(ui32 boundIndex) const
{
    return static_cast<const TDerived*>(this)->GetBound(boundIndex);
}

template <class TDerived>
TUnversionedValue TBoundsIteratorBase<TDerived>::GetItem(ui32 boundIndex) const
{
    auto bound = GetBound(boundIndex);
    YT_ASSERT(ColumnId < bound.GetCount());
    return bound[ColumnId];
}

template <class TDerived>
void TBoundsIteratorBase<TDerived>::SetReadSpan(TReadSpan span)
{
    BoundIndex = span.Lower;
    Limit = span.Upper;
}

template <class TDerived>
TUnversionedValue TBoundsIteratorBase<TDerived>::GetValue()
{
    return GetItem(BoundIndex);
}

template <class TDerived>
template <EValueType Type>
TReadSpan TBoundsIteratorBase<TDerived>::SkipWhileEqual(TUnversionedValue value)
{
    YT_ASSERT(BoundIndex != Limit);
    YT_ASSERT(GetItem(BoundIndex) == value);
    auto start = BoundIndex;
    BoundIndex = ExponentialSearch(BoundIndex + 1, Limit, [&] (auto index) {
        return CompareValues<Type>(GetItem(index), value) == 0;
    });

    return {start, BoundIndex};
}

template <class TDerived>
template <EValueType Type>
bool TBoundsIteratorBase<TDerived>::SkipTo(TUnversionedValue value)
{
    BoundIndex = ExponentialSearch(BoundIndex, Limit, [&] (auto index) {
        return CompareValues<Type>(GetItem(index), value) < 0;
    });

    return BoundIndex == Limit;
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedRow TBoundsIterator<TLegacyKey>::GetBound(ui32 boundIndex) const
{
    return Keys[boundIndex];
}

std::optional<TReadSpan> TBoundsIterator<TLegacyKey>::CoversIntervalBeforeBound() const
{
    return std::nullopt;
}

template <EValueType Type>
std::optional<TReadSpan> TBoundsIterator<TLegacyKey>::SkipWhileEqual(TUnversionedValue value)
{
    return TBase::template SkipWhileEqual<Type>(value);
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedRow TBoundsIterator<TRowRange>::GetBound(ui32 boundIndex) const
{
    return boundIndex & 1 ? Ranges[boundIndex / 2].second : Ranges[boundIndex / 2].first;
}

std::optional<TReadSpan> TBoundsIterator<TRowRange>::CoversIntervalBeforeBound() const
{
    if (CoversIntervalBeforeBound(BoundIndex)) {
        return TReadSpan{BoundIndex, BoundIndex};
    } else {
        return std::nullopt;
    }
}

template <EValueType Type>
std::optional<TReadSpan> TBoundsIterator<TRowRange>::SkipWhileEqual(TUnversionedValue value)
{
    // Fixes span according to sentinel values.
    auto result = ShrinkRange(TBase::template SkipWhileEqual<Type>(value));

    if (!IsEmpty(result) || CoversIntervalBeforeBound(result.Lower)) {
        return result;
    } else {
        return std::nullopt;
    }
}

bool TBoundsIterator<TRowRange>::CoversIntervalBeforeBound(ui32 boundIndex) const
{
    return boundIndex & 1;
}

EValueType TBoundsIterator<TRowRange>::NextColumnItemType(ui32 boundIndex)
{
    auto bound = GetBound(boundIndex);
    YT_VERIFY(ColumnId < bound.GetCount());

    return ColumnId + 1 != bound.GetCount()
        ? bound[ColumnId + 1].Type
        : EValueType::Min;
}

TReadSpan TBoundsIterator<TRowRange>::ShrinkRange(TReadSpan span)
{
    auto [begin, end] = span;

    // For each key column in table schema except last
    // the next column value is either null or non-sentinel value. It is greater than Min and less than Max.
    // For the last key column in table schema there is 'no value'. And it is less than both Min and Max.
    // [x] is less than [x, Min]
    // [x, null or value] is greater than [x, Min]

    // Support cases when key ranges are:
    // [x]..[x] -> empty
    // [x, Min]..[x, Min] -> empty
    // [x, Max]..[x, Max] -> empty
    // [x]..[x, Max] -> not empty

    // Supports also series of ranges. For example: [[x]..[x, Min]], [[x, Min]..[x, Max]]

    // LastColumn = ColumnId + 1 == ColumnRefiners_.size();
    if (LastColumn) {
        // [x]..[x, Min] -> not empty
        // [x, Min]..[x, Max] -> empty

        while (begin != end && ColumnId + 1 == GetBound(begin).GetCount()) {
            ++begin;
        }
        while (end != begin && ColumnId + 1 < GetBound(end - 1).GetCount()) {
            --end;
        }
    } else {
        // [x]..[x, Min] -> empty
        // [x, Min]..[x, Max] -> not empty

        while (begin != end && NextColumnItemType(begin) == EValueType::Min) {
            ++begin;
        }
        while (end != begin && NextColumnItemType(end - 1) == EValueType::Max) {
            --end;
        }
    }

    return {begin, end};
}

using TRangeSliceAdapter =  TBoundsIterator<TRowRange>;

// TODO(lukyan): push_back_inserter iterator or other consume callback?

template <EValueType Type, class TSliceAdapter>
void BuildReadRowRanges(
    TColumnIterator<Type>* chunk,
    TSliceAdapter* keys,
    std::vector<TSpanMatching>* nextMatchings)
{
    // 1. Exhausted -> SkipTillEnd according to cover and set next span to [index, index].
    // 2. Equal -> SkipWhileEqual and set next span to [startIndex, nextIndex].
    // 3. Less -> SkipWhileLess according to cover and set next span to [index, index].

    while (!chunk->IsExhausted()) {
        auto currentValue = chunk->GetValue();

        if (keys->template SkipTo<Type>(currentValue)) {
            // Check for open upper bound.
            if (auto controlSpan = keys->CoversIntervalBeforeBound()) {
                nextMatchings->emplace_back(chunk->SkipTillEnd(), *controlSpan);
            }

            break;
        }

        auto controlValue = keys->GetValue();

        if (CompareValues<Type>(currentValue, controlValue) == 0) {
            // Need to skip anyway to update currentValue.
            auto chunkSpan = chunk->SkipWhile([&] (TUnversionedValue value) {
                return CompareValues<Type>(value, controlValue) == 0;
            });

            if (auto controlSpan = keys->template SkipWhileEqual<Type>(controlValue)) {
                nextMatchings->emplace_back(chunkSpan, *controlSpan);
            }
        } else {
            // Need to skip anyway to update currentValue.
            auto chunkSpan = chunk->SkipWhile([&] (TUnversionedValue value) {
                return CompareValues<Type>(value, controlValue) < 0;
            });

            if (auto controlSpan = keys->CoversIntervalBeforeBound()) {
                nextMatchings->emplace_back(chunkSpan, *controlSpan);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
