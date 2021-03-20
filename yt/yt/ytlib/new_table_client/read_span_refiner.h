#pragma once

#include "public.h"
#include "segment_readers.h"

#include <yt/yt/ytlib/table_chunk_format/helpers.h>

#include <yt/yt/core/misc/tls_guard.h>

namespace NYT::NNewTableClient {

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

template <class TDerived>
class TRleIterator
    : protected TRleBase
{
public:
    void SetBlock(TSharedRef block, TSharedRange<NProto::TSegmentMeta> segmentsMeta)
    {
        if (Block_.Begin() == block.Begin() && Block_.End() == block.End()) {
            return;
        }
        Block_ = block;
        SegmentsMeta_ = segmentsMeta;
        SegmentIndex_ = 0;
        TRleBase::Reset();
    }

    Y_FORCE_INLINE void SkipTo(ui32 rowIndex)
    {
        if (Y_UNLIKELY(rowIndex >= SegmentRowLimit_)) {
            // Search segment
            auto segmentIt = ExponentialSearch(
                SegmentsMeta_.begin() + SegmentIndex_,
                SegmentsMeta_.end(),
                [=] (auto segmentMetaIt) {
                    return segmentMetaIt->chunk_row_count() <= rowIndex;
                });

            YT_VERIFY(segmentIt != SegmentsMeta_.end());
            SegmentIndex_ = std::distance(SegmentsMeta_.begin(), segmentIt);
            static_cast<TDerived*>(this)->UpdateSegment();
            Position_ = 0;
        }

        Position_ = ExponentialSearch(Position_, Count_, [=] (auto position) {
            return UpperRowBound(position) <= rowIndex;
        });

        YT_ASSERT(LowerRowBound(Position_) <= rowIndex);
        YT_ASSERT(Position_ < Count_);
    }

    // For pred(index) no need to know value, move helper to rle base.
    template <class TPredicate>
    Y_FORCE_INLINE ui32 SkipInSegment(ui32 upperRowBound, TPredicate pred, ui32 position)
    {
        return ExponentialSearch(position, Count_, [=] (auto position) {
            return UpperRowBound(position) < upperRowBound && pred(position);
        });
    }

    template <class TPredicate>
    Y_FORCE_INLINE ui32 SkipWhileImpl(ui32 upperRowBound, TPredicate pred)
    {
        if (upperRowBound <= UpperRowBound(Position_)) {
            // No need to call predicate.
            // TODO(lukyan): Increment position if equal?
            return upperRowBound;
        }
        // Predicate is true at current position so start search from next position.
        Position_ = SkipInSegment(upperRowBound, pred, Position_ + 1);

        if (Y_UNLIKELY(Position_ == Count_)) {
            // TODO(lukyan): Use lookup segment readers here.
            YT_ASSERT(UpperRowBound(Count_ - 1) < upperRowBound && pred(Count_ - 1));

            do {
                ++SegmentIndex_;
                YT_VERIFY(SegmentIndex_ != SegmentsMeta_.size());
                static_cast<TDerived*>(this)->UpdateSegment();
            } while (UpperRowBound(Count_ - 1) < upperRowBound && pred(Count_ - 1));

            Position_ = SkipInSegment(upperRowBound, pred, 0);
        }

        YT_VERIFY(Position_ != Count_);
        return pred(Position_) ? upperRowBound : LowerRowBound(Position_);
    }

protected:
    TSharedRef Block_;
    TSharedRange<NProto::TSegmentMeta> SegmentsMeta_;

    ui32 SegmentIndex_ = 0;
    ui32 Position_ = 0;
};

template <EValueType Type>
class TColumnIterator
    : public TRleIterator<TColumnIterator<Type>>
{
public:
    using TBase = TRleIterator<TColumnIterator<Type>>;

    using TBase::Position_;
    using TBase::LowerRowBound;

    TColumnIterator()
    {
        TBase::InitNull();
        Value_.InitNull();
    }

    void UpdateSegment()
    {
        const auto& segmentMeta = TBase::SegmentsMeta_[TBase::SegmentIndex_];
        DoReadSegment(&Value_, this, segmentMeta, TBase::Block_.Begin() + segmentMeta.offset(), &LocalBuffers_);
    }

    void Init(TReadSpan rowSpan)
    {
        YT_ASSERT(Span_.Upper <= rowSpan.Lower);
        Span_ = rowSpan;
        YT_ASSERT(rowSpan.Lower <= rowSpan.Upper);

        // Skip if not exhausted.
        if (rowSpan.Lower < rowSpan.Upper) {
            TBase::SkipTo(rowSpan.Lower);
        }
    }

    // This is the only place where Value used.
    TUnversionedValue GetValue()
    {
        return Value_[Position_];
    }

    TReadSpan SkipTillEnd() const
    {
        YT_ASSERT(LowerRowBound(Position_) <= Span_.Lower);

        return Span_;
    }

    template <class TPredicate>
    Y_FORCE_INLINE TReadSpan SkipWhile(TPredicate pred)
    {
        YT_ASSERT(LowerRowBound(Position_) <= Span_.Lower);

        auto lower = Span_.Lower;
        auto upper = TBase::SkipWhileImpl(Span_.Upper, [=] (ui32 index) {
            return pred(Value_[index]);
        });

        Span_.Lower = upper;
        return TReadSpan{lower, upper};
    }

    bool IsExhausted() const
    {
        YT_ASSERT(Span_.Lower <= Span_.Upper);
        return Span_.Lower == Span_.Upper;
    }

private:
    TValueExtractor<Type> Value_;
    TReadSpan Span_;
    TTmpBuffers LocalBuffers_;

};

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
struct TKeysSliceBase
{
    ui32 ColumnId = 0;
    ui32 Index = 0;
    ui32 Limit = 0;

    Y_FORCE_INLINE TUnversionedRow GetBound(ui32 index) const
    {
        return static_cast<const TDerived*>(this)->GetBound(index);
    }

    Y_FORCE_INLINE TUnversionedValue GetItem(ui32 index) const
    {
        auto bound = GetBound(index);
        YT_ASSERT(ColumnId < bound.GetCount());
        return bound[ColumnId];
    }

    void Init(TReadSpan span)
    {
        Index = span.Lower;
        Limit = span.Upper;
    }

    Y_FORCE_INLINE TUnversionedValue GetValue()
    {
        return GetItem(Index);
    }

    template <EValueType Type>
    Y_FORCE_INLINE TReadSpan SkipWhileEqual(TUnversionedValue value)
    {
        YT_ASSERT(Index != Limit);
        YT_ASSERT(GetItem(Index) == value);
        auto start = Index;
        Index = ExponentialSearch(Index + 1, Limit, [=] (auto index) {
            return CompareValues<Type>(GetItem(index), value) == 0;
        });

        return {start, Index};
    }

    // Returns exhausted flag.
    template <EValueType Type>
    Y_FORCE_INLINE bool SkipTo(TUnversionedValue value)
    {
        Index = ExponentialSearch(Index, Limit, [=] (auto index) {
            return CompareValues<Type>(GetItem(index), value) < 0;
        });

        return Index == Limit;
    }
};

template <class T>
struct TKeysSlice;

template <>
struct TKeysSlice<TLegacyKey>
    : public TKeysSliceBase<TKeysSlice<TLegacyKey>>
{
    using TBase = TKeysSliceBase<TKeysSlice<TLegacyKey>>;

    TRange<TLegacyKey> Keys;

    TUnversionedRow GetBound(ui32 index) const
    {
        return Keys[index];
    }

    std::optional<TReadSpan> Cover() const
    {
        return std::nullopt;
    }

    template <EValueType Type>
    std::optional<TReadSpan> SkipWhileEqual(TUnversionedValue value)
    {
        return TBase::template SkipWhileEqual<Type>(value);
    }
};

template <>
struct TKeysSlice<TRowRange>
    : public TKeysSliceBase<TKeysSlice<TRowRange>>
{
    using TBase = TKeysSliceBase<TKeysSlice<TRowRange>>;

    TRange<TRowRange> Ranges;
    bool LastColumn = false;

    TUnversionedRow GetBound(ui32 index) const
    {
        return index & 1 ? Ranges[index / 2].second : Ranges[index / 2].first;
    }

    bool Cover(ui32 index) const
    {
        return index & 1;
    }

    std::optional<TReadSpan> Cover() const
    {
        if (Cover(Index)) {
            return TReadSpan{Index, Index};
        } else {
            return std::nullopt;
        }
    }

    template <EValueType Type>
    std::optional<TReadSpan> SkipWhileEqual(TUnversionedValue value)
    {
        // Fixes span according to sentinel values.
        auto result = ShrinkRange(TBase::template SkipWhileEqual<Type>(value));

        if (!IsEmpty(result) || Cover(result.Lower)) {
            return result;
        } else {
            return std::nullopt;
        }
    }

private:
    EValueType NextColumnItemType(ui32 index)
    {
        auto bound = GetBound(index);
        YT_VERIFY(ColumnId < bound.GetCount());

        return ColumnId + 1 != bound.GetCount()
            ? bound[ColumnId + 1].Type
            : EValueType::Min;
    }

    TReadSpan ShrinkRange(TReadSpan span)
    {
        auto [begin, end] = span;

        // For each key column in table schema except last
        // the next column value is either null or non-sentinel value. It is greater than Min and less than Max.
        // For the last key column in table schema there is 'no value'. And it is less than both Min and Max.
        // [x] is less than [x, Min]
        // [x, null or value] is greater than [x, Min]

        // Supports cases when key ranges are:
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

};

using TRangeSliceAdapter =  TKeysSlice<TRowRange>;

struct TSpanMatching
{
    TReadSpan Chunk;
    TReadSpan Control;

    TSpanMatching(TReadSpan chunk, TReadSpan control)
        : Chunk(chunk)
        , Control(control)
    { }
};

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
            if (auto controlSpan = keys->Cover()) {
                nextMatchings->emplace_back(chunk->SkipTillEnd(), *controlSpan);
            }

            break;
        }

        auto controlValue = keys->GetValue();

        if (CompareValues<Type>(currentValue, controlValue) == 0) {
            // Need to skip anyway to update currentValue.
            auto chunkSpan = chunk->SkipWhile([=] (TUnversionedValue value) {
                return CompareValues<Type>(value, controlValue) == 0;
            });

            if (auto controlSpan = keys->template SkipWhileEqual<Type>(controlValue)) {
                nextMatchings->emplace_back(chunkSpan, *controlSpan);
            }
        } else {
            // Need to skip anyway to update currentValue.
            auto chunkSpan = chunk->SkipWhile([=] (TUnversionedValue value) {
                return CompareValues<Type>(value, controlValue) < 0;
            });

            if (auto controlSpan = keys->Cover()) {
                nextMatchings->emplace_back(chunkSpan, *controlSpan);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
