#pragma once

#include "public.h"

#include <yt/yt/library/query/engine_api/public.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TValueBound
{
    TValue Value;
    // Bounds are located between values.
    // Flag denotes when the bound is before value or after it.
    // For upper bound Flag = Included.
    // For lower bound Flag = !Included.
    bool Flag;
};

extern TValueBound MinBound;
extern TValueBound MaxBound;

constexpr ui32 SentinelColumnId = std::numeric_limits<ui32>::max();

bool operator < (const TValueBound& lhs, const TValueBound& rhs);

bool operator <= (const TValueBound& lhs, const TValueBound& rhs);

bool operator == (const TValueBound& lhs, const TValueBound& rhs);

bool TestValue(TValue value, const TValueBound& lower, const TValueBound& upper);

struct TConstraintRef
{
    // Universal constraint if ColumnId is sentinel.
    ui32 ColumnId = SentinelColumnId;
    ui32 StartIndex = 0;
    ui32 EndIndex = 0;

    static TConstraintRef Empty()
    {
        TConstraintRef result;
        result.ColumnId = 0;
        return result;
    }

    static TConstraintRef Universal()
    {
        return {};
    }
};

struct TConstraint
{
    // For exact match (key = ...) both values are equal.
    TValue LowerValue;
    TValue UpperValue;

    TConstraintRef Next;

    // TValueBound is not used to get more tight layout.
    bool LowerIncluded;
    bool UpperIncluded;

    TValueBound GetLowerBound() const
    {
        return {LowerValue, !LowerIncluded};
    }

    TValueBound GetUpperBound() const
    {
        return {UpperValue, UpperIncluded};
    }

    static TConstraint Make(TValueBound lowerBound, TValueBound upperBound, TConstraintRef next = TConstraintRef::Universal())
    {
        YT_VERIFY(lowerBound < upperBound);
        return {
            lowerBound.Value,
            upperBound.Value,
            next,
            !lowerBound.Flag,
            upperBound.Flag};
    }
};

struct TColumnConstraint
{
    TValueBound Lower;
    TValueBound Upper;

    TValue GetValue() const
    {
        YT_ASSERT(IsExact());
        return Lower.Value;
    }

    bool IsExact() const
    {
        return Lower.Value == Upper.Value && !Lower.Flag && Upper.Flag;
    }

    bool IsRange() const
    {
        return Lower.Value < Upper.Value;
    }

    bool IsUniversal() const
    {
        return Lower.Value.Type == EValueType::Min && Upper.Value.Type == EValueType::Max;
    }
};

struct TColumnConstraints
    : public std::vector<TConstraint>
{ };

extern TColumnConstraint UniversalInterval;

struct TConstraintsHolder
    : public std::vector<TColumnConstraints>
{
    explicit TConstraintsHolder(ui32 columnCount)
        : std::vector<TColumnConstraints>(columnCount)
    { }

    TConstraintRef Append(std::initializer_list<TConstraint> constraints, ui32 keyPartIndex);

    TConstraintRef Interval(TValueBound lower, TValueBound upper, ui32 keyPartIndex);

    TConstraintRef Intersect(TConstraintRef lhs, TConstraintRef rhs);

    TConstraintRef Unite(TConstraintRef lhs, TConstraintRef rhs);

    TConstraintRef ExtractFromExpression(
        const TConstExpressionPtr& expr,
        const TKeyColumns& keyColumns,
        const TRowBufferPtr& rowBuffer,
        const TConstConstraintExtractorMapPtr& constraintExtractors = GetBuiltinConstraintExtractors());
};

TString ToString(const TConstraintsHolder& constraints, TConstraintRef root);

class TReadRangesGenerator
{
public:
    explicit TReadRangesGenerator(const TConstraintsHolder& constraints)
        : Constraints_(constraints)
        , Row_(Constraints_.size(), UniversalInterval)
    { }

    template <class TOnRange>
    void GenerateReadRanges(TConstraintRef constraintRef, const TOnRange& onRange, ui64 rangeExpansionLimit = std::numeric_limits<ui64>::max())
    {
        auto columnId = constraintRef.ColumnId;
        if (columnId == SentinelColumnId) {
            // Leaf node.
            onRange(Row_, rangeExpansionLimit);
            return;
        }

        auto intervals = MakeRange(Constraints_[columnId])
            .Slice(constraintRef.StartIndex, constraintRef.EndIndex);

        if (rangeExpansionLimit < intervals.Size()) {
            Row_[columnId] = TColumnConstraint{intervals.Front().GetLowerBound(), intervals.Back().GetUpperBound()};

            onRange(Row_, rangeExpansionLimit);
        } else if (!intervals.Empty()) {
            ui64 nextRangeExpansionLimit = rangeExpansionLimit / intervals.Size();
            YT_VERIFY(nextRangeExpansionLimit > 0);
            for (const auto& item : intervals) {
                Row_[columnId] = TColumnConstraint{item.GetLowerBound(), item.GetUpperBound()};

                Row_[columnId].Lower.Value.Id = columnId;
                Row_[columnId].Upper.Value.Id = columnId;

                GenerateReadRanges(item.Next, onRange, nextRangeExpansionLimit);
            }
        }

        Row_[columnId] = UniversalInterval;
    }

private:
    const TConstraintsHolder& Constraints_;
    std::vector<TColumnConstraint> Row_;
};

TMutableRow MakeLowerBound(TRowBuffer* buffer, TRange<TValue> boundPrefix, TValueBound lastBound);

TMutableRow MakeUpperBound(TRowBuffer* buffer, TRange<TValue> boundPrefix, TValueBound lastBound);

TRowRange RowRangeFromPrefix(TRowBuffer* buffer, TRange<TValue> boundPrefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
