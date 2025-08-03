#pragma once

#include "public.h"
#include "vector_over_memory_chunk_provider.h"

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

    static TConstraintRef Empty();
    bool IsEmpty() const;

    static TConstraintRef Universal();
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

    TValueBound GetLowerBound() const;

    TValueBound GetUpperBound() const;

    static TConstraint Make(TValueBound lowerBound, TValueBound upperBound, TConstraintRef next = TConstraintRef::Universal());
};

struct TColumnConstraint
{
    TValueBound Lower;
    TValueBound Upper;

    TValue GetValue() const;

    bool IsExact() const;

    bool IsRange() const;

    bool IsUniversal() const;
};

struct TColumnConstraints
    : public TVectorOverMemoryChunkProvider<TConstraint>
{
    TColumnConstraints(
        TRefCountedTypeCookie cookie,
        IMemoryChunkProviderPtr memoryChunkProvider);
};

extern TColumnConstraint UniversalInterval;

struct TConstraintsHolder
    : public std::vector<TColumnConstraints>
{
    TConstraintsHolder(
        ui32 columnCount,
        TRefCountedTypeCookie cookie,
        IMemoryChunkProviderPtr memoryChunkProvider);

    TConstraintRef Append(std::initializer_list<TConstraint> constraints, ui32 keyPartIndex);

    TConstraintRef Constant(TValue value, ui32 keyPartIndex);

    TConstraintRef Interval(TValueBound lower, TValueBound upper, ui32 keyPartIndex);

    TConstraintRef Intersect(TConstraintRef lhs, TConstraintRef rhs);

    TConstraintRef Unite(TConstraintRef lhs, TConstraintRef rhs);

    TConstraintRef Invert(TConstraintRef constraints);

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
    explicit TReadRangesGenerator(const TConstraintsHolder& constraints);

    template <class TOnRange>
    void GenerateReadRanges(TConstraintRef constraintRef, const TOnRange& onRange, ui32 keyWidthLimit = std::numeric_limits<ui32>::max());

    ui64 EstimateExpansion(TConstraintRef constraintRef, ui32 keyWidthLimit, ui64 rangeExpansionLimit = std::numeric_limits<ui64>::max());
    std::pair<ui32, ui64> GetExpansionDepthAndEstimation(TConstraintRef constraintRef, ui64 rangeExpansionLimit = std::numeric_limits<ui64>::max());

private:
    const TConstraintsHolder& Constraints_;
    std::vector<TColumnConstraint> Row_;
};

TMutableRow MakeLowerBound(TRowBuffer* rowBuffer, TRange<TValue> boundPrefix, TValueBound lastBound);

TMutableRow MakeUpperBound(TRowBuffer* rowBuffer, TRange<TValue> boundPrefix, TValueBound lastBound);

TRowRange RowRangeFromPrefix(TRowBuffer* rowBuffer, TRange<TValue> boundPrefix);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define CONSTRAINTS_INL_H_
#include "constraints-inl.h"
#undef CONSTRAINTS_INL_H_
