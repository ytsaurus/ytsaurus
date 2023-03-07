#pragma once

#include "private.h"

#include "resource_vector.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TScalarPiecewiseLinearFunction ExtractComponent(int resourceIndex, const TVectorPiecewiseLinearFunction& vecFunc);

TScalarPiecewiseSegment ExtractComponent(int resourceIndex, const TVectorPiecewiseSegment& vecSegment);

//! Transposed representation of a vector-valued segment, where its individual components are stored as separate scalar-valued vectors.
using TUnpackedVectorPiecewiseSegment = std::vector<TScalarPiecewiseSegment>;
TUnpackedVectorPiecewiseSegment UnpackVectorSegment(const TVectorPiecewiseSegment& vecSegment);

////////////////////////////////////////////////////////////////////////////////

template <class TPiecewiseFunction>
void VerifyNondecreasing(const TPiecewiseFunction& vecFunc, const NLogging::TLogger& logger = SchedulerLogger);

////////////////////////////////////////////////////////////////////////////////

//! Given two vectors U and V, their orientation is the sign of their 2D cross product.
//! Orientation of two segments is defined as the orientation of their corresponding vectors.
//! Currently we only have to deal with monotonic functions and all segments are pointing to the upper-right,
//! so if the orientation of vectors U and V is positive, then it means that V is "above" U (the same for negative/below).
//!
//! Returns the orientation of two segments.
int CompareSegments(const TScalarPiecewiseSegment& firstSegment, const TScalarPiecewiseSegment& secondSegment);

//! Returns the segment that connects the start of |firstSegment| with the end of |secondSegment|.
template <class TSegment>
TSegment ConnectSegments(const TSegment& firstSegment, const TSegment& secondSegment);

//! Given a scalar segment, if we shift its right value by +|epsilon| and -|epsilon|,
//! then the resulting segments are called the top and bottom bounds for this segment.
//! This struct holds bounds for individual components of a vector-valued segment.
struct TUnpackedVectorPiecewiseSegmentBounds
{
    TUnpackedVectorPiecewiseSegment Top;
    TUnpackedVectorPiecewiseSegment Bottom;
};
TUnpackedVectorPiecewiseSegmentBounds GetBounds(const TUnpackedVectorPiecewiseSegment& segments, double epsilon);

//! Transforms the function so that:
//! (1) the resulting function differs from the original by less than |epsilon| pointwise, and
//! (2) it has as few segments as possible.
//!
//! Unfortunately, we couldn't think of an efficient algorithm that solves this problem exactly,
//! so here we implemented a greedy algorithm, that gives a good approximation (we think).
//! Details: https://wiki.yandex-team.ru/yt/internal/hdrfv-function-compression/.
TVectorPiecewiseLinearFunction CompressFunction(
    const TVectorPiecewiseLinearFunction& vecFunc,
    double epsilon = RatioComputationPrecision);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

#define PIECEWISE_LINEAR_FUNCTION_HELPERS_H_
#include "piecewise_linear_function_helpers-inl.h"
#undef PIECEWISE_LINEAR_FUNCTION_HELPERS_H_
