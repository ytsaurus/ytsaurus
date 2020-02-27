#pragma once
#ifndef PIECEWISE_LINEAR_FUNCTION_HELPERS_H_
#error "Direct inclusion of this file is not allowed, include piecewise_linear_function_helpers.h"
// For the sake of sane code completion.
#include "piecewise_linear_function_helpers.h"
#endif

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TPiecewiseFunction>
void VerifyNondecreasing(const TPiecewiseFunction& vecFunc, const NLogging::TLogger& logger)
{
    const auto& Logger = logger;
    using TValue = typename TPiecewiseFunction::TValueType;

    auto dominates = [&] (const TValue& lhs, const TValue& rhs) -> bool {
        if constexpr (std::is_same_v<TValue, double>) {
            return lhs >= rhs;
        } else {
            return Dominates(lhs, rhs);
        }
    };

    for (const auto& segment : vecFunc.Segments()) {
        YT_LOG_FATAL_IF(!dominates(segment.RightValue(), segment.LeftValue()),
            "The vector function is decreasing at segment {%.16v, %.16v}. Values at bounds: {%.16v, %.16v}",
            segment.LeftBound(),
            segment.RightBound(),
            segment.LeftValue(),
            segment.RightValue());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TSegment>
TSegment ConnectSegments(const TSegment& firstSegment, const TSegment& secondSegment) {
    return TSegment(
        {firstSegment.LeftBound(), firstSegment.LeftValue()},
        {secondSegment.RightBound(), secondSegment.RightValue()}
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
