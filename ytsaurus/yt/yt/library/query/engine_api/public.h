#pragma once

#include <yt/yt/library/query/base/public.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IFunctionCodegen)
DECLARE_REFCOUNTED_STRUCT(IAggregateCodegen)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFunctionProfilerMap)
using TConstFunctionProfilerMapPtr = TIntrusivePtr<const TFunctionProfilerMap>;

DECLARE_REFCOUNTED_STRUCT(TAggregateProfilerMap)
using TConstAggregateProfilerMapPtr = TIntrusivePtr<const TAggregateProfilerMap>;

DECLARE_REFCOUNTED_STRUCT(TRangeExtractorMap)
using TConstRangeExtractorMapPtr = TIntrusivePtr<const TRangeExtractorMap>;

////////////////////////////////////////////////////////////////////////////////

const TConstFunctionProfilerMapPtr GetBuiltinFunctionProfilers();
const TConstAggregateProfilerMapPtr GetBuiltinAggregateProfilers();
const TConstRangeExtractorMapPtr GetBuiltinRangeExtractors();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
