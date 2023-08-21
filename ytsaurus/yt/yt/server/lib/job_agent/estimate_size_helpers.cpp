#include "estimate_size_helpers.h"

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/yson/string.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

size_t EstimateSize(const TString& value)
{
    return EstimatedValueSize + value.size();
}

size_t EstimateSize(const NYson::TYsonString& value)
{
    return value ? EstimatedValueSize + value.AsStringBuf().size() : 0;
}

size_t EstimateSize(i64 /*value*/)
{
    return EstimatedValueSize;
}

size_t EstimateSize(TGuid value)
{
    return value.IsEmpty() ? 0 : EstimatedValueSize * 2;
}

size_t EstimateSize(TInstant /*value*/)
{
    return EstimatedValueSize;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
