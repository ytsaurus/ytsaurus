#include "estimate_size_helpers.h"

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/yson/string.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t EstimatedValueSize = 16;

size_t EstimateSize(const TString& s)
{
    return EstimatedValueSize + s.size();
}

size_t EstimateSize(const NYson::TYsonString& s)
{
    return s ? EstimatedValueSize + s.AsStringBuf().size() : 0;
}

size_t EstimateSize(i64)
{
    return EstimatedValueSize;
}

size_t EstimateSize(TGuid id)
{
    return id.IsEmpty() ? 0 : EstimatedValueSize * 2;
}

size_t EstimateSize(TInstant)
{
    return EstimatedValueSize;
}

////////////////////////////////////////////////////////////////////////////////

size_t EstimateSizes()
{
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NJobAgent
