#pragma once

#include <mapreduce/yt/interface/config.h>

#include <util/generic/strbuf.h>

namespace NYT {
namespace NDetail {

void IncDebugMetricImpl(TStringBuf name);

// Helper functions that allows to track various events inside YT library, useful for testing.
inline void IncDebugMetric(TStringBuf name)
{
    if (TConfig::Get()->EnableDebugMetrics) {
        IncDebugMetricImpl(name);
    }
}
ui64 GetDebugMetric(TStringBuf name);

} // namespace NDetail
} // namespace NYT
