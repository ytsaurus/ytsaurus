#pragma once
#ifndef STATISTICS_INL_H_
#error "Direct inclusion of this file is not allowed, include statistics.h"
#endif

#include <yt/core/ypath/tokenizer.h>

namespace NYT {
namespace NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TStatistics::AddSample(const NYPath::TYPath& path, const T& sample)
{
    AddSample(path, NYTree::ConvertToNode(sample));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobTrackerClient
} // namespace NYT
