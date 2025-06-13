#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

IFileWriterPtr CreateFileWriter(
    IClientPtr client,
    const NYPath::TRichYPath& path,
    const TFileWriterOptions& options = TFileWriterOptions(),
    IMemoryUsageTrackerPtr memoryUsageTracker = GetNullMemoryUsageTracker());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

