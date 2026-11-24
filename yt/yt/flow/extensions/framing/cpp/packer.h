#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/computation/computation_base.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

std::string PackBatch(
    const std::vector<TOutputMessageConstPtr>& messages,
    const std::string& dataColumn,
    const EFramingFormat framingFormat);

std::vector<std::pair<std::string, std::vector<TOutputMessageConstPtr>>> PackBatches(
    const std::deque<TOutputMessageConstPtr>& messages,
    const std::string& dataColumn,
    const EFramingFormat framingFormat,
    const i32 maxRowsPerMessage,
    const i32 maxBytesPerMessage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
