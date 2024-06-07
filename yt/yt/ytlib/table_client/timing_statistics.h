#pragma once

#include "private.h"

#include <library/cpp/yt/string/format.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): consider moving this class to NChunkClient.

struct TTimingStatistics
{
    //! Time spent while waiting on reader ready event.
    TDuration WaitTime;
    //! Time spent in synchronous manner in Read().
    TDuration ReadTime;
    //! Time of not waiting and not reading.
    TDuration IdleTime;
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TTimingStatistics& statistics, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
