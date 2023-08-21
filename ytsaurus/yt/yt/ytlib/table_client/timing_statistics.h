#pragma once

#include "private.h"

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

TString ToString(const TTimingStatistics& statistics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
