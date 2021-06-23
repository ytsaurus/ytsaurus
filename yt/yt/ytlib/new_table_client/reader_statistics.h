#pragma once

#include <yt/yt/core/misc/common.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TReaderTimeStatistics final
{
    TDuration DecodeTimestampSegmentTime;
    TDuration DecodeKeySegmentTime;
    TDuration DecodeValueSegmentTime;
    TDuration FetchBlockTime;
    TDuration BuildRangesTime;
    TDuration DoReadTime;
};

using TReaderTimeStatisticsPtr = TIntrusivePtr<TReaderTimeStatistics>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
