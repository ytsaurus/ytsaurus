#pragma once

#include <yt/yt/core/misc/common.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TReaderStatistics final
{
    TDuration BuildReadWindowsTime;
    TDuration BuildSchemaIdMappingTime;
    TDuration CreateColumnBlockHoldersTime;
    TDuration BuildBlockInfosTime;
    TDuration CreateBlockFetcherTime;

    TDuration DecodeTimestampSegmentTime;
    TDuration DecodeKeySegmentTime;
    TDuration DecodeValueSegmentTime;
    TDuration FetchBlockTime;
    TDuration BuildRangesTime;

    TDuration DoReadTime;
    TDuration CollectCountsTime;
    TDuration AllocateRowsTime;
    TDuration DoReadKeysTime;
    TDuration DoReadValuesTime;

    size_t TryUpdateWindowCallCount = 0;
    size_t SkipToBlockCallCount = 0;
    size_t FetchBlockCallCount = 0;
    size_t SetBlockCallCount = 0;

    size_t UpdateSegmentCallCount = 0;
    size_t DoReadCallCount = 0;
};

using TReaderStatisticsPtr = TIntrusivePtr<TReaderStatistics>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
