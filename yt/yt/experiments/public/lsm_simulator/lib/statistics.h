#pragma once

#include "public.h"

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

struct TStatistics
{
    i64 BytesWritten = 0;
    i64 BytesFlushed = 0;
    i64 BytesCompacted = 0;
    i64 BytesPartitioned = 0;

    i64 ChunksFlushed = 0;
};

TStatistics& operator+= (TStatistics& lhs, const TStatistics& rhs);
TStatistics operator+ (TStatistics lhs, const TStatistics& rhs);

TStatistics& operator-= (TStatistics& lhs, const TStatistics& rhs);
TStatistics operator- (TStatistics lhs, const TStatistics& rhs);

TStatistics operator/ (TStatistics lhs, int rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
